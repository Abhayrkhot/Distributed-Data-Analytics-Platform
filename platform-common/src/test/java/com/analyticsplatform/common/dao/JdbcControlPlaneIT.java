package com.analyticsplatform.common.dao;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.analyticsplatform.common.config.PlatformConfig;
import com.analyticsplatform.common.dao.JdbcControlPlane.ControlPlaneException;
import com.analyticsplatform.common.run.ControlPlane.MetricSample;
import com.analyticsplatform.common.run.ControlPlane.MetricSample.AttemptScope;
import com.analyticsplatform.common.run.ControlPlane.RunOutcome;
import com.analyticsplatform.common.run.ControlPlane.RunSpec;
import com.analyticsplatform.common.run.RunContext;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * {@link JdbcControlPlane} against the real Postgres control plane.
 *
 * <p>Verifies the boundary a fake cannot: that the SQL matches the schema, that the CHECK
 * constraints accept what the DAO writes, and that {@code ::jsonb} accepts the hand-rolled JSON.
 *
 * <p>Requires the stack to be up. Run via {@code ./scripts/test-integration.sh}, which points
 * {@code PG_JDBC_URL} at the published port rather than the compose-internal hostname.
 *
 * <p>Isolation (§51): every test tags its rows with a unique job name and deletes them afterwards,
 * so a failure here cannot make a later test pass or fail.
 */
class JdbcControlPlaneIT {

    private static PlatformConfig config;
    private static ConnectionSource connections;

    private JdbcControlPlane controlPlane;
    private String jobName;

    @BeforeAll
    static void connect() {
        config = PlatformConfig.fromEnvironment();
        connections = ConnectionSource.postgres(config);
    }

    @BeforeEach
    void setUp() {
        controlPlane = new JdbcControlPlane(connections);
        jobName = "IT-" + UUID.randomUUID();
    }

    @AfterEach
    void cleanUp() throws SQLException {
        // Metrics cascade from etl_run, so deleting runs is enough.
        try (Connection connection = connections.open();
             PreparedStatement statement = connection.prepareStatement(
                     "DELETE FROM control.etl_run WHERE job_name = ?")) {
            statement.setString(1, jobName);
            statement.executeUpdate();
        }
    }

    private <T> T queryOne(String sql, long runId, SqlMapper<T> mapper) throws SQLException {
        try (Connection connection = connections.open();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setLong(1, runId);
            try (ResultSet rows = statement.executeQuery()) {
                assertThat(rows.next()).as("expected a row for run %s", runId).isTrue();
                return mapper.map(rows);
            }
        }
    }

    @FunctionalInterface
    private interface SqlMapper<T> {
        T map(ResultSet rows) throws SQLException;
    }

    @Test
    @DisplayName("a started run is RUNNING with no end time")
    void startRunOpensTheRow() throws SQLException {
        long runId = controlPlane.startRun(RunSpec.of(jobName));

        assertThat(runId).isPositive();
        String status = queryOne(
                "SELECT status FROM control.etl_run WHERE run_id = ?", runId,
                rows -> rows.getString(1));
        assertThat(status).isEqualTo("RUNNING");
    }

    @Test
    @DisplayName("finishing records status, counters and a database-computed duration")
    void finishRunClosesTheRow() throws SQLException {
        long runId = controlPlane.startRun(RunSpec.of(jobName));
        controlPlane.finishRun(runId, new RunOutcome(
                RunOutcome.Status.SUCCESS, 100, 90, 10, null, null));

        Object[] row = queryOne("""
                SELECT status, rows_read, rows_written, rows_rejected, duration_ms, ended_at
                  FROM control.etl_run WHERE run_id = ?
                """, runId, rows -> new Object[] {
                    rows.getString(1), rows.getLong(2), rows.getLong(3),
                    rows.getLong(4), rows.getLong(5), rows.getTimestamp(6)});

        assertThat(row[0]).isEqualTo("SUCCESS");
        assertThat(row[1]).isEqualTo(100L);
        assertThat(row[2]).isEqualTo(90L);
        assertThat(row[3]).isEqualTo(10L);
        assertThat((Long) row[4]).isNotNegative();
        assertThat(row[5]).isNotNull();
    }

    @Test
    @DisplayName("a failed run stores its error class and message")
    void failedRunRecordsError() throws SQLException {
        long runId = controlPlane.startRun(RunSpec.of(jobName));
        controlPlane.finishRun(runId, new RunOutcome(
                RunOutcome.Status.FAILED, 0, 0, 0, "java.io.IOException", "disk gone"));

        String[] row = queryOne(
                "SELECT error_class, error_message FROM control.etl_run WHERE run_id = ?", runId,
                rows -> new String[] {rows.getString(1), rows.getString(2)});

        assertThat(row[0]).isEqualTo("java.io.IOException");
        assertThat(row[1]).isEqualTo("disk gone");
    }

    /**
     * The guard that stops a second, contradictory outcome from looking like it was recorded.
     * Without the {@code status = 'RUNNING'} predicate this would silently update zero rows.
     */
    @Test
    @DisplayName("finishing an already-finished run is refused, not silently ignored")
    void doubleFinishIsRefused() {
        long runId = controlPlane.startRun(RunSpec.of(jobName));
        controlPlane.finishRun(runId,
                new RunOutcome(RunOutcome.Status.SUCCESS, 1, 1, 0, null, null));

        assertThatThrownBy(() -> controlPlane.finishRun(runId, new RunOutcome(
                RunOutcome.Status.FAILED, 0, 0, 0, "java.lang.IllegalStateException", "late")))
                .isInstanceOf(ControlPlaneException.class)
                .hasMessageContaining("not in RUNNING state");
    }

    @Test
    @DisplayName("both attempt scopes persist as distinct rows")
    void metricsPersistUnderBothScopes() throws SQLException {
        long runId = controlPlane.startRun(RunSpec.of(jobName));

        controlPlane.recordMetric(runId, new MetricSample(
                "shuffle_read_bytes", 200, "bytes", AttemptScope.ALL_ATTEMPTS));
        controlPlane.recordMetric(runId, new MetricSample(
                "shuffle_read_bytes", 100, "bytes", AttemptScope.SUCCESSFUL_ONLY));

        long count = queryOne("""
                SELECT count(*) FROM control.etl_run_metric
                 WHERE run_id = ? AND metric_name = 'shuffle_read_bytes'
                """, runId, rows -> rows.getLong(1));

        assertThat(count).isEqualTo(2);
    }

    @Test
    @DisplayName("re-recording a metric updates rather than conflicting")
    void metricUpsertOverwrites() throws SQLException {
        long runId = controlPlane.startRun(RunSpec.of(jobName));

        controlPlane.recordMetric(runId, new MetricSample(
                "task_count", 5, "count", AttemptScope.ALL_ATTEMPTS));
        controlPlane.recordMetric(runId, new MetricSample(
                "task_count", 9, "count", AttemptScope.ALL_ATTEMPTS));

        double value = queryOne("""
                SELECT metric_value FROM control.etl_run_metric
                 WHERE run_id = ? AND metric_name = 'task_count'
                   AND attempt_scope = 'all_attempts'
                """, runId, rows -> rows.getDouble(1));

        assertThat(value).isEqualTo(9.0);
    }

    /** The hand-rolled encoder has to satisfy Postgres's ::jsonb cast, not just look right. */
    @Test
    @DisplayName("config JSON with awkward characters survives the jsonb cast")
    void configJsonRoundTrips() throws SQLException {
        long runId = controlPlane.startRun(new RunSpec(
                jobName, "1.0", "bronze", "app-1", "optimized", "abc123",
                Map.of("quote", "a\"b", "newline", "a\nb", "unicode", "Åland")));

        String stored = queryOne(
                "SELECT config_json->>'quote' FROM control.etl_run WHERE run_id = ?", runId,
                rows -> rows.getString(1));

        assertThat(stored).isEqualTo("a\"b");
    }

    /** RunContext's semantics must survive contact with the real schema's CHECK constraints. */
    @Test
    @DisplayName("RunContext records a real failure end to end")
    void runContextFailurePathAgainstRealSchema() throws SQLException {
        IOException original = new IOException("real failure");

        Throwable thrown = catchThrowable(() ->
                RunContext.execute(controlPlane, RunSpec.of(jobName), run -> {
                    throw original;
                }));

        assertThat(thrown).isSameAs(original);

        try (Connection connection = connections.open();
             PreparedStatement statement = connection.prepareStatement("""
                     SELECT status, error_class FROM control.etl_run
                      WHERE job_name = ? ORDER BY run_id DESC LIMIT 1
                     """)) {
            statement.setString(1, jobName);
            try (ResultSet rows = statement.executeQuery()) {
                assertThat(rows.next()).isTrue();
                assertThat(rows.getString(1)).isEqualTo("FAILED");
                assertThat(rows.getString(2)).isEqualTo("java.io.IOException");
            }
        }
    }
}
