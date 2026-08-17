package com.analyticsplatform.common.dao;

import com.analyticsplatform.common.run.ControlPlane;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.UUID;

/**
 * Postgres-backed {@link ControlPlane}.
 *
 * <p>Deliberately thin. The interesting behaviour — when a run counts as successful, how failures
 * are recorded — lives in {@code RunContext} and is tested against a fake; this class only has to
 * translate that faithfully into SQL. Keeping it dumb is what makes the failure semantics
 * testable without a database.
 */
public final class JdbcControlPlane implements ControlPlane {

    private static final String INSERT_RUN = """
            INSERT INTO control.etl_run
                (run_uuid, job_name, job_version, layer, spark_app_id,
                 config_json, config_label, git_commit, status)
            VALUES (?, ?, ?, ?, ?, ?::jsonb, ?, ?, 'RUNNING')
            RETURNING run_id
            """;

    /**
     * duration_ms is computed from started_at in the database rather than from a clock on the
     * driver, so a run's duration does not depend on which machine submitted it.
     */
    private static final String FINISH_RUN = """
            UPDATE control.etl_run
               SET status        = ?,
                   ended_at      = now(),
                   duration_ms   = GREATEST(0, (EXTRACT(EPOCH FROM (now() - started_at)) * 1000)::bigint),
                   rows_read     = ?,
                   rows_written  = ?,
                   rows_rejected = ?,
                   error_class   = ?,
                   error_message = ?
             WHERE run_id = ?
               AND status = 'RUNNING'
            """;

    /**
     * Upsert rather than plain insert: a job may emit a metric more than once (a retry of the
     * reporting step, say), and the last observation is the correct one. The unique key includes
     * attempt_scope, so the two scopes never collide.
     */
    private static final String UPSERT_METRIC = """
            INSERT INTO control.etl_run_metric
                (run_id, metric_name, metric_value, metric_unit, attempt_scope)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT (run_id, metric_name, attempt_scope)
            DO UPDATE SET metric_value = EXCLUDED.metric_value,
                          metric_unit  = EXCLUDED.metric_unit,
                          recorded_at  = now()
            """;

    private final ConnectionSource connections;

    public JdbcControlPlane(ConnectionSource connections) {
        this.connections = connections;
    }

    @Override
    public long startRun(RunSpec spec) {
        try (Connection connection = connections.open();
             PreparedStatement statement = connection.prepareStatement(INSERT_RUN)) {

            statement.setObject(1, UUID.randomUUID());
            statement.setString(2, spec.jobName());
            statement.setString(3, spec.jobVersion());
            statement.setString(4, spec.layer());
            statement.setString(5, spec.sparkAppId());
            statement.setString(6, toJson(spec.config()));
            statement.setString(7, spec.configLabel());
            statement.setString(8, spec.gitCommit());

            try (ResultSet rows = statement.executeQuery()) {
                if (!rows.next()) {
                    throw new ControlPlaneException("startRun returned no run_id");
                }
                return rows.getLong(1);
            }
        } catch (SQLException e) {
            throw new ControlPlaneException("failed to start run " + spec.jobName(), e);
        }
    }

    @Override
    public void finishRun(long runId, RunOutcome outcome) {
        try (Connection connection = connections.open();
             PreparedStatement statement = connection.prepareStatement(FINISH_RUN)) {

            statement.setString(1, outcome.status().name());
            statement.setLong(2, outcome.rowsRead());
            statement.setLong(3, outcome.rowsWritten());
            statement.setLong(4, outcome.rowsRejected());
            statement.setString(5, outcome.errorClass());
            statement.setString(6, outcome.errorMessage());
            statement.setLong(7, runId);

            int updated = statement.executeUpdate();
            if (updated == 0) {
                // The WHERE clause requires status='RUNNING', so zero rows means this run was
                // already closed. Silently succeeding would let a second, contradictory outcome
                // look like it had been recorded.
                throw new ControlPlaneException(
                        "run " + runId + " was not in RUNNING state; refusing to overwrite a "
                                + "terminal status with " + outcome.status());
            }
        } catch (SQLException e) {
            throw new ControlPlaneException("failed to finish run " + runId, e);
        }
    }

    @Override
    public void recordMetric(long runId, MetricSample sample) {
        try (Connection connection = connections.open();
             PreparedStatement statement = connection.prepareStatement(UPSERT_METRIC)) {

            statement.setLong(1, runId);
            statement.setString(2, sample.name());
            statement.setDouble(3, sample.value());
            statement.setString(4, sample.unit());
            statement.setString(5, sample.attemptScope().dbValue());
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new ControlPlaneException(
                    "failed to record metric " + sample.name() + " for run " + runId, e);
        }
    }

    /** Records many metrics in one round trip. */
    public void recordMetrics(long runId, Iterable<MetricSample> samples) {
        try (Connection connection = connections.open();
             PreparedStatement statement = connection.prepareStatement(UPSERT_METRIC)) {

            for (MetricSample sample : samples) {
                statement.setLong(1, runId);
                statement.setString(2, sample.name());
                statement.setDouble(3, sample.value());
                statement.setString(4, sample.unit());
                statement.setString(5, sample.attemptScope().dbValue());
                statement.addBatch();
            }
            statement.executeBatch();
        } catch (SQLException e) {
            throw new ControlPlaneException("failed to record metrics for run " + runId, e);
        }
    }

    /**
     * Minimal JSON object encoder for the {@code config_json} column.
     *
     * <p>Hand-rolled rather than pulling in Jackson: Spark supplies its own Jackson at runtime and
     * a version mismatch here would surface as a {@code NoSuchMethodError} on the cluster, which
     * is a miserable failure to diagnose for the sake of serializing a flat string map.
     */
    static String toJson(Map<String, String> values) {
        if (values == null || values.isEmpty()) {
            return "{}";
        }
        StringBuilder json = new StringBuilder("{");
        boolean first = true;
        for (Map.Entry<String, String> entry : values.entrySet()) {
            if (!first) {
                json.append(',');
            }
            first = false;
            json.append(quote(entry.getKey())).append(':').append(quote(entry.getValue()));
        }
        return json.append('}').toString();
    }

    private static String quote(String raw) {
        if (raw == null) {
            return "null";
        }
        StringBuilder out = new StringBuilder(raw.length() + 2).append('"');
        for (int i = 0; i < raw.length(); i++) {
            char c = raw.charAt(i);
            switch (c) {
                case '"' -> out.append("\\\"");
                case '\\' -> out.append("\\\\");
                case '\n' -> out.append("\\n");
                case '\r' -> out.append("\\r");
                case '\t' -> out.append("\\t");
                case '\b' -> out.append("\\b");
                case '\f' -> out.append("\\f");
                default -> {
                    if (c < 0x20) {
                        out.append(String.format("\\u%04x", (int) c));
                    } else {
                        out.append(c);
                    }
                }
            }
        }
        return out.append('"').toString();
    }

    /** Wraps SQL failures so callers are not forced to handle checked {@link SQLException}. */
    public static final class ControlPlaneException extends RuntimeException {
        public ControlPlaneException(String message) {
            super(message);
        }

        public ControlPlaneException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
