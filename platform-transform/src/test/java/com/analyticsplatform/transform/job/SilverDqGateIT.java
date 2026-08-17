package com.analyticsplatform.transform.job;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.analyticsplatform.common.config.PlatformConfig;
import com.analyticsplatform.common.dao.ConnectionSource;
import com.analyticsplatform.common.dao.JdbcControlPlane;
import com.analyticsplatform.common.dao.LineageRecorder;
import com.analyticsplatform.common.run.ControlPlane.RunSpec;
import com.analyticsplatform.common.schema.SchemaRegistry;
import com.analyticsplatform.common.testing.Fixtures;
import com.analyticsplatform.common.testing.SparkTestSupport;
import com.analyticsplatform.ingest.publish.ProcessingUnitStore;
import com.analyticsplatform.ingest.publish.StagedPublisher;
import com.analyticsplatform.ingest.source.SourceNormalizer;
import com.analyticsplatform.transform.dq.DqEngine;
import com.analyticsplatform.transform.dq.DqRuleStore;
import com.analyticsplatform.transform.job.SilverTransformJob.DqGateFailure;
import com.analyticsplatform.transform.job.SilverTransformJob.Inputs;
import com.analyticsplatform.transform.job.SilverTransformJob.Result;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.UUID;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tier 1 evidence for the data-quality-enforcement claim.
 *
 * <p>The assertion that matters throughout is not "the run failed" but "<em>nothing was
 * published</em>". A gate that reports a breach after the data is already in the warehouse has
 * detected a problem it failed to prevent.
 *
 * <p>Each test installs its own rules against a unique dataset name rather than mutating the shared
 * production rule set, which would leave the next run judged by whatever the last test configured.
 */
class SilverDqGateIT {

    private static ConnectionSource connections;
    private static JdbcControlPlane controlPlane;

    private String dataset;
    private String suffix;
    private long runId;
    private ProcessingUnitStore store;
    private SilverTransformJob job;
    private Dataset<Row> bronze;

    @TempDir
    Path root;

    private Path target;

    @BeforeAll
    static void connect() {
        connections = ConnectionSource.postgres(PlatformConfig.fromEnvironment());
        controlPlane = new JdbcControlPlane(connections);
    }

    @BeforeEach
    void setUp() {
        suffix = UUID.randomUUID().toString();
        dataset = "it.silver." + suffix;
        runId = controlPlane.startRun(RunSpec.of("IT-silver-" + suffix));
        target = root.resolve("warehouse/silver");

        store = new ProcessingUnitStore(connections);
        job = new SilverTransformJob(
                SparkTestSupport.spark(), dataset,
                new SchemaRegistry(connections, SchemaRegistry.Policy.ALLOW_WIDENING),
                new DqRuleStore(connections), new DqEngine(),
                new StagedPublisher(store, root.resolve("staging"), 900),
                new LineageRecorder(connections));

        bronze = SourceNormalizer.normalizeYellow(Fixtures.yellow2024())
                .union(SourceNormalizer.normalizeYellow(Fixtures.yellow2025()))
                .union(SourceNormalizer.normalizeGreen(Fixtures.green2024()));
    }

    @AfterEach
    void cleanUp() throws Exception {
        try (Connection connection = connections.open()) {
            for (String sql : new String[] {
                "DELETE FROM control.dq_result WHERE dataset_name = ?",
                "DELETE FROM control.dq_rule WHERE dataset_name = ?",
                "DELETE FROM control.unit_manifest WHERE dataset_name = ?",
                "DELETE FROM control.processing_unit WHERE dataset_name = ?",
                "DELETE FROM control.schema_version WHERE dataset_name = ?"}) {
                try (PreparedStatement statement = connection.prepareStatement(sql)) {
                    statement.setString(1, dataset);
                    statement.executeUpdate();
                }
            }
            try (PreparedStatement statement = connection.prepareStatement(
                    "DELETE FROM control.lineage_edge WHERE run_id = ?")) {
                statement.setLong(1, runId);
                statement.executeUpdate();
            }
            try (PreparedStatement statement = connection.prepareStatement(
                    "DELETE FROM control.lineage_node WHERE node_name = ?")) {
                statement.setString(1, dataset);
                statement.executeUpdate();
            }
            try (PreparedStatement statement = connection.prepareStatement(
                    "DELETE FROM control.etl_run WHERE run_id = ?")) {
                statement.setLong(1, runId);
                statement.executeUpdate();
            }
        }
    }

    /** Installs a rule for this test's dataset only. */
    private void installRule(String name, String type, String column, String params,
                             String severity, String thresholdType, String threshold,
                             String nullPolicy) throws Exception {
        try (Connection connection = connections.open();
             PreparedStatement statement = connection.prepareStatement("""
                     INSERT INTO control.dq_rule
                         (rule_name, dataset_name, rule_type, target_column, rule_params,
                          severity, threshold_type, threshold_value, null_policy)
                     VALUES (?, ?, ?, ?, ?::jsonb, ?, ?, ?, ?)
                     """)) {
            statement.setString(1, name + "-" + suffix);
            statement.setString(2, dataset);
            statement.setString(3, type);
            statement.setString(4, column);
            statement.setString(5, params);
            statement.setString(6, severity);
            statement.setString(7, thresholdType);
            statement.setBigDecimal(8, new java.math.BigDecimal(threshold));
            statement.setString(9, nullPolicy);
            statement.executeUpdate();
        }
    }

    private Result run() {
        return job.run(new Inputs(bronze, Fixtures.taxiZones(),
                "yellow/2024-01", target, runId, "owner-" + suffix));
    }

    private boolean targetExists() {
        return Files.isDirectory(target);
    }

    private long dqResultCount() throws Exception {
        try (Connection connection = connections.open();
             PreparedStatement statement = connection.prepareStatement(
                     "SELECT count(*) FROM control.dq_result WHERE dataset_name = ?")) {
            statement.setString(1, dataset);
            try (ResultSet rows = statement.executeQuery()) {
                rows.next();
                return rows.getLong(1);
            }
        }
    }

    @Nested
    @DisplayName("a passing gate publishes")
    class Passing {

        @Test
        @DisplayName("clean data publishes and records its DQ results")
        void cleanDataPublishes() throws Exception {
            installRule("fare_range", "range", "fare_amount",
                    "{\"min\": 0, \"max\": 10000}", "FAIL",
                    "max_violation_fraction", "0.0", "pass");

            Result result = run();

            assertThat(result.published()).isTrue();
            assertThat(result.dqReport().blocked()).isFalse();
            assertThat(targetExists()).isTrue();
            assertThat(store.findManifest(new ProcessingUnitStore.UnitKey(
                    dataset, SilverTransformJob.STAGE, "yellow/2024-01"))).isPresent();
            assertThat(dqResultCount()).isEqualTo(1);
        }

        /** A breached WARN records the breach and lets the data through. */
        @Test
        @DisplayName("a breached WARN records but does not block")
        void warnDoesNotBlock() throws Exception {
            installRule("impossible_warn", "range", "fare_amount",
                    "{\"min\": 1000, \"max\": 2000}", "WARN",
                    "max_violation_fraction", "0.0", "pass");

            Result result = run();

            assertThat(result.published()).as("WARN must not block").isTrue();
            assertThat(result.dqReport().breaches()).hasSize(1);
            assertThat(result.dqReport().blocked()).isFalse();
            assertThat(targetExists()).isTrue();
        }
    }

    @Nested
    @DisplayName("a blocking gate publishes nothing")
    class Blocking {

        /**
         * The central claim. Not merely that the run failed — that the target was never written.
         */
        @Test
        @DisplayName("a breached FAIL aborts and leaves the target untouched")
        void failBlocksPublication() throws Exception {
            installRule("impossible_fail", "range", "fare_amount",
                    "{\"min\": 1000, \"max\": 2000}", "FAIL",
                    "max_violation_fraction", "0.0", "pass");

            Throwable thrown = catchThrowable(SilverDqGateIT.this::run);

            assertThat(thrown).isInstanceOf(DqGateFailure.class);
            assertThat(targetExists()).as("NOTHING may be published").isFalse();
            assertThat(store.findManifest(new ProcessingUnitStore.UnitKey(
                    dataset, SilverTransformJob.STAGE, "yellow/2024-01"))).isEmpty();
        }

        /** The breach must still be recorded — a blocked run is evidence, not a silent abort. */
        @Test
        @DisplayName("a blocked run still records its DQ results")
        void blockedRunRecordsResults() throws Exception {
            installRule("impossible_fail", "range", "fare_amount",
                    "{\"min\": 1000, \"max\": 2000}", "FAIL",
                    "max_violation_fraction", "0.0", "pass");

            catchThrowable(SilverDqGateIT.this::run);

            assertThat(dqResultCount()).as("the breach is recorded").isEqualTo(1);
            try (Connection connection = connections.open();
                 PreparedStatement statement = connection.prepareStatement(
                         "SELECT passed, rows_violated FROM control.dq_result "
                                 + "WHERE dataset_name = ?")) {
                statement.setString(1, dataset);
                try (ResultSet rows = statement.executeQuery()) {
                    assertThat(rows.next()).isTrue();
                    assertThat(rows.getBoolean("passed")).isFalse();
                    assertThat(rows.getLong("rows_violated")).isEqualTo(14);
                }
            }
        }

        /** A blocked run must leave no lineage claiming the derivation happened. */
        @Test
        @DisplayName("a blocked run records no lineage")
        void blockedRunRecordsNoLineage() throws Exception {
            installRule("impossible_fail", "range", "fare_amount",
                    "{\"min\": 1000, \"max\": 2000}", "FAIL",
                    "max_violation_fraction", "0.0", "pass");

            catchThrowable(SilverDqGateIT.this::run);

            try (Connection connection = connections.open();
                 PreparedStatement statement = connection.prepareStatement(
                         "SELECT count(*) FROM control.lineage_edge WHERE run_id = ?")) {
                statement.setLong(1, runId);
                try (ResultSet rows = statement.executeQuery()) {
                    rows.next();
                    assertThat(rows.getLong(1))
                            .as("no lineage for a derivation that never happened").isZero();
                }
            }
        }

        /**
         * A run that SKIPS because the unit is already committed must not record lineage again.
         * The blocked path cannot prove this — its exception short-circuits before the lineage
         * call is reached — so without this case the guard is never actually exercised.
         */
        @Test
        @DisplayName("a skipped run records no new lineage")
        void skippedRunRecordsNoLineage() throws Exception {
            installRule("fare_range", "range", "fare_amount",
                    "{\"min\": 0, \"max\": 10000}", "FAIL",
                    "max_violation_fraction", "0.0", "pass");
            assertThat(run().published()).isTrue();

            long secondRunId = controlPlane.startRun(RunSpec.of("IT-silver-skip-" + suffix));
            try {
                Result second = job.run(new Inputs(bronze, Fixtures.taxiZones(),
                        "yellow/2024-01", target, secondRunId, "owner2-" + suffix));
                assertThat(second.published()).as("already committed").isFalse();

                try (Connection connection = connections.open();
                     PreparedStatement statement = connection.prepareStatement(
                             "SELECT count(*) FROM control.lineage_edge WHERE run_id = ?")) {
                    statement.setLong(1, secondRunId);
                    try (ResultSet rows = statement.executeQuery()) {
                        rows.next();
                        assertThat(rows.getLong(1))
                                .as("a skipped run derived nothing").isZero();
                    }
                }
            } finally {
                try (Connection connection = connections.open()) {
                    try (PreparedStatement s = connection.prepareStatement(
                            "DELETE FROM control.lineage_edge WHERE run_id = ?")) {
                        s.setLong(1, secondRunId); s.executeUpdate();
                    }
                    try (PreparedStatement s = connection.prepareStatement(
                            "DELETE FROM control.etl_run WHERE run_id = ?")) {
                        s.setLong(1, secondRunId); s.executeUpdate();
                    }
                }
            }
        }

        /** After fixing the rule, the same unit publishes — the block is not permanent. */
        @Test
        @DisplayName("the unit publishes once the rule is satisfied")
        void retryAfterFixSucceeds() throws Exception {
            installRule("impossible_fail", "range", "fare_amount",
                    "{\"min\": 1000, \"max\": 2000}", "FAIL",
                    "max_violation_fraction", "0.0", "pass");
            catchThrowable(SilverDqGateIT.this::run);
            assertThat(targetExists()).isFalse();

            try (Connection connection = connections.open();
                 PreparedStatement statement = connection.prepareStatement(
                         "UPDATE control.dq_rule SET rule_params = '{\"min\": 0, \"max\": 10000}'"
                                 + " WHERE dataset_name = ?")) {
                statement.setString(1, dataset);
                statement.executeUpdate();
            }

            assertThat(run().published()).isTrue();
            assertThat(targetExists()).isTrue();
        }
    }

    @Nested
    @DisplayName("null policy decides the verdict end to end")
    class NullPolicyEndToEnd {

        /**
         * The zero-fare trip's {@code tip_pct} is null in silver. Under VIOLATION that null is a
         * breach and publication stops; under PASS the same data publishes. Same rule, same data,
         * opposite outcomes — which is exactly why the policy has to be declared rather than
         * inferred.
         */
        @Test
        @DisplayName("VIOLATION blocks on a null that PASS tolerates")
        void nullPolicyChangesTheOutcome() throws Exception {
            installRule("tip_not_null", "range", "tip_pct",
                    "{\"min\": 0, \"max\": 5}", "FAIL",
                    "max_violation_fraction", "0.0", "violation");

            assertThat(catchThrowable(SilverDqGateIT.this::run))
                    .as("the null tip_pct is a violation").isInstanceOf(DqGateFailure.class);
            assertThat(targetExists()).isFalse();

            try (Connection connection = connections.open();
                 PreparedStatement statement = connection.prepareStatement(
                         "UPDATE control.dq_rule SET null_policy = 'pass' WHERE dataset_name = ?")) {
                statement.setString(1, dataset);
                statement.executeUpdate();
            }

            assertThat(run().published()).as("the same data passes under PASS").isTrue();
        }
    }

    @Nested
    @DisplayName("misconfiguration is not a silent pass")
    class Misconfiguration {

        /**
         * A rule pointing at a column that does not exist must abort, not be skipped. Skipping
         * would report a clean bill of health from a check that never ran.
         */
        @Test
        @DisplayName("a rule targeting an absent column aborts the run")
        void absentColumnAborts() throws Exception {
            installRule("nonexistent", "not_null", "column_that_does_not_exist",
                    "{}", "FAIL", "max_violation_fraction", "0.0", "violation");

            Throwable thrown = catchThrowable(SilverDqGateIT.this::run);

            assertThat(thrown).isInstanceOf(IllegalStateException.class);
            assertThat(thrown).hasMessageContaining("absent from silver");
            assertThat(targetExists()).isFalse();
        }
    }
}
