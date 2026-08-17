package com.analyticsplatform.ingest.job;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.analyticsplatform.common.config.PlatformConfig;
import com.analyticsplatform.common.dao.ConnectionSource;
import com.analyticsplatform.common.dao.JdbcControlPlane;
import com.analyticsplatform.common.dao.LineageRecorder;
import com.analyticsplatform.common.run.ControlPlane.RunOutcome;
import com.analyticsplatform.common.run.ControlPlane.RunSpec;
import com.analyticsplatform.common.schema.SchemaRegistry;
import com.analyticsplatform.common.schema.SchemaRegistry.SchemaEvolutionException;
import com.analyticsplatform.common.testing.Fixtures;
import com.analyticsplatform.common.testing.SparkTestSupport;
import com.analyticsplatform.common.unit.ProcessingUnitState.Status;
import com.analyticsplatform.ingest.publish.ProcessingUnitStore;
import com.analyticsplatform.ingest.publish.ProcessingUnitStore.UnitKey;
import com.analyticsplatform.ingest.publish.StagedPublisher;
import com.analyticsplatform.ingest.source.SourceNormalizer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;
import java.util.UUID;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * {@link BronzeIngestJob} against the real control plane.
 *
 * <p>Lives here rather than only being exercised by the end-to-end suite in platform-transform.
 * That suite does run this job, but JaCoCo measures coverage per module and cannot see execution
 * driven from another one — so without this the job read as entirely untested and the gate failed.
 * The deeper reason to keep it here is ownership: a job belongs to the module it lives in, and its
 * contract should be verifiable without depending on a downstream module.
 */
class BronzeIngestJobIT {

    private static ConnectionSource connections;
    private static JdbcControlPlane controlPlane;

    private String dataset;
    private String suffix;
    private long runId;
    private ProcessingUnitStore store;
    private UnitKey key;

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
        suffix = UUID.randomUUID().toString().substring(0, 8);
        dataset = "it.bronzejob." + suffix;
        runId = controlPlane.startRun(RunSpec.of("IT-bronze-" + suffix));
        store = new ProcessingUnitStore(connections);
        target = root.resolve("warehouse/bronze/source=yellow");
        key = new UnitKey(dataset, BronzeIngestJob.STAGE, "yellow/2024-01");
    }

    @AfterEach
    void cleanUp() throws Exception {
        try (Connection connection = connections.open()) {
            for (String sql : new String[] {
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
            controlPlane.finishRun(runId,
                    new RunOutcome(RunOutcome.Status.SUCCESS, 0, 0, 0, null, null));
            try (PreparedStatement statement = connection.prepareStatement(
                    "DELETE FROM control.etl_run WHERE run_id = ?")) {
                statement.setLong(1, runId);
                statement.executeUpdate();
            }
        }
    }

    private BronzeIngestJob job(SchemaRegistry.Policy policy) {
        return new BronzeIngestJob(dataset,
                new SchemaRegistry(connections, policy),
                new StagedPublisher(store, root.resolve("staging"), 900),
                new LineageRecorder(connections));
    }

    private BronzeIngestJob.Result run() {
        return job(SchemaRegistry.Policy.ALLOW_WIDENING).run(new BronzeIngestJob.Inputs(
                Fixtures.yellow2024(), "yellow", "yellow/2024-01", target, runId, "owner"));
    }

    private long lineageEdgeCount() throws Exception {
        return edgeCountFor(runId);
    }

    private long edgeCountFor(long forRun) throws Exception {
        try (Connection connection = connections.open();
             PreparedStatement statement = connection.prepareStatement(
                     "SELECT count(*) FROM control.lineage_edge WHERE run_id = ?")) {
            statement.setLong(1, forRun);
            try (ResultSet rows = statement.executeQuery()) {
                rows.next();
                return rows.getLong(1);
            }
        }
    }

    @Nested
    @DisplayName("publishing")
    class Publishing {

        @Test
        @DisplayName("normalizes and publishes every row, rejecting none")
        void publishesEverything() {
            BronzeIngestJob.Result result = run();

            assertThat(result.published()).isTrue();
            assertThat(result.rowsRead()).isEqualTo(12);
            assertThat(result.rowsWritten()).as("bronze rejects nothing").isEqualTo(12);
            assertThat(store.findManifest(key)).isPresent();
            assertThat(store.status(key)).contains(Status.COMPLETE);
        }

        @Test
        @DisplayName("the published output carries the canonical bronze schema")
        void outputIsCanonical() {
            run();

            assertThat(List.of(SparkTestSupport.spark().read()
                    .parquet(target.toString()).columns()))
                    .containsExactlyElementsOf(SourceNormalizer.bronzeColumns());
        }

        @Test
        @DisplayName("green normalizes through the same job")
        void greenAlsoPublishes() {
            Path greenTarget = root.resolve("warehouse/bronze/source=green");

            BronzeIngestJob.Result result = job(SchemaRegistry.Policy.ALLOW_WIDENING)
                    .run(new BronzeIngestJob.Inputs(Fixtures.green2024(), "green",
                            "green/2024-01", greenTarget, runId, "owner"));

            assertThat(result.published()).isTrue();
            assertThat(result.rowsWritten()).isEqualTo(4);
        }

        @Test
        @DisplayName("a rerun skips without republishing")
        void rerunSkips() {
            run();

            BronzeIngestJob.Result second = run();

            assertThat(second.outcome().skipped()).isTrue();
            assertThat(second.published()).isFalse();
        }
    }

    @Nested
    @DisplayName("lineage")
    class Lineage {

        @Test
        @DisplayName("a publishing run records its derivation")
        void publishRecordsLineage() throws Exception {
            run();

            assertThat(lineageEdgeCount()).isPositive();
        }

        /**
         * A skipped run publishes nothing, so it must record no lineage.
         *
         * <p>The rerun uses a SECOND run id deliberately. With the same id, LineageRecorder's
         * per-run deduplication silently absorbs a spurious edge and the test passes even when the
         * guard is removed — which is exactly what mutation testing caught. Only a distinct run id
         * makes the unwanted write observable.
         */
        @Test
        @DisplayName("a skipped rerun records no lineage under its own run id")
        void skippedRunAddsNoLineage() throws Exception {
            run();
            assertThat(lineageEdgeCount()).isPositive();

            long rerunId = controlPlane.startRun(RunSpec.of("IT-bronze-rerun-" + suffix));
            try {
                job(SchemaRegistry.Policy.ALLOW_WIDENING).run(new BronzeIngestJob.Inputs(
                        Fixtures.yellow2024(), "yellow", "yellow/2024-01",
                        target, rerunId, "owner-2"));

                assertThat(edgeCountFor(rerunId))
                        .as("a skipped run derives nothing, so it records nothing")
                        .isZero();
            } finally {
                try (Connection connection = connections.open()) {
                    try (PreparedStatement statement = connection.prepareStatement(
                            "DELETE FROM control.lineage_edge WHERE run_id = ?")) {
                        statement.setLong(1, rerunId);
                        statement.executeUpdate();
                    }
                    controlPlane.finishRun(rerunId,
                            new RunOutcome(RunOutcome.Status.SUCCESS, 0, 0, 0, null, null));
                    try (PreparedStatement statement = connection.prepareStatement(
                            "DELETE FROM control.etl_run WHERE run_id = ?")) {
                        statement.setLong(1, rerunId);
                        statement.executeUpdate();
                    }
                }
            }
        }
    }

    @Nested
    @DisplayName("schema gate")
    class SchemaGate {

        /** A breaking change must abort before anything is written. */
        @Test
        @DisplayName("a narrowing schema aborts and publishes nothing")
        void breakingSchemaPublishesNothing() throws Exception {
            StructType widened = new StructType(new StructField[] {
                new StructField("vendor_id", DataTypes.LongType, true, Metadata.empty()),
            });
            // Pre-register a version whose vendor_id is wider than the job will produce.
            StructType produced = SourceNormalizer.normalizeYellow(Fixtures.yellow2024()).schema();
            StructField[] fields = new StructField[produced.fields().length];
            for (int i = 0; i < fields.length; i++) {
                StructField f = produced.fields()[i];
                fields[i] = f.name().equals("vendor_id")
                        ? new StructField(f.name(), DataTypes.LongType, f.nullable(),
                                Metadata.empty())
                        : f;
            }
            StructType altered = new StructType(fields);

            try (Connection connection = connections.open();
                 PreparedStatement statement = connection.prepareStatement("""
                         INSERT INTO control.schema_version
                             (dataset_name, version, schema_json, schema_hash, change_type)
                         VALUES (?, 1, ?::jsonb, ?, 'initial')
                         """)) {
                statement.setString(1, dataset);
                statement.setString(2, altered.json());
                statement.setString(3,
                        com.analyticsplatform.common.schema.CanonicalSchema.hash(altered));
                statement.executeUpdate();
            }

            assertThat(catchThrowable(BronzeIngestJobIT.this::run))
                    .isInstanceOf(SchemaEvolutionException.class);
            assertThat(Files.isDirectory(target)).as("NOTHING published").isFalse();
            assertThat(store.findManifest(key)).isEmpty();
            assertThat(widened).isNotNull();
        }

        /**
         * Green after yellow is NOT a schema change, because the normalizer already conformed them
         * to one bronze schema — which is the entire reason it exists. Asserting it under STRICT
         * makes the guarantee explicit: adding a source does not force a schema version bump.
         *
         * <p>An earlier version of this test expected an exception here. That expectation was
         * wrong, not the code.
         */
        @Test
        @DisplayName("a second source does not trip the gate, even under STRICT")
        void conformedSourcesDoNotTripStrict() throws Exception {
            SchemaRegistry.Policy strict = SchemaRegistry.Policy.STRICT;

            job(strict).run(new BronzeIngestJob.Inputs(
                    Fixtures.yellow2024(), "yellow", "yellow/2024-01", target, runId, "owner"));

            BronzeIngestJob.Result green = job(strict).run(new BronzeIngestJob.Inputs(
                    Fixtures.green2024(), "green", "green/2024-01",
                    root.resolve("warehouse/bronze/source=green2"), runId, "owner"));

            assertThat(green.published()).as("green conforms, so STRICT permits it").isTrue();

            try (Connection connection = connections.open();
                 PreparedStatement statement = connection.prepareStatement(
                         "SELECT count(*) FROM control.schema_version WHERE dataset_name = ?")) {
                statement.setString(1, dataset);
                try (ResultSet rows = statement.executeQuery()) {
                    rows.next();
                    assertThat(rows.getLong(1))
                            .as("one schema version covers both sources").isEqualTo(1);
                }
            }
        }
    }

    @Nested
    @DisplayName("input validation")
    class Validation {

        /** An unknown source must fail at construction, not produce a nonsense bronze row. */
        @Test
        @DisplayName("an unknown source is refused")
        void unknownSourceIsRefused() {
            assertThatThrownBy(() -> new BronzeIngestJob.Inputs(
                    Fixtures.yellow2024(), "blue", "blue/2024-01", target, runId, "owner"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("unknown source");
        }

        @Test
        @DisplayName("both known sources are accepted")
        void knownSourcesAreAccepted() {
            assertThat(new BronzeIngestJob.Inputs(Fixtures.yellow2024(), "yellow",
                    "u", target, runId, "o").source()).isEqualTo("yellow");
            assertThat(new BronzeIngestJob.Inputs(Fixtures.green2024(), "green",
                    "u", target, runId, "o").source()).isEqualTo("green");
        }
    }
}
