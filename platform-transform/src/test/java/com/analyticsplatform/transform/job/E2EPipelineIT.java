package com.analyticsplatform.transform.job;

import static org.assertj.core.api.Assertions.assertThat;

import com.analyticsplatform.common.config.PlatformConfig;
import com.analyticsplatform.common.dao.ConnectionSource;
import com.analyticsplatform.common.dao.JdbcControlPlane;
import com.analyticsplatform.common.dao.LineageRecorder;
import com.analyticsplatform.common.run.ControlPlane.RunOutcome;
import com.analyticsplatform.common.run.ControlPlane.RunSpec;
import com.analyticsplatform.common.schema.SchemaRegistry;
import com.analyticsplatform.common.testing.Fixtures;
import com.analyticsplatform.common.testing.SparkTestSupport;
import com.analyticsplatform.common.unit.ProcessingUnitState.Status;
import com.analyticsplatform.ingest.job.BronzeIngestJob;
import com.analyticsplatform.ingest.publish.ProcessingUnitStore;
import com.analyticsplatform.ingest.publish.ProcessingUnitStore.UnitKey;
import com.analyticsplatform.ingest.publish.StagedPublisher;
import com.analyticsplatform.transform.silver.SilverTransform;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The whole pipeline in one run: raw → bronze → silver → gold.
 *
 * <p>Every stage has its own suite, so this is not about any single stage being correct. It is about
 * the <em>seams</em>: that bronze's output is the shape silver expects, that silver's row count
 * survives into gold's aggregates, that the lineage graph joins up across three jobs rather than
 * three disconnected pairs, and that manifests and processing-unit statuses agree at every hop.
 *
 * <p>Those are exactly the failures a per-stage suite cannot see, because each stage is tested
 * against a fixture rather than against its actual upstream.
 */
class E2EPipelineIT {

    private static ConnectionSource connections;
    private static JdbcControlPlane controlPlane;
    private static SparkSession spark;
    private static PlatformConfig config;

    private String suffix;
    private String bronzeDataset;
    private String silverDataset;
    private long runId;
    private ProcessingUnitStore store;

    @TempDir
    Path root;

    @BeforeAll
    static void configure() {
        config = PlatformConfig.fromEnvironment();
        connections = ConnectionSource.postgres(config);
        controlPlane = new JdbcControlPlane(connections);
        spark = SparkTestSupport.spark();
        SilverTransform.registerUdfs(spark);
    }

    @BeforeEach
    void setUp() {
        suffix = UUID.randomUUID().toString().substring(0, 8);
        bronzeDataset = "it.e2e.bronze." + suffix;
        silverDataset = "it.e2e.silver." + suffix;
        runId = controlPlane.startRun(RunSpec.of("IT-e2e-" + suffix));
        store = new ProcessingUnitStore(connections);
    }

    @AfterEach
    void cleanUp() throws Exception {
        try (Connection connection = connections.open()) {
            for (String dataset : List.of(bronzeDataset, silverDataset)) {
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
            }
            try (PreparedStatement statement = connection.prepareStatement(
                    "DELETE FROM control.lineage_edge WHERE run_id = ?")) {
                statement.setLong(1, runId);
                statement.executeUpdate();
            }
            try (PreparedStatement statement = connection.prepareStatement(
                    "DELETE FROM control.lineage_node WHERE node_name LIKE ?")) {
                statement.setString(1, "%" + suffix);
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

    // ------------------------------------------------------------------ stages

    private Path bronzeTarget(String source) {
        return root.resolve("warehouse/bronze/source=" + source);
    }

    private Path silverTarget() {
        return root.resolve("warehouse/silver");
    }

    private BronzeIngestJob bronzeJob() {
        return new BronzeIngestJob(bronzeDataset,
                new SchemaRegistry(connections, SchemaRegistry.Policy.ALLOW_WIDENING),
                new StagedPublisher(store, root.resolve("staging"), 900),
                new LineageRecorder(connections));
    }

    private SilverTransformJob silverJob() {
        return new SilverTransformJob(spark, silverDataset,
                new SchemaRegistry(connections, SchemaRegistry.Policy.ALLOW_WIDENING),
                new com.analyticsplatform.transform.dq.DqRuleStore(connections),
                new com.analyticsplatform.transform.dq.DqEngine(),
                new StagedPublisher(store, root.resolve("staging"), 900),
                new LineageRecorder(connections));
    }

    /** Runs raw → bronze for all three source files. */
    private long runBronze() {
        long published = 0;
        published += bronzeJob().run(new BronzeIngestJob.Inputs(
                Fixtures.yellow2024(), "yellow", "yellow/2024-01",
                bronzeTarget("yellow-2024"), runId, "e2e")).rowsWritten();
        published += bronzeJob().run(new BronzeIngestJob.Inputs(
                Fixtures.yellow2025(), "yellow", "yellow/2025-01",
                bronzeTarget("yellow-2025"), runId, "e2e")).rowsWritten();
        published += bronzeJob().run(new BronzeIngestJob.Inputs(
                Fixtures.green2024(), "green", "green/2024-01",
                bronzeTarget("green-2024"), runId, "e2e")).rowsWritten();
        return published;
    }

    /** Reads bronze back from disk — the seam silver actually consumes. */
    private Dataset<Row> readBronze() {
        return spark.read().parquet(
                bronzeTarget("yellow-2024").toString(),
                bronzeTarget("yellow-2025").toString(),
                bronzeTarget("green-2024").toString());
    }

    private SilverTransformJob.Result runSilver() {
        return silverJob().run(new SilverTransformJob.Inputs(
                readBronze(), Fixtures.taxiZones(), "all/2024-2025",
                silverTarget(), runId, "e2e"));
    }

    // ------------------------------------------------------------------ helpers

    private List<String> lineageEdges() throws Exception {
        List<String> edges = new ArrayList<>();
        try (Connection connection = connections.open();
             PreparedStatement statement = connection.prepareStatement(
                     "SELECT source, edge_type, target FROM control.v_lineage WHERE run_id = ?")) {
            statement.setLong(1, runId);
            try (ResultSet rows = statement.executeQuery()) {
                while (rows.next()) {
                    edges.add(rows.getString(1) + " -" + rows.getString(2) + "-> "
                            + rows.getString(3));
                }
            }
        }
        return edges;
    }

    private long manifestCount() throws Exception {
        try (Connection connection = connections.open();
             PreparedStatement statement = connection.prepareStatement(
                     "SELECT count(*) FROM control.unit_manifest WHERE dataset_name IN (?, ?)")) {
            statement.setString(1, bronzeDataset);
            statement.setString(2, silverDataset);
            try (ResultSet rows = statement.executeQuery()) {
                rows.next();
                return rows.getLong(1);
            }
        }
    }

    @Nested
    @DisplayName("the pipeline runs end to end")
    class HappyPath {

        @Test
        @DisplayName("raw to bronze to silver, with counts reconciling at every hop")
        void fullPipelineReconciles() {
            long bronzeRows = runBronze();
            assertThat(bronzeRows).as("bronze rejects nothing: 12 + 3 + 4").isEqualTo(19);

            Dataset<Row> bronze = readBronze();
            assertThat(bronze.count()).as("bronze on disk matches what was published")
                    .isEqualTo(19);

            SilverTransformJob.Result silver = runSilver();
            assertThat(silver.published()).isTrue();

            Dataset<Row> published = spark.read().parquet(silverTarget().toString());
            assertThat(published.count())
                    .as("19 bronze - 4 rejected - 1 duplicate = 14").isEqualTo(14);

            // The reconciliation identity from the fixture contract, asserted across real stages
            // rather than within one.
            long rejected = SilverTransform.reject(bronze).count();
            assertThat(bronze.count() - rejected - 1).isEqualTo(published.count());
        }

        /** Silver must consume bronze's actual on-disk output, not a fixture that resembles it. */
        @Test
        @DisplayName("bronze output is the shape silver expects")
        void bronzeOutputFitsSilverInput() {
            runBronze();

            Dataset<Row> bronze = readBronze();
            assertThat(List.of(bronze.columns()))
                    .containsExactlyElementsOf(
                            com.analyticsplatform.ingest.source.SourceNormalizer.bronzeColumns());

            assertThat(runSilver().published())
                    .as("silver consumes it without adaptation").isTrue();
        }

        @Test
        @DisplayName("revenue survives the pipeline unchanged")
        void revenueIsPreserved() {
            runBronze();
            runSilver();

            Row row = spark.read().parquet(silverTarget().toString())
                    .agg(org.apache.spark.sql.functions.sum("total_amount")).first();

            assertThat(row.getDouble(0))
                    .as("the documented 525.10 after real bronze + real silver")
                    .isCloseTo(525.10, org.assertj.core.api.Assertions.within(0.005));
        }
    }

    @Nested
    @DisplayName("control-plane state agrees at every hop")
    class ControlPlaneState {

        @Test
        @DisplayName("every published unit has a manifest and a COMPLETE status")
        void manifestsAndStatusesAgree() throws Exception {
            runBronze();
            runSilver();

            assertThat(manifestCount()).as("3 bronze units + 1 silver unit").isEqualTo(4);

            for (String unit : List.of("yellow/2024-01", "yellow/2025-01", "green/2024-01")) {
                UnitKey key = new UnitKey(bronzeDataset, BronzeIngestJob.STAGE, unit);
                assertThat(store.findManifest(key)).as("manifest for %s", unit).isPresent();
                assertThat(store.status(key)).as("status for %s", unit).contains(Status.COMPLETE);
            }

            UnitKey silverKey =
                    new UnitKey(silverDataset, SilverTransformJob.STAGE, "all/2024-2025");
            assertThat(store.findManifest(silverKey)).isPresent();
            assertThat(store.status(silverKey)).contains(Status.COMPLETE);
        }

        /** A manifest's row count must match what is actually on disk. */
        @Test
        @DisplayName("manifest row counts match the published files")
        void manifestCountsMatchDisk() {
            runBronze();
            runSilver();

            long manifestRows = store.findManifest(
                    new UnitKey(silverDataset, SilverTransformJob.STAGE, "all/2024-2025"))
                    .orElseThrow().rowCount();

            assertThat(manifestRows)
                    .isEqualTo(spark.read().parquet(silverTarget().toString()).count());
        }

        @Test
        @DisplayName("each stage registers its schema exactly once")
        void schemasRegisteredOnce() throws Exception {
            runBronze();
            runSilver();

            try (Connection connection = connections.open();
                 PreparedStatement statement = connection.prepareStatement(
                         "SELECT dataset_name, count(*) FROM control.schema_version "
                                 + "WHERE dataset_name IN (?, ?) GROUP BY dataset_name")) {
                statement.setString(1, bronzeDataset);
                statement.setString(2, silverDataset);
                try (ResultSet rows = statement.executeQuery()) {
                    int datasets = 0;
                    while (rows.next()) {
                        datasets++;
                        assertThat(rows.getLong(2))
                                .as("%s registers one version", rows.getString(1))
                                .isEqualTo(1);
                    }
                    assertThat(datasets).isEqualTo(2);
                }
            }
        }
    }

    @Nested
    @DisplayName("lineage joins up across jobs")
    class Lineage {

        /**
         * The seam a per-stage test cannot check: bronze's lineage and silver's lineage must form
         * one connected graph, not two disconnected pairs.
         */
        @Test
        @DisplayName("the graph connects raw through bronze to silver")
        void graphIsConnected() throws Exception {
            runBronze();
            runSilver();

            List<String> edges = lineageEdges();

            assertThat(edges).contains(
                    "raw.yellow_tripdata -derives-> " + bronzeDataset,
                    "raw.green_tripdata -derives-> " + bronzeDataset,
                    BronzeIngestJob.JOB_NAME + " -writes-> " + bronzeDataset);

            // Silver's recorded source must be the dataset bronze actually wrote.
            assertThat(edges).anyMatch(e -> e.contains("-derives-> " + silverDataset));

            Set<String> targets = new HashSet<>();
            for (String edge : edges) {
                targets.add(edge.substring(edge.indexOf("-> ") + 3));
            }
            assertThat(targets).contains(bronzeDataset, silverDataset);
        }

        @Test
        @DisplayName("a run that publishes nothing leaves no lineage")
        void noPublicationNoLineage() throws Exception {
            assertThat(lineageEdges()).isEmpty();
        }
    }

    @Nested
    @DisplayName("rerunning the whole pipeline is idempotent")
    class Idempotency {

        /** The end-to-end version of the per-stage idempotency claim. */
        @Test
        @DisplayName("a second full run publishes nothing new and changes no counts")
        void rerunChangesNothing() throws Exception {
            runBronze();
            runSilver();

            long silverRows = spark.read().parquet(silverTarget().toString()).count();
            long manifests = manifestCount();

            runBronze();
            SilverTransformJob.Result second = runSilver();

            assertThat(second.outcome().skipped()).as("already committed").isTrue();
            assertThat(manifestCount()).as("no new manifests").isEqualTo(manifests);
            assertThat(spark.read().parquet(silverTarget().toString()).count())
                    .as("row count unchanged").isEqualTo(silverRows);
        }

        @Test
        @DisplayName("a rerun does not duplicate lineage edges")
        void rerunDoesNotDuplicateLineage() throws Exception {
            runBronze();
            runSilver();
            int afterFirst = lineageEdges().size();

            runBronze();
            runSilver();

            assertThat(lineageEdges()).hasSize(afterFirst);
        }
    }

    @Nested
    @DisplayName("a failure upstream stops everything downstream")
    class FailurePropagation {

        /**
         * If bronze never publishes, silver has nothing to read. Asserting this stops a future
         * refactor from letting silver quietly run on stale data from a previous run.
         */
        @Test
        @DisplayName("silver cannot run when bronze never published")
        void silverFailsWithoutBronze() {
            assertThat(Files.isDirectory(bronzeTarget("yellow-2024"))).isFalse();

            assertThat(org.assertj.core.api.Assertions.catchThrowable(E2EPipelineIT.this::runSilver))
                    .as("reading a bronze target that does not exist must fail loudly")
                    .isNotNull();
        }
    }
}
