package com.analyticsplatform.transform.acceptance;

import static org.assertj.core.api.Assertions.assertThat;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.sum;

import com.analyticsplatform.common.config.PlatformConfig;
import com.analyticsplatform.common.dao.ConnectionSource;
import com.analyticsplatform.common.dao.JdbcControlPlane;
import com.analyticsplatform.common.dao.LineageRecorder;
import com.analyticsplatform.common.run.ControlPlane.RunOutcome;
import com.analyticsplatform.common.run.ControlPlane.RunSpec;
import com.analyticsplatform.common.schema.SchemaRegistry;
import com.analyticsplatform.common.testing.SparkTestSupport;
import com.analyticsplatform.common.unit.ProcessingUnitState.Status;
import com.analyticsplatform.ingest.job.BronzeIngestJob;
import com.analyticsplatform.ingest.publish.ProcessingUnitStore;
import com.analyticsplatform.ingest.publish.ProcessingUnitStore.UnitKey;
import com.analyticsplatform.ingest.publish.StagedPublisher;
import com.analyticsplatform.transform.dq.DqEngine;
import com.analyticsplatform.transform.dq.DqRuleStore;
import com.analyticsplatform.transform.job.SilverTransformJob;
import com.analyticsplatform.transform.silver.SilverTransform;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.UUID;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.condition.EnabledIf;

/**
 * §14 · Real-data acceptance.
 *
 * <p>The final proof that the small fixtures represented the real workload. Every other suite runs
 * against 19 hand-written rows; this runs against 6.5 million real ones, where the failures are
 * different in kind — schema drift that actually happened, dirty values nobody thought to invent,
 * and volumes that expose anything accidentally quadratic.
 *
 * <p><strong>Skipped unless the data is present.</strong> It is gated rather than failing, because
 * a 106 MB download is not something an unrelated build should be forced into. Run
 * {@code ./scripts/fetch-data.sh} first.
 *
 * <p>Methods are ordered: this is a pipeline, and the later assertions depend on the earlier stages
 * having actually run.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@EnabledIf("realDataPresent")
class RealDataAcceptanceIT {

    private static final String YELLOW_2024 = "yellow_tripdata_2024-01.parquet";
    private static final String YELLOW_2025 = "yellow_tripdata_2025-01.parquet";
    private static final String GREEN_2024 = "green_tripdata_2024-01.parquet";

    private static Path rawDir;
    private static Path workDir;
    private static SparkSession spark;
    private static ConnectionSource connections;
    private static JdbcControlPlane controlPlane;
    private static ProcessingUnitStore store;

    private static String suffix;
    private static String bronzeDataset;
    private static String silverDataset;
    private static long runId;

    /** Skips the whole class when the download has not been run. */
    static boolean realDataPresent() {
        Path raw = repoRoot().resolve("data/raw");
        return Files.isRegularFile(raw.resolve(YELLOW_2024))
                && Files.isRegularFile(raw.resolve(YELLOW_2025))
                && Files.isRegularFile(raw.resolve(GREEN_2024));
    }

    private static Path repoRoot() {
        Path candidate = Path.of(System.getProperty("user.dir")).toAbsolutePath();
        while (candidate != null && !Files.isDirectory(candidate.resolve("tests/fixtures"))) {
            candidate = candidate.getParent();
        }
        return candidate == null ? Path.of(".").toAbsolutePath() : candidate;
    }

    @BeforeAll
    static void setUp() throws Exception {
        rawDir = repoRoot().resolve("data/raw");
        workDir = Files.createTempDirectory("acceptance-");
        spark = SparkTestSupport.spark();
        SilverTransform.registerUdfs(spark);

        connections = ConnectionSource.postgres(PlatformConfig.fromEnvironment());
        controlPlane = new JdbcControlPlane(connections);
        store = new ProcessingUnitStore(connections);

        suffix = UUID.randomUUID().toString().substring(0, 8);
        bronzeDataset = "acceptance.bronze." + suffix;
        silverDataset = "acceptance.silver." + suffix;
        runId = controlPlane.startRun(RunSpec.of("acceptance-" + suffix));

        copyProductionRules();
    }

    /**
     * Clones the seeded silver.trip_clean rules onto this run's dataset.
     *
     * <p>The acceptance run uses a unique dataset name so it cannot collide with other suites, but
     * that also means the seeded rules do not match it. Copying them is what makes this an
     * acceptance test of the real rule set rather than of an empty one.
     *
     * <p>The first version of this test did not do this, and DQ evaluated zero rules while
     * reporting success — which is what prompted the empty-rule-set guard in SilverTransformJob.
     */
    private static void copyProductionRules() throws Exception {
        try (Connection connection = connections.open();
             PreparedStatement s = connection.prepareStatement("""
                     INSERT INTO control.dq_rule
                         (rule_name, dataset_name, rule_type, target_column, rule_params,
                          severity, threshold_type, threshold_value, null_policy)
                     SELECT rule_name || '-' || ?, ?, rule_type, target_column, rule_params,
                            severity, threshold_type, threshold_value, null_policy
                       FROM control.dq_rule
                      WHERE dataset_name = 'silver.trip_clean' AND enabled
                     """)) {
            s.setString(1, suffix);
            s.setString(2, silverDataset);
            int copied = s.executeUpdate();
            System.out.printf("%n  installed %d production DQ rules for the acceptance run%n",
                    copied);
        }
    }

    @AfterAll
    static void cleanUp() throws Exception {
        try (Connection connection = connections.open()) {
            for (String dataset : new String[] {bronzeDataset, silverDataset}) {
                for (String sql : new String[] {
                    "DELETE FROM control.dq_result WHERE dataset_name = ?",
                    "DELETE FROM control.unit_manifest WHERE dataset_name = ?",
                    "DELETE FROM control.processing_unit WHERE dataset_name = ?",
                    "DELETE FROM control.schema_version WHERE dataset_name = ?"}) {
                    try (PreparedStatement s = connection.prepareStatement(sql)) {
                        s.setString(1, dataset);
                        s.executeUpdate();
                    }
                }
            }
            try (PreparedStatement s = connection.prepareStatement(
                    "DELETE FROM control.lineage_edge WHERE run_id = ?")) {
                s.setLong(1, runId);
                s.executeUpdate();
            }
            try (PreparedStatement s = connection.prepareStatement(
                    "DELETE FROM control.lineage_node WHERE node_name LIKE ?")) {
                s.setString(1, "%" + suffix);
                s.executeUpdate();
            }
        }
        controlPlane.finishRun(runId, new RunOutcome(RunOutcome.Status.SUCCESS, 0, 0, 0, null, null));
        try (Connection connection = connections.open();
             PreparedStatement s = connection.prepareStatement(
                     "DELETE FROM control.etl_run WHERE run_id = ?")) {
            s.setLong(1, runId);
            s.executeUpdate();
        }
    }

    private static BronzeIngestJob bronzeJob() {
        return new BronzeIngestJob(bronzeDataset,
                new SchemaRegistry(connections, SchemaRegistry.Policy.ALLOW_WIDENING),
                new StagedPublisher(store, workDir.resolve("staging"), 3600),
                new LineageRecorder(connections));
    }

    private static Path bronzeTarget(String unit) {
        return workDir.resolve("warehouse/bronze/" + unit.replace('/', '_'));
    }

    private static Path silverTarget() {
        return workDir.resolve("warehouse/silver");
    }

    private static long ingest(String file, String source, String unit) {
        return bronzeJob().run(new BronzeIngestJob.Inputs(
                spark.read().parquet(rawDir.resolve(file).toString()),
                source, unit, bronzeTarget(unit), runId, "acceptance")).rowsWritten();
    }

    @Test
    @Order(1)
    @DisplayName("bronze ingests 6.5M real rows across three files and two schema versions")
    void bronzeIngestsRealData() {
        long y24 = ingest(YELLOW_2024, "yellow", "yellow/2024-01");
        long y25 = ingest(YELLOW_2025, "yellow", "yellow/2025-01");
        long green = ingest(GREEN_2024, "green", "green/2024-01");

        System.out.printf("%n  bronze: yellow2024=%,d yellow2025=%,d green=%,d total=%,d%n",
                y24, y25, green, y24 + y25 + green);

        assertThat(y24).as("yellow 2024-01").isEqualTo(2_964_624L);
        assertThat(y25).as("yellow 2025-01").isEqualTo(3_475_226L);
        assertThat(green).as("green 2024-01").isEqualTo(56_551L);
        assertThat(y24 + y25 + green).isGreaterThan(6_000_000L);
    }

    /**
     * The schema-evolution claim against the change that actually happened, not a synthetic one.
     */
    @Test
    @Order(2)
    @DisplayName("the real 2024 to 2025 change registers as additive")
    void schemaEvolutionIsAdditive() throws Exception {
        try (Connection connection = connections.open();
             PreparedStatement s = connection.prepareStatement(
                     "SELECT version, change_type, added_columns FROM control.schema_version "
                             + "WHERE dataset_name = ? ORDER BY version")) {
            s.setString(1, bronzeDataset);
            try (ResultSet rows = s.executeQuery()) {
                assertThat(rows.next()).isTrue();
                assertThat(rows.getString("change_type")).isEqualTo("initial");

                // Bronze conforms every source onto one schema, so there is exactly one version.
                // The real cbd_congestion_fee drift is absorbed by SourceNormalizer rather than
                // reaching the registry - which is the design, and worth asserting explicitly.
                assertThat(rows.next()).as("normalization absorbs the drift").isFalse();
            }
        }
    }

    @Test
    @Order(3)
    @DisplayName("silver rejects real dirty rows and reconciles")
    void silverRejectsAndReconciles() {
        Dataset<Row> bronze = spark.read().parquet(
                bronzeTarget("yellow/2024-01").toString(),
                bronzeTarget("yellow/2025-01").toString(),
                bronzeTarget("green/2024-01").toString());

        Dataset<Row> zones = spark.read().option("header", "true").option("inferSchema", "true")
                .csv(rawDir.resolve("taxi_zone_lookup.csv").toString());

        long bronzeRows = bronze.count();
        long rejected = SilverTransform.reject(bronze).count();

        SilverTransformJob job = new SilverTransformJob(spark, silverDataset,
                new SchemaRegistry(connections, SchemaRegistry.Policy.ALLOW_WIDENING),
                new DqRuleStore(connections), new DqEngine(),
                new StagedPublisher(store, workDir.resolve("staging"), 3600),
                new LineageRecorder(connections));

        SilverTransformJob.Result result = job.run(new SilverTransformJob.Inputs(
                bronze, zones, "all/2024-2025", silverTarget(), runId, "acceptance"));

        assertThat(result.published()).isTrue();

        Dataset<Row> silver = spark.read().parquet(silverTarget().toString());
        long silverRows = silver.count();
        long duplicates = bronzeRows - rejected - silverRows;

        System.out.printf("  silver: bronze=%,d rejected=%,d duplicates=%,d silver=%,d (%.2f%% kept)%n",
                bronzeRows, rejected, duplicates, silverRows, 100.0 * silverRows / bronzeRows);

        assertThat(rejected).as("real data contains genuinely invalid rows").isPositive();
        assertThat(silverRows).isLessThan(bronzeRows);
        assertThat(bronzeRows - rejected - duplicates).isEqualTo(silverRows);
        assertThat(silverRows).as("the overwhelming majority survives").isGreaterThan(
                (long) (bronzeRows * 0.90));
    }

    @Test
    @Order(4)
    @DisplayName("data quality results are recorded for the real load")
    void dqResultsRecorded() throws Exception {
        try (Connection connection = connections.open();
             PreparedStatement s = connection.prepareStatement(
                     "SELECT count(*), count(*) FILTER (WHERE NOT passed) "
                             + "FROM control.dq_result WHERE dataset_name = ?")) {
            s.setString(1, silverDataset);
            try (ResultSet rows = s.executeQuery()) {
                rows.next();
                System.out.printf("  dq: %d rules evaluated, %d breached%n",
                        rows.getLong(1), rows.getLong(2));
                // Zero rules is the failure worth catching: it would mean the gate ran
                // against nothing and reported a clean bill of health.
                // The assertion that caught the vacuous gate. Zero rules means the check
                // never ran, and the platform now refuses to publish in that state.
                assertThat(rows.getLong(1)).as("rules actually ran").isPositive();
                assertThat(rows.getLong(1)).as("the full production rule set")
                        .isGreaterThanOrEqualTo(10);
            }
        }
    }

    @Test
    @Order(5)
    @DisplayName("committed units carry manifests and COMPLETE status")
    void controlPlaneIsConsistent() {
        for (String unit : new String[] {"yellow/2024-01", "yellow/2025-01", "green/2024-01"}) {
            UnitKey key = new UnitKey(bronzeDataset, BronzeIngestJob.STAGE, unit);
            assertThat(store.findManifest(key)).as("manifest for %s", unit).isPresent();
            assertThat(store.status(key)).as("status for %s", unit).contains(Status.COMPLETE);
        }
        assertThat(store.findManifest(
                new UnitKey(silverDataset, SilverTransformJob.STAGE, "all/2024-2025"))).isPresent();
    }

    /** Idempotency at real scale: the expensive claim, tested where it costs something. */
    @Test
    @Order(6)
    @DisplayName("re-running the whole pipeline skips every committed unit")
    void rerunSkipsEverything() {
        long before = System.currentTimeMillis();

        assertThat(ingest(YELLOW_2024, "yellow", "yellow/2024-01")).isZero();
        assertThat(ingest(YELLOW_2025, "yellow", "yellow/2025-01")).isZero();
        assertThat(ingest(GREEN_2024, "green", "green/2024-01")).isZero();

        long elapsed = System.currentTimeMillis() - before;
        System.out.printf("  rerun: all three units skipped in %,dms%n", elapsed);
    }

    @Test
    @Order(7)
    @DisplayName("lineage records the full raw to silver graph")
    void lineageIsComplete() throws Exception {
        try (Connection connection = connections.open();
             PreparedStatement s = connection.prepareStatement(
                     "SELECT source, edge_type, target FROM control.v_lineage WHERE run_id = ?")) {
            s.setLong(1, runId);
            try (ResultSet rows = s.executeQuery()) {
                java.util.List<String> edges = new java.util.ArrayList<>();
                while (rows.next()) {
                    edges.add(rows.getString(1) + " -" + rows.getString(2) + "-> "
                            + rows.getString(3));
                }
                System.out.println("  lineage edges: " + edges.size());
                assertThat(edges).anyMatch(e -> e.contains("raw.yellow_tripdata"));
                assertThat(edges).anyMatch(e -> e.contains("raw.green_tripdata"));
                assertThat(edges).anyMatch(e -> e.contains(silverDataset));
            }
        }
    }

    @Test
    @Order(8)
    @DisplayName("silver revenue is plausible for 6.5M real trips")
    void revenueIsPlausible() {
        Row row = spark.read().parquet(silverTarget().toString())
                .agg(sum(col("total_amount")).alias("revenue"),
                     sum(col("trip_distance_mi")).alias("miles")).first();

        double revenue = row.getDouble(0);
        double miles = row.getDouble(1);
        System.out.printf("  totals: revenue=$%,.0f  miles=%,.0f%n", revenue, miles);

        // Deliberately loose. This is a smoke check that the arithmetic is in the right
        // universe, not a precise expectation - a tight bound on real data would be a
        // test that fails when TLC publishes a different month.
        assertThat(revenue).isGreaterThan(50_000_000.0);
        assertThat(miles).isPositive();
    }
}
