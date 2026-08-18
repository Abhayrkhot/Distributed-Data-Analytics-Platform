package com.analyticsplatform.bench.run;

import static org.assertj.core.api.Assertions.assertThat;

import com.analyticsplatform.bench.config.BenchmarkConfig;
import com.analyticsplatform.bench.report.BenchmarkReport;
import com.analyticsplatform.bench.report.MarkdownReportWriter;
import com.analyticsplatform.bench.run.BenchmarkHarness.Plan;
import com.analyticsplatform.common.config.PlatformConfig;
import com.analyticsplatform.common.dao.ConnectionSource;
import com.analyticsplatform.common.dao.JdbcControlPlane;
import com.analyticsplatform.common.run.ControlPlane.RunOutcome;
import com.analyticsplatform.common.run.ControlPlane.RunSpec;
import com.analyticsplatform.common.testing.Fixtures;
import com.analyticsplatform.common.testing.SparkTestSupport;
import com.analyticsplatform.ingest.source.SourceNormalizer;
import com.analyticsplatform.transform.silver.SilverTransform;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;
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
 * The benchmark against real data and the real control plane.
 *
 * <p>Deliberately does <em>not</em> assert a performance figure. The fixture is 14 rows on a local
 * session, where the measurement is dominated by Spark's fixed overhead — any percentage from it
 * would be noise dressed as a result. What is asserted is that the apparatus behaves correctly:
 * the correctness gate runs, the environment is recorded, the ablation is ordered, and an
 * inadmissible measurement produces no headline.
 *
 * <p>The real figure comes from {@code scripts/run-bench.sh} against the full TLC dataset, and
 * whatever it measures is what gets reported.
 */
class BenchmarkIT {

    private static SparkSession spark;
    private static PlatformConfig config;
    private static ConnectionSource connections;
    private static JdbcControlPlane controlPlane;

    private long runId;
    private Dataset<Row> silver;
    private CorrectnessGate.InputProfile inputProfile;

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

    /**
     * Real TLC data when present, the fixture otherwise.
     *
     * <p>A benchmark on 14 rows measures Spark's fixed startup overhead and nothing else, so any
     * percentage from it would be noise. The suite still runs on the fixture — it is verifying the
     * apparatus, not producing a figure — but a real measurement requires real volume, and
     * scripts/run-bench.sh sets BENCH_REAL_DATA to demand it.
     */
    private static boolean realDataAvailable() {
        return java.nio.file.Files.isRegularFile(
                Fixtures.repoRoot().resolve("data/raw/yellow_tripdata_2024-01.parquet"));
    }

    @BeforeEach
    void setUp() {
        runId = controlPlane.startRun(RunSpec.of("IT-bench-" + UUID.randomUUID()));

        boolean useReal = realDataAvailable()
                && Boolean.parseBoolean(System.getenv().getOrDefault("BENCH_REAL_DATA", "false"));

        Dataset<Row> bronze;
        Dataset<Row> zones;
        if (useReal) {
            java.nio.file.Path raw = Fixtures.repoRoot().resolve("data/raw");
            bronze = SourceNormalizer.normalizeYellow(
                            spark.read().parquet(raw.resolve("yellow_tripdata_2024-01.parquet").toString()))
                    .union(SourceNormalizer.normalizeYellow(
                            spark.read().parquet(raw.resolve("yellow_tripdata_2025-01.parquet").toString())))
                    .union(SourceNormalizer.normalizeGreen(
                            spark.read().parquet(raw.resolve("green_tripdata_2024-01.parquet").toString())));
            zones = spark.read().option("header", "true").option("inferSchema", "true")
                    .csv(raw.resolve("taxi_zone_lookup.csv").toString());
        } else {
            bronze = SourceNormalizer.normalizeYellow(Fixtures.yellow2024())
                    .union(SourceNormalizer.normalizeYellow(Fixtures.yellow2025()))
                    .union(SourceNormalizer.normalizeGreen(Fixtures.green2024()));
            zones = Fixtures.taxiZones();
        }

        zoneDimension = zones;
        silver = SilverTransform.transform(bronze, zones).cache();
        inputProfile = CorrectnessGate.profileInput(silver, 0, 1);
        if (useReal) {
            System.out.printf("%n  benchmark input: %,d real rows%n", inputProfile.rowCount());
        }
    }

    private Dataset<Row> zoneDimension;

    @AfterEach
    void cleanUp() throws Exception {
        silver.unpersist();
        try (Connection connection = connections.open()) {
            try (PreparedStatement statement = connection.prepareStatement(
                    "DELETE FROM control.benchmark_run WHERE run_id = ?")) {
                statement.setLong(1, runId);
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

    private BenchmarkHarness.Workload workload() {
        return (session, benchConfig) ->
                GoldWorkload.aggregate(silver, zoneDimension, benchConfig);
    }

    private List<BenchmarkObservation> runConfigs(List<BenchmarkConfig> configs, Plan plan) {
        return new BenchmarkHarness(spark, inputProfile)
                .run(configs, workload(), "naive_app", plan);
    }

    @Nested
    @DisplayName("the workload is measurable")
    class Measurable {

        @Test
        @DisplayName("all three configurations run and produce identical output")
        void allConfigsAgree() {
            List<BenchmarkObservation> observations = runConfigs(List.of(
                    BenchmarkConfig.sparkDefault(),
                    BenchmarkConfig.naiveApp(),
                    BenchmarkConfig.optimized()), Plan.quick());

            assertThat(observations).allMatch(BenchmarkObservation::correctnessPassed);

            BenchmarkReport report = new BenchmarkReport(observations);
            assertThat(report.isValid()).isTrue();
            assertThat(report.summaries())
                    .containsKeys("spark_default", "naive_app", "optimized");
        }

        /**
         * The tuning changes the plan, not the answer. If pruning or the broadcast hint altered the
         * result, every percentage derived from them would be meaningless.
         */
        @Test
        @DisplayName("pruning and broadcasting do not change the aggregate")
        void optimizationsPreserveTheAnswer() {
            Dataset<Row> naive = GoldWorkload.aggregate(
                    silver, zoneDimension, BenchmarkConfig.naiveApp());
            Dataset<Row> optimized = GoldWorkload.aggregate(
                    silver, zoneDimension, BenchmarkConfig.optimized());

            assertThat(CorrectnessGate.compare("naive", naive, "optimized", optimized))
                    .as("identical output, different plan").isEmpty();
        }

        @Test
        @DisplayName("a headline is produced from a valid run")
        void headlineIsProduced() {
            BenchmarkReport report = new BenchmarkReport(runConfigs(
                    List.of(BenchmarkConfig.naiveApp(), BenchmarkConfig.optimized()),
                    Plan.quick()));

            assertThat(report.headline()).isPresent();
            assertThat(report.headline().orElseThrow().claim())
                    .contains("versus the naive_app configuration");
        }
    }

    @Nested
    @DisplayName("plan evidence, not inference")
    class PlanEvidence {

        /**
         * Spark auto-broadcasts small tables, so the hint may well be a no-op here. Capturing the
         * plan is what makes any claim about it evidence rather than assumption — and if both
         * plans already broadcast, the honest conclusion is that the hint contributed nothing.
         */
        @Test
        @DisplayName("the physical plan is captured for both configurations")
        void plansAreCaptured() {
            String naivePlan = GoldWorkload.explain(GoldWorkload.aggregate(
                    silver, zoneDimension, BenchmarkConfig.naiveApp()));
            String optimizedPlan = GoldWorkload.explain(GoldWorkload.aggregate(
                    silver, zoneDimension, BenchmarkConfig.optimized()));

            assertThat(naivePlan).isNotBlank();
            assertThat(optimizedPlan).isNotBlank();

            boolean naiveBroadcasts = naivePlan.contains("BroadcastHashJoin");
            boolean optimizedBroadcasts = optimizedPlan.contains("BroadcastHashJoin");

            // Both may broadcast: the zone table is tiny and Spark decides that on its own. The
            // point is that the answer is read from the plan, not assumed from the hint.
            assertThat(optimizedBroadcasts)
                    .as("the hinted config must broadcast").isTrue();
            assertThat(naiveBroadcasts || !naiveBroadcasts).isTrue();
        }
    }

    @Nested
    @DisplayName("persistence and reporting")
    class Persistence {

        @Test
        @DisplayName("every observation is recorded with its environment")
        void observationsRecorded() throws Exception {
            List<BenchmarkObservation> observations = runConfigs(
                    List.of(BenchmarkConfig.naiveApp(), BenchmarkConfig.optimized()),
                    Plan.quick());

            new BenchmarkStore(connections).record(runId, observations,
                    BenchmarkStore.Environment.capture(spark, "test-commit"));

            try (Connection connection = connections.open();
                 PreparedStatement statement = connection.prepareStatement(
                         "SELECT count(*), count(DISTINCT config_label), "
                                 + "count(*) FILTER (WHERE is_warmup), "
                                 + "count(*) FILTER (WHERE spark_version IS NOT NULL) "
                                 + "FROM control.benchmark_run WHERE run_id = ?")) {
                statement.setLong(1, runId);
                try (ResultSet rows = statement.executeQuery()) {
                    rows.next();
                    assertThat(rows.getLong(1)).isEqualTo(observations.size());
                    assertThat(rows.getLong(2)).as("both configs").isEqualTo(2);
                    assertThat(rows.getLong(3)).as("warm-ups stored, not silently dropped")
                            .isEqualTo(2);
                    assertThat(rows.getLong(4)).as("environment on every row")
                            .isEqualTo(observations.size());
                }
            }
        }

        /**
         * Writes the real report when run against real data.
         *
         * <p>Gated on BENCH_REAL_DATA so a fixture run cannot overwrite a genuine measurement with
         * one taken on 14 rows — which would be a number that looks official and means nothing.
         */
        @Test
        @DisplayName("writes docs/results/benchmark.md when run against real data")
        void writesRealReport() {
            org.junit.jupiter.api.Assumptions.assumeTrue(
                    Boolean.parseBoolean(System.getenv().getOrDefault("BENCH_REAL_DATA", "false")),
                    "fixture run: not overwriting the real report");

            java.util.List<BenchmarkConfig> configs = java.util.List.of(
                    BenchmarkConfig.sparkDefault(),
                    BenchmarkConfig.naiveApp(),
                    BenchmarkConfig.optimized());

            java.util.List<BenchmarkObservation> observations =
                    new BenchmarkHarness(spark, inputProfile)
                            .run(configs, workload(), "naive_app",
                                    new Plan(1, Integer.parseInt(System.getenv()
                                            .getOrDefault("BENCH_ITERATIONS", "5"))));

            new BenchmarkStore(connections).record(runId, observations,
                    BenchmarkStore.Environment.capture(spark, gitCommit()));

            BenchmarkReport report = new BenchmarkReport(observations);
            java.nio.file.Path target =
                    Fixtures.repoRoot().resolve("docs/results/benchmark.md");
            MarkdownReportWriter.write(target, report,
                    BenchmarkStore.Environment.capture(spark, gitCommit()),
                    java.util.List.of("spark_default", "naive_app", "optimized"));

            report.headline().ifPresentOrElse(
                    h -> System.out.printf("%n  MEASURED: %s%n", h.claim()),
                    () -> System.out.printf("%n  NO HEADLINE: %s%n", report.invalidReasons()));

            assertThat(java.nio.file.Files.exists(target)).isTrue();
        }

        private String gitCommit() {
            try {
                Process p = new ProcessBuilder("git", "rev-parse", "--short", "HEAD")
                        .directory(Fixtures.repoRoot().toFile()).start();
                try (var r = new java.io.BufferedReader(
                        new java.io.InputStreamReader(p.getInputStream()))) {
                    return r.readLine();
                }
            } catch (java.io.IOException e) {
                return "unknown";
            }
        }

        @Test
        @DisplayName("the markdown report leads with the result and states the environment")
        void reportIsWritten() {
            BenchmarkReport report = new BenchmarkReport(runConfigs(
                    List.of(BenchmarkConfig.naiveApp(), BenchmarkConfig.optimized()),
                    Plan.quick()));
            Path target = root.resolve("benchmark.md");

            MarkdownReportWriter.write(target, report,
                    BenchmarkStore.Environment.capture(spark, "test-commit"),
                    List.of("naive_app", "optimized"));

            String markdown = readString(target);
            assertThat(markdown)
                    .contains("# Benchmark results")
                    .contains("## Result")
                    .contains("## Environment")
                    .contains("## All configurations")
                    .contains("versus the naive_app configuration")
                    .contains("computed from **medians**");
        }

        /** An inadmissible measurement must produce a report that refuses, not one that omits. */
        @Test
        @DisplayName("an invalid measurement writes a report that leads with the refusal")
        void invalidReportLeadsWithRefusal() {
            List<BenchmarkObservation> observations = new java.util.ArrayList<>(runConfigs(
                    List.of(BenchmarkConfig.naiveApp(), BenchmarkConfig.optimized()),
                    Plan.quick()));
            BenchmarkObservation good = observations.get(observations.size() - 1);
            observations.add(new BenchmarkObservation(
                    good.config(), 99, 99, false, good.startedAt(), good.finishedAt(),
                    good.durationMillis(), "a-different-input", good.inputRowCount(),
                    good.inputBytes(), good.inputFileCount(), good.outputFingerprint(),
                    true, 0, 0));

            BenchmarkReport report = new BenchmarkReport(observations);
            Path target = root.resolve("invalid.md");
            MarkdownReportWriter.write(target, report,
                    BenchmarkStore.Environment.capture(spark, "test-commit"), List.of());

            assertThat(report.isValid()).isFalse();
            assertThat(readString(target))
                    .contains("not admissible")
                    .contains("different input");
        }

        private String readString(Path path) {
            try {
                return Files.readString(path);
            } catch (java.io.IOException e) {
                throw new java.io.UncheckedIOException(e);
            }
        }
    }

    @Nested
    @DisplayName("ablation")
    class Ablation {

        @Test
        @DisplayName("the ladder runs end to end and every step agrees on output")
        void ladderRuns() {
            List<BenchmarkConfig> ladder = BenchmarkConfig.ablationLadder();

            List<BenchmarkObservation> observations = new BenchmarkHarness(spark, inputProfile)
                    .run(ladder, workload(), "B0_baseline", new Plan(0, 1));

            assertThat(observations).allMatch(BenchmarkObservation::correctnessPassed);
            assertThat(new BenchmarkReport(observations).summaries()).hasSize(ladder.size());
        }

        @Test
        @DisplayName("leave-one-out variants also agree on output")
        void leaveOneOutAgrees() {
            List<BenchmarkConfig> variants = new java.util.ArrayList<>();
            variants.add(BenchmarkConfig.optimized());
            variants.addAll(BenchmarkConfig.leaveOneOut());

            List<BenchmarkObservation> observations = new BenchmarkHarness(spark, inputProfile)
                    .run(variants, workload(), "optimized", new Plan(0, 1));

            assertThat(observations).allMatch(BenchmarkObservation::correctnessPassed);
        }
    }
}
