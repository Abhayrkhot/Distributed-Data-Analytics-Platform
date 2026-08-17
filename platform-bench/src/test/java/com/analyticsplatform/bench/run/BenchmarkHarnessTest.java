package com.analyticsplatform.bench.run;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.analyticsplatform.bench.config.BenchmarkConfig;
import com.analyticsplatform.bench.report.BenchmarkReport;
import com.analyticsplatform.common.testing.SparkTestSupport;
import com.analyticsplatform.bench.run.BenchmarkHarness.Plan;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * The harness itself, driven by a trivial workload.
 *
 * <p>Testing the measurement apparatus matters as much as testing the pipeline: a harness that
 * silently includes warm-ups, or skips the correctness check on the first iteration, produces a
 * number that is wrong in a way nobody can see by inspection.
 */
class BenchmarkHarnessTest {

    private static final StructType SCHEMA = new StructType(new StructField[] {
        new StructField("k", DataTypes.StringType, true, Metadata.empty()),
        new StructField("v", DataTypes.LongType, true, Metadata.empty()),
    });

    private static Dataset<Row> data(long... values) {
        List<Row> rows = new ArrayList<>();
        for (long value : values) {
            rows.add(RowFactory.create("k" + value, value));
        }
        return SparkTestSupport.spark().createDataFrame(rows, SCHEMA);
    }

    private static CorrectnessGate.InputProfile profile() {
        return new CorrectnessGate.InputProfile("fixed-input", 3, 1000, 1);
    }

    private BenchmarkHarness harness() {
        return new BenchmarkHarness(SparkTestSupport.spark(), profile());
    }

    @Nested
    @DisplayName("execution plan")
    class ExecutionPlan {

        @Test
        @DisplayName("runs warm-ups plus measured iterations for every configuration")
        void runCounts() {
            List<BenchmarkConfig> configs =
                    List.of(BenchmarkConfig.naiveApp(), BenchmarkConfig.optimized());

            List<BenchmarkObservation> observations = harness().run(
                    configs, (spark, config) -> data(1, 2, 3), "naive_app", new Plan(1, 3));

            assertThat(observations).hasSize(2 * (1 + 3));
            assertThat(observations.stream().filter(BenchmarkObservation::warmup).count())
                    .isEqualTo(2);
            assertThat(observations.stream().filter(BenchmarkObservation::isMeasured).count())
                    .isEqualTo(6);
        }

        /** Warm-ups must all precede measurement, or one config pays the JIT cost on a live run. */
        @Test
        @DisplayName("every warm-up runs before any measured iteration")
        void warmupsComeFirst() {
            List<BenchmarkObservation> observations = harness().run(
                    List.of(BenchmarkConfig.naiveApp(), BenchmarkConfig.optimized()),
                    (spark, config) -> data(1, 2, 3), "naive_app", new Plan(1, 2));

            int lastWarmup = -1;
            int firstMeasured = Integer.MAX_VALUE;
            for (BenchmarkObservation observation : observations) {
                if (observation.warmup()) {
                    lastWarmup = Math.max(lastWarmup, observation.sequencePosition());
                } else {
                    firstMeasured = Math.min(firstMeasured, observation.sequencePosition());
                }
            }

            assertThat(lastWarmup).isLessThan(firstMeasured);
        }

        /**
         * Running all of A then all of B lets cache and thermal drift be attributed to the
         * configuration rather than to time. Alternating spreads it evenly.
         */
        @Test
        @DisplayName("configuration order alternates between iterations")
        void orderAlternates() {
            List<BenchmarkObservation> measured = harness().run(
                    List.of(BenchmarkConfig.naiveApp(), BenchmarkConfig.optimized()),
                    (spark, config) -> data(1, 2, 3), "naive_app", new Plan(0, 2))
                    .stream().filter(o -> !o.warmup()).toList();

            String firstOfIteration0 = measured.stream()
                    .filter(o -> o.iterationIndex() == 0)
                    .min(java.util.Comparator.comparingInt(BenchmarkObservation::sequencePosition))
                    .orElseThrow().config().label();
            String firstOfIteration1 = measured.stream()
                    .filter(o -> o.iterationIndex() == 1)
                    .min(java.util.Comparator.comparingInt(BenchmarkObservation::sequencePosition))
                    .orElseThrow().config().label();

            assertThat(firstOfIteration1)
                    .as("the same config must not always go first")
                    .isNotEqualTo(firstOfIteration0);
        }

        @Test
        @DisplayName("sequence positions are unique and ordered")
        void sequenceIsAuditable() {
            List<BenchmarkObservation> observations = harness().run(
                    List.of(BenchmarkConfig.naiveApp(), BenchmarkConfig.optimized()),
                    (spark, config) -> data(1, 2, 3), "naive_app", new Plan(1, 2));

            List<Integer> positions = observations.stream()
                    .map(BenchmarkObservation::sequencePosition).sorted().toList();

            assertThat(positions).doesNotHaveDuplicates();
            assertThat(positions.get(0)).isZero();
            assertThat(positions.get(positions.size() - 1)).isEqualTo(observations.size() - 1);
        }

        @ParameterizedTest(name = "an invalid plan ({0} warm-ups, {1} iterations) is refused")
        @CsvSource({"-1, 5", "1, 0", "0, -3"})
        void invalidPlansRefused(int warmups, int iterations) {
            assertThatThrownBy(() -> new Plan(warmups, iterations))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Nested
    @DisplayName("correctness gate")
    class Correctness {

        @Test
        @DisplayName("identical output passes for every configuration")
        void identicalOutputPasses() {
            List<BenchmarkObservation> observations = harness().run(
                    List.of(BenchmarkConfig.naiveApp(), BenchmarkConfig.optimized()),
                    (spark, config) -> data(1, 2, 3), "naive_app", new Plan(0, 2));

            assertThat(observations).allMatch(BenchmarkObservation::correctnessPassed);
            assertThat(new BenchmarkReport(observations).isValid()).isTrue();
        }

        /**
         * The case the gate exists for: a configuration that is faster because it computed
         * something else. It must be rejected, not reported.
         */
        @Test
        @DisplayName("a configuration producing different output fails the gate")
        void divergentOutputFails() {
            List<BenchmarkObservation> observations = harness().run(
                    List.of(BenchmarkConfig.naiveApp(), BenchmarkConfig.optimized()),
                    // "optimized" quietly processes less data - the classic false win.
                    (spark, config) -> config.label().equals("optimized")
                            ? data(1, 2)
                            : data(1, 2, 3),
                    "naive_app", new Plan(0, 2));

            assertThat(observations.stream()
                    .filter(o -> o.config().label().equals("optimized"))
                    .allMatch(o -> !o.correctnessPassed()))
                    .as("every optimized run is marked incorrect").isTrue();

            BenchmarkReport report = new BenchmarkReport(observations);
            assertThat(report.isValid()).isFalse();
            assertThat(report.headline()).as("no headline from a divergent run").isEmpty();
        }

        /**
         * With alternating order, a non-baseline config can run before the baseline in the first
         * iteration. Those runs must still be verified once the baseline is known — otherwise an
         * unverified run is silently counted as correct.
         */
        @Test
        @DisplayName("runs measured before the baseline are still verified afterwards")
        void earlyRunsAreReconciled() {
            List<BenchmarkObservation> observations = harness().run(
                    // Baseline second, so the other config is measured first.
                    List.of(BenchmarkConfig.optimized(), BenchmarkConfig.naiveApp()),
                    (spark, config) -> config.label().equals("optimized")
                            ? data(9, 9, 9)
                            : data(1, 2, 3),
                    "naive_app", new Plan(0, 1));

            assertThat(observations.stream()
                    .filter(o -> o.config().label().equals("optimized"))
                    .allMatch(o -> !o.correctnessPassed()))
                    .as("the early run was reconciled and found divergent").isTrue();
        }

        /** Order and partitioning change under these configs; the hash must not react to them. */
        @Test
        @DisplayName("row order and partition count do not affect the fingerprint")
        void fingerprintIsOrderIndependent() {
            Dataset<Row> forward = data(1, 2, 3, 4, 5);
            Dataset<Row> shuffled = data(5, 3, 1, 4, 2);

            assertThat(CorrectnessGate.contentHash(shuffled))
                    .isEqualTo(CorrectnessGate.contentHash(forward));
            assertThat(CorrectnessGate.contentHash(forward.repartition(7)))
                    .isEqualTo(CorrectnessGate.contentHash(forward.repartition(1)));
        }

        @Test
        @DisplayName("differing content changes the fingerprint")
        void differingContentDiffers() {
            assertThat(CorrectnessGate.contentHash(data(1, 2, 3)))
                    .isNotEqualTo(CorrectnessGate.contentHash(data(1, 2, 4)));
            assertThat(CorrectnessGate.contentHash(data(1, 2, 3)))
                    .isNotEqualTo(CorrectnessGate.contentHash(data(1, 2)));
        }

        @Test
        @DisplayName("compare names the difference it found")
        void compareDescribesTheDifference() {
            assertThat(CorrectnessGate.compare("base", data(1, 2, 3), "cand", data(1, 2, 3)))
                    .isEmpty();
            assertThat(CorrectnessGate.compare("base", data(1, 2, 3), "cand", data(1, 2)))
                    .isPresent()
                    .get().asString().contains("different output");
        }
    }

    @Nested
    @DisplayName("configuration validation")
    class ConfigValidation {

        /**
         * Compression in an execution config makes the resulting percentage unattributable —
         * it is no longer a measurement of execution tuning at all.
         */
        @Test
        @DisplayName("compression in an Experiment A config is refused")
        void compressionRefusedInExecutionExperiment() {
            assertThatThrownBy(() -> new BenchmarkConfig("bad",
                    BenchmarkConfig.Experiment.A_EXECUTION,
                    java.util.Map.of("spark.sql.parquet.compression.codec", "zstd"),
                    false, false, false))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("compression belongs to Experiment B");
        }

        @Test
        @DisplayName("compression is permitted in Experiment B")
        void compressionAllowedInStorageExperiment() {
            assertThat(BenchmarkConfig.storage("zstd_partitioned", "zstd", true).sparkSettings())
                    .containsKey("spark.sql.parquet.compression.codec");
        }

        /**
         * AQE is on by default in Spark 3.5, so a baseline that disables it is a handicap rather
         * than a default. Asserting this stops the honest baseline being quietly weakened later.
         */
        @Test
        @DisplayName("the default baseline leaves AQE enabled")
        void defaultBaselineKeepsAqeOn() {
            assertThat(BenchmarkConfig.sparkDefault().sparkSettings())
                    .containsEntry("spark.sql.adaptive.enabled", "true");
            assertThat(BenchmarkConfig.naiveApp().sparkSettings())
                    .containsEntry("spark.sql.adaptive.enabled", "true");
        }

        @Test
        @DisplayName("the ablation ladder builds cumulatively from the baseline")
        void ladderIsCumulative() {
            List<BenchmarkConfig> ladder = BenchmarkConfig.ablationLadder();

            assertThat(ladder).hasSize(7);
            assertThat(ladder.get(0).label()).isEqualTo("B0_baseline");
            assertThat(ladder.get(0).partitionPruning()).isFalse();
            assertThat(ladder.get(ladder.size() - 1).partitionPruning()).isTrue();
            assertThat(ladder.get(ladder.size() - 1).broadcastHint()).isTrue();
        }

        @Test
        @DisplayName("leave-one-out removes exactly one change at a time")
        void leaveOneOutRemovesOne() {
            List<BenchmarkConfig> variants = BenchmarkConfig.leaveOneOut();

            assertThat(variants).hasSize(4);
            assertThat(variants).anyMatch(v -> !v.partitionPruning() && v.columnPruning());
            assertThat(variants).anyMatch(v -> v.partitionPruning() && !v.columnPruning());
            assertThat(variants).anyMatch(v -> !v.broadcastHint());
        }
    }
}
