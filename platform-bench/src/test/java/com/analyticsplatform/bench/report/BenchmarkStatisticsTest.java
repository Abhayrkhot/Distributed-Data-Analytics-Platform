package com.analyticsplatform.bench.report;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;

import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * Statistics over known inputs.
 *
 * <p>Tested with fake durations because a bug here does not crash anything — it produces a false
 * number that ends up on a résumé. That is a worse failure than an exception, and the only defence
 * is checking the arithmetic against values computed by hand.
 */
class BenchmarkStatisticsTest {

    /** The canonical sample: mean 120, median 120, sd 15.81. */
    private static final List<Long> SAMPLE = List.of(100L, 110L, 120L, 130L, 140L);

    @Nested
    @DisplayName("central tendency")
    class CentralTendency {

        @Test
        @DisplayName("mean, median, min and max over a known sample")
        void knownSample() {
            BenchmarkStatistics stats = BenchmarkStatistics.of(SAMPLE);

            assertThat(stats.observations()).isEqualTo(5);
            assertThat(stats.meanMillis()).isCloseTo(120.0, within(0.001));
            assertThat(stats.medianMillis()).isCloseTo(120.0, within(0.001));
            assertThat(stats.minMillis()).isEqualTo(100);
            assertThat(stats.maxMillis()).isEqualTo(140);
        }

        @Test
        @DisplayName("an odd count takes the middle value")
        void oddCountMedian() {
            assertThat(BenchmarkStatistics.of(List.of(10L, 20L, 90L)).medianMillis())
                    .isCloseTo(20.0, within(0.001));
        }

        @Test
        @DisplayName("an even count averages the middle pair")
        void evenCountMedian() {
            assertThat(BenchmarkStatistics.of(List.of(10L, 20L, 30L, 40L)).medianMillis())
                    .isCloseTo(25.0, within(0.001));
        }

        @Test
        @DisplayName("input order does not matter")
        void orderIndependent() {
            assertThat(BenchmarkStatistics.of(List.of(140L, 100L, 130L, 110L, 120L)).medianMillis())
                    .isEqualTo(BenchmarkStatistics.of(SAMPLE).medianMillis());
        }

        @Test
        @DisplayName("a single observation is handled without dividing by zero")
        void singleObservation() {
            BenchmarkStatistics stats = BenchmarkStatistics.of(List.of(100L));

            assertThat(stats.observations()).isEqualTo(1);
            assertThat(stats.meanMillis()).isCloseTo(100.0, within(0.001));
            assertThat(stats.medianMillis()).isCloseTo(100.0, within(0.001));
            assertThat(stats.stddevMillis()).as("no spread with one point").isZero();
        }
    }

    @Nested
    @DisplayName("outliers")
    class Outliers {

        /**
         * The reason median is reported alongside mean. One disturbed run in Docker moves the mean
         * substantially and leaves the median alone; reporting only the mean would hide it.
         */
        @Test
        @DisplayName("an outlier moves the mean far more than the median")
        void outlierMovesMeanNotMedian() {
            BenchmarkStatistics clean = BenchmarkStatistics.of(SAMPLE);
            BenchmarkStatistics disturbed =
                    BenchmarkStatistics.of(List.of(100L, 110L, 120L, 130L, 5000L));

            assertThat(disturbed.medianMillis())
                    .as("median barely moves").isCloseTo(120.0, within(0.001));
            assertThat(disturbed.meanMillis())
                    .as("mean is dragged upward").isGreaterThan(clean.meanMillis() * 4);
        }

        @Test
        @DisplayName("a high coefficient of variation exposes disagreement between runs")
        void coefficientOfVariationSignalsSpread() {
            assertThat(BenchmarkStatistics.of(List.of(100L, 101L, 99L, 100L))
                    .coefficientOfVariation()).isLessThan(0.05);
            assertThat(BenchmarkStatistics.of(List.of(100L, 500L, 50L, 900L))
                    .coefficientOfVariation()).isGreaterThan(0.5);
        }

        @ParameterizedTest(name = "{0} observations meaningful = {1}")
        @CsvSource({"1, false", "4, false", "5, true", "10, true"})
        void meaningfulnessThreshold(int count, boolean expected) {
            List<Long> sample = new java.util.ArrayList<>();
            for (int i = 0; i < count; i++) {
                sample.add(100L + i);
            }

            assertThat(BenchmarkStatistics.of(sample).isStatisticallyMeaningful())
                    .isEqualTo(expected);
        }
    }

    @Nested
    @DisplayName("rejecting broken measurements")
    class Rejection {

        /**
         * A zero duration is a broken measurement, not an infinitely fast run. Averaging it in
         * would understate every configuration that contained one.
         */
        @ParameterizedTest(name = "a duration of {0}ms is refused")
        @CsvSource({"0", "-1", "-1000"})
        void nonPositiveDurationsRefused(long duration) {
            assertThatThrownBy(() -> BenchmarkStatistics.of(List.of(100L, duration)))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("broken measurement");
        }

        @Test
        @DisplayName("an empty sample is refused")
        void emptySampleRefused() {
            assertThatThrownBy(() -> BenchmarkStatistics.of(List.of()))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("empty sample");
            assertThatThrownBy(() -> BenchmarkStatistics.of(null))
                    .isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        @DisplayName("a null duration is refused")
        void nullDurationRefused() {
            List<Long> withNull = new java.util.ArrayList<>();
            withNull.add(100L);
            withNull.add(null);

            assertThatThrownBy(() -> BenchmarkStatistics.of(withNull))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("null duration");
        }
    }

    @Nested
    @DisplayName("improvement")
    class Improvement {

        @Test
        @DisplayName("a halved median is a 50% improvement")
        void halvedIsFiftyPercent() {
            BenchmarkStatistics baseline = BenchmarkStatistics.of(List.of(200L, 200L, 200L));
            BenchmarkStatistics faster = BenchmarkStatistics.of(List.of(100L, 100L, 100L));

            assertThat(faster.improvementOver(baseline)).isCloseTo(0.5, within(0.0001));
        }

        @Test
        @DisplayName("identical performance is zero improvement")
        void identicalIsZero() {
            BenchmarkStatistics stats = BenchmarkStatistics.of(SAMPLE);

            assertThat(stats.improvementOver(stats)).isCloseTo(0.0, within(0.0001));
        }

        /** A regression is a finding. Returning it negative rather than clamping keeps it visible. */
        @Test
        @DisplayName("a slower configuration reports a negative improvement")
        void regressionIsNegative() {
            BenchmarkStatistics baseline = BenchmarkStatistics.of(List.of(100L, 100L, 100L));
            BenchmarkStatistics slower = BenchmarkStatistics.of(List.of(150L, 150L, 150L));

            assertThat(slower.improvementOver(baseline)).isCloseTo(-0.5, within(0.0001));
        }

        /** Improvement is computed from medians, so an outlier cannot inflate the claim. */
        @Test
        @DisplayName("an outlier in the baseline does not inflate the reported improvement")
        void outlierDoesNotInflateClaim() {
            BenchmarkStatistics cleanBaseline =
                    BenchmarkStatistics.of(List.of(200L, 200L, 200L, 200L, 200L));
            BenchmarkStatistics disturbedBaseline =
                    BenchmarkStatistics.of(List.of(200L, 200L, 200L, 200L, 9000L));
            BenchmarkStatistics optimized =
                    BenchmarkStatistics.of(List.of(100L, 100L, 100L, 100L, 100L));

            assertThat(optimized.improvementOver(disturbedBaseline))
                    .as("median-based, so the outlier is ignored")
                    .isCloseTo(optimized.improvementOver(cleanBaseline), within(0.0001));
        }

        @Test
        @DisplayName("a null or zero baseline is refused")
        void invalidBaselineRefused() {
            BenchmarkStatistics stats = BenchmarkStatistics.of(SAMPLE);

            assertThatThrownBy(() -> stats.improvementOver(null))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Nested
    @DisplayName("throughput")
    class Throughput {

        @Test
        @DisplayName("rows per second from median duration")
        void rowsPerSecond() {
            BenchmarkStatistics stats = BenchmarkStatistics.of(List.of(1000L, 1000L, 1000L));

            assertThat(stats.rowsPerSecond(5000)).isPresent();
            assertThat(stats.rowsPerSecond(5000).getAsDouble())
                    .isCloseTo(5000.0, within(0.001));
        }

        @Test
        @DisplayName("no throughput without an input count")
        void noThroughputWithoutRows() {
            assertThat(BenchmarkStatistics.of(SAMPLE).rowsPerSecond(0)).isEmpty();
            assertThat(BenchmarkStatistics.of(SAMPLE).rowsPerSecond(-1)).isEmpty();
        }
    }

    @Test
    @DisplayName("describe reports every statistic, not just the flattering one")
    void describeIsComplete() {
        assertThat(BenchmarkStatistics.of(SAMPLE).describe())
                .contains("n=5", "mean=120ms", "median=120ms", "min=100ms", "max=140ms", "sd=", "cv=");
    }
}
