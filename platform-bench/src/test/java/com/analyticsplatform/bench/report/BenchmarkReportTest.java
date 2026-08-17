package com.analyticsplatform.bench.report;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

import com.analyticsplatform.bench.config.BenchmarkConfig;
import com.analyticsplatform.bench.run.BenchmarkObservation;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * The reporter's refusal conditions.
 *
 * <p>This is the class most able to produce a false claim, because its job is to turn numbers into
 * a percentage someone will quote. Most of these tests check that it <em>declines</em>.
 */
class BenchmarkReportTest {

    private static final String FINGERPRINT = "input-abc123";

    private static BenchmarkObservation observation(
            BenchmarkConfig config, int iteration, long durationMillis,
            boolean warmup, boolean correctnessPassed, String fingerprint) {

        Instant start = Instant.parse("2026-01-01T00:00:00Z");
        return new BenchmarkObservation(config, iteration, iteration, warmup,
                start, start.plusMillis(durationMillis), durationMillis,
                fingerprint, 1000, 50_000, 4, "output-x", correctnessPassed, 12, 500_000);
    }

    private static List<BenchmarkObservation> measured(
            BenchmarkConfig config, long... durations) {
        List<BenchmarkObservation> out = new ArrayList<>();
        for (int i = 0; i < durations.length; i++) {
            out.add(observation(config, i, durations[i], false, true, FINGERPRINT));
        }
        return out;
    }

    @Nested
    @DisplayName("a valid measurement produces a headline")
    class Valid {

        @Test
        @DisplayName("headline names its baseline and reports the measured improvement")
        void headlineIsProduced() {
            List<BenchmarkObservation> all = new ArrayList<>();
            all.addAll(measured(BenchmarkConfig.naiveApp(), 200, 200, 200, 200, 200));
            all.addAll(measured(BenchmarkConfig.optimized(), 100, 100, 100, 100, 100));

            BenchmarkReport report = new BenchmarkReport(all);

            assertThat(report.isValid()).isTrue();
            assertThat(report.headline()).isPresent();
            assertThat(report.headline().orElseThrow().improvementFraction())
                    .isCloseTo(0.5, within(0.0001));
            assertThat(report.headline().orElseThrow().claim())
                    .contains("50.0%")
                    .contains("versus the naive_app configuration");
        }

        @Test
        @DisplayName("every configuration is summarized, not only the flattering pair")
        void allConfigsSummarized() {
            List<BenchmarkObservation> all = new ArrayList<>();
            all.addAll(measured(BenchmarkConfig.sparkDefault(), 180, 180, 180));
            all.addAll(measured(BenchmarkConfig.naiveApp(), 200, 200, 200));
            all.addAll(measured(BenchmarkConfig.optimized(), 100, 100, 100));

            assertThat(new BenchmarkReport(all).summaries())
                    .containsKeys("spark_default", "naive_app", "optimized");
        }

        /** Warm-ups must not reach the statistics: they measure JIT, not the workload. */
        @Test
        @DisplayName("warm-up runs are excluded from the statistics")
        void warmupsExcluded() {
            List<BenchmarkObservation> all = new ArrayList<>();
            all.add(observation(BenchmarkConfig.optimized(), 0, 9999, true, true, FINGERPRINT));
            all.addAll(measured(BenchmarkConfig.optimized(), 100, 100, 100));

            BenchmarkReport report = new BenchmarkReport(all);

            assertThat(report.summaryFor("optimized")).isPresent();
            assertThat(report.summaryFor("optimized").orElseThrow().statistics().observations())
                    .as("3 measured, the warm-up dropped").isEqualTo(3);
            assertThat(report.summaryFor("optimized").orElseThrow().statistics().maxMillis())
                    .as("the 9999ms warm-up never reached the stats").isEqualTo(100);
        }
    }

    @Nested
    @DisplayName("the reporter refuses when the measurement is inadmissible")
    class Refusal {

        /**
         * The most seductive error: a configuration that looks faster because it was quietly given
         * less data. Differing fingerprints make the comparison meaningless however clean the
         * numbers appear.
         */
        @Test
        @DisplayName("differing input fingerprints invalidate the comparison")
        void differingInputRefused() {
            List<BenchmarkObservation> all = new ArrayList<>();
            all.addAll(measured(BenchmarkConfig.naiveApp(), 200, 200, 200));
            all.add(observation(BenchmarkConfig.optimized(), 0, 100, false, true, "different-input"));

            BenchmarkReport report = new BenchmarkReport(all);

            assertThat(report.isValid()).isFalse();
            assertThat(report.headline()).as("no headline from an invalid comparison").isEmpty();
            assertThat(report.invalidReasons())
                    .anyMatch(r -> r.contains("different input"));
        }

        /** Being faster at producing different output is not an achievement. */
        @Test
        @DisplayName("a failed correctness gate invalidates the report")
        void correctnessFailureRefused() {
            List<BenchmarkObservation> all = new ArrayList<>();
            all.addAll(measured(BenchmarkConfig.naiveApp(), 200, 200, 200));
            all.add(observation(BenchmarkConfig.optimized(), 0, 100, false, false, FINGERPRINT));

            BenchmarkReport report = new BenchmarkReport(all);

            assertThat(report.isValid()).isFalse();
            assertThat(report.headline()).isEmpty();
            assertThat(report.invalidReasons())
                    .anyMatch(r -> r.contains("correctness gate failed"));
        }

        @Test
        @DisplayName("warm-ups alone produce no report")
        void warmupsOnlyRefused() {
            BenchmarkReport report = new BenchmarkReport(List.of(
                    observation(BenchmarkConfig.optimized(), 0, 100, true, true, FINGERPRINT)));

            assertThat(report.isValid()).isFalse();
            assertThat(report.invalidReasons()).anyMatch(r -> r.contains("no measured observations"));
        }

        @Test
        @DisplayName("a missing configuration yields no headline rather than a partial one")
        void missingConfigYieldsNoHeadline() {
            BenchmarkReport report = new BenchmarkReport(
                    measured(BenchmarkConfig.optimized(), 100, 100, 100));

            assertThat(report.isValid()).as("nothing invalid about the data").isTrue();
            assertThat(report.headline()).as("but there is no baseline to compare against")
                    .isEmpty();
        }

        /** A correctness failure poisons the whole report, not just its own configuration. */
        @Test
        @DisplayName("one failed configuration invalidates the entire report")
        void oneFailurePoisonsTheReport() {
            List<BenchmarkObservation> all = new ArrayList<>();
            all.addAll(measured(BenchmarkConfig.naiveApp(), 200, 200, 200));
            all.addAll(measured(BenchmarkConfig.optimized(), 100, 100, 100));
            all.add(observation(BenchmarkConfig.sparkDefault(), 0, 150, false, false, FINGERPRINT));

            assertThat(new BenchmarkReport(all).headline())
                    .as("a broken third config still blocks the headline").isEmpty();
        }
    }

    @Nested
    @DisplayName("ablation")
    class Ablation {

        /**
         * Marginal contribution per step. A single headline percentage says the tuning worked; this
         * says which part of it did.
         */
        @Test
        @DisplayName("deltas report each step's contribution over the previous one")
        void deltasArePerStep() {
            List<BenchmarkConfig> ladder = BenchmarkConfig.ablationLadder();
            List<BenchmarkObservation> all = new ArrayList<>();
            long[] durations = {200, 160, 150, 120, 115, 100, 100};

            for (int i = 0; i < ladder.size(); i++) {
                all.addAll(measured(ladder.get(i), durations[i], durations[i], durations[i]));
            }

            var deltas = new BenchmarkReport(all).ablationDeltas(
                    ladder.stream().map(BenchmarkConfig::label).toList());

            assertThat(deltas).hasSize(ladder.size() - 1);
            assertThat(deltas.get("B1_shuffle_partitions"))
                    .as("200ms -> 160ms is 20%").isCloseTo(0.2, within(0.0001));
            assertThat(deltas.get("B6_full"))
                    .as("a step that changes nothing contributes nothing")
                    .isCloseTo(0.0, within(0.0001));
        }

        @Test
        @DisplayName("an absent step is skipped rather than breaking the chain")
        void absentStepSkipped() {
            List<BenchmarkObservation> all = new ArrayList<>();
            all.addAll(measured(BenchmarkConfig.naiveApp(), 200, 200, 200));
            all.addAll(measured(BenchmarkConfig.optimized(), 100, 100, 100));

            var deltas = new BenchmarkReport(all)
                    .ablationDeltas(List.of("naive_app", "never_ran", "optimized"));

            assertThat(deltas).containsOnlyKeys("optimized");
        }
    }

    @Nested
    @DisplayName("observation validation")
    class ObservationValidation {

        @Test
        @DisplayName("a non-positive duration is refused at construction")
        void nonPositiveDurationRefused() {
            Instant start = Instant.parse("2026-01-01T00:00:00Z");

            org.assertj.core.api.Assertions.assertThatThrownBy(() -> new BenchmarkObservation(
                    BenchmarkConfig.optimized(), 0, 0, false, start, start, 0,
                    FINGERPRINT, 1, 1, 1, "o", true, 0, 0))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("not a valid measurement");
        }

        /** Without a fingerprint the comparison cannot be validated at all. */
        @Test
        @DisplayName("a missing input fingerprint is refused")
        void missingFingerprintRefused() {
            Instant start = Instant.parse("2026-01-01T00:00:00Z");

            org.assertj.core.api.Assertions.assertThatThrownBy(() -> new BenchmarkObservation(
                    BenchmarkConfig.optimized(), 0, 0, false, start, start.plusMillis(10), 10,
                    "  ", 1, 1, 1, "o", true, 0, 0))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("inputFingerprint is required");
        }
    }
}
