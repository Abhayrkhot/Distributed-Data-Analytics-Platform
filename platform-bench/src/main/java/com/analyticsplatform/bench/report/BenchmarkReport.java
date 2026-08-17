package com.analyticsplatform.bench.report;

import com.analyticsplatform.bench.config.BenchmarkConfig;
import com.analyticsplatform.bench.run.BenchmarkObservation;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

/**
 * Turns observations into a defensible report, or refuses to.
 *
 * <h2>Refusing is the point</h2>
 *
 * <p>The easiest way for this project to produce a false claim is for the reporter to average
 * whatever it was handed. So it will not emit a headline percentage when the measurement is
 * inadmissible:
 *
 * <ul>
 *   <li><strong>Input fingerprints differ</strong> — the configurations did not process the same
 *       data, so the comparison is meaningless however clean the numbers look.
 *   <li><strong>The correctness gate failed</strong> — a configuration that produced different
 *       output was not doing the same work. Being faster at it is not an achievement.
 *   <li><strong>No measured observations</strong> — only warm-ups, which measure JIT.
 * </ul>
 *
 * <p>In each case {@link #headline()} returns empty and {@link #invalidReasons()} says why. The
 * report is still generated, because a refusal with its reason is more useful than no report.
 */
public final class BenchmarkReport {

    /** Per-configuration summary. */
    public record ConfigSummary(
            BenchmarkConfig config,
            BenchmarkStatistics statistics,
            long inputRowCount,
            long filesScanned,
            long bytesScanned) {
    }

    /** The comparison a reader actually wants, once it has been earned. */
    public record Headline(
            String baselineLabel,
            String optimizedLabel,
            double improvementFraction,
            BenchmarkStatistics baseline,
            BenchmarkStatistics optimized) {

        /**
         * The claim, phrased so it names its baseline.
         *
         * <p>"Improved Spark performance by X%" invites the reader to assume the largest available
         * gap. Naming the baseline is the difference between a measurement and a boast.
         */
        public String claim() {
            return String.format(
                    "Reduced runtime by %.1f%% versus the %s configuration (median %.0fms -> %.0fms)",
                    improvementFraction * 100, baselineLabel,
                    baseline.medianMillis(), optimized.medianMillis());
        }
    }

    private final List<BenchmarkObservation> observations;
    private final Map<String, ConfigSummary> summaries = new LinkedHashMap<>();
    private final List<String> invalidReasons = new ArrayList<>();

    public BenchmarkReport(List<BenchmarkObservation> observations) {
        this.observations = List.copyOf(observations);
        validate();
        summarize();
    }

    private void validate() {
        List<BenchmarkObservation> measured = measured();

        if (measured.isEmpty()) {
            invalidReasons.add("no measured observations (warm-ups do not count)");
            return;
        }

        // Every configuration must have consumed identical input. This is the check that catches
        // the most seductive error: a "faster" configuration that was quietly given less data.
        Set<String> fingerprints = new TreeSet<>();
        for (BenchmarkObservation observation : measured) {
            fingerprints.add(observation.inputFingerprint());
        }
        if (fingerprints.size() > 1) {
            invalidReasons.add("configurations consumed different input ("
                    + fingerprints.size() + " distinct fingerprints: " + fingerprints
                    + "); the comparison is invalid");
        }

        List<String> failed = observations.stream()
                .filter(o -> !o.warmup() && !o.correctnessPassed())
                .map(o -> o.config().label())
                .distinct()
                .toList();
        if (!failed.isEmpty()) {
            invalidReasons.add("correctness gate failed for " + failed);
        }
    }

    private void summarize() {
        Map<String, List<BenchmarkObservation>> byConfig = new LinkedHashMap<>();
        for (BenchmarkObservation observation : measured()) {
            byConfig.computeIfAbsent(observation.config().label(), k -> new ArrayList<>())
                    .add(observation);
        }

        byConfig.forEach((label, group) -> {
            List<Long> durations = group.stream()
                    .map(BenchmarkObservation::durationMillis).toList();
            summaries.put(label, new ConfigSummary(
                    group.get(0).config(),
                    BenchmarkStatistics.of(durations),
                    group.get(0).inputRowCount(),
                    group.stream().mapToLong(BenchmarkObservation::filesScanned).max().orElse(0),
                    group.stream().mapToLong(BenchmarkObservation::bytesScanned).max().orElse(0)));
        });
    }

    private List<BenchmarkObservation> measured() {
        return observations.stream().filter(BenchmarkObservation::isMeasured).toList();
    }

    public boolean isValid() {
        return invalidReasons.isEmpty();
    }

    public List<String> invalidReasons() {
        return List.copyOf(invalidReasons);
    }

    public Map<String, ConfigSummary> summaries() {
        return Map.copyOf(summaries);
    }

    public Optional<ConfigSummary> summaryFor(String label) {
        return Optional.ofNullable(summaries.get(label));
    }

    /**
     * The headline comparison, if it has been earned.
     *
     * @return empty when the measurement is inadmissible, or when either configuration is absent
     */
    public Optional<Headline> headline() {
        return headline("naive_app", "optimized");
    }

    public Optional<Headline> headline(String baselineLabel, String optimizedLabel) {
        if (!isValid()) {
            return Optional.empty();
        }
        ConfigSummary baseline = summaries.get(baselineLabel);
        ConfigSummary optimized = summaries.get(optimizedLabel);
        if (baseline == null || optimized == null) {
            return Optional.empty();
        }
        return Optional.of(new Headline(
                baselineLabel, optimizedLabel,
                optimized.statistics().improvementOver(baseline.statistics()),
                baseline.statistics(), optimized.statistics()));
    }

    /**
     * Marginal contribution of each ablation step relative to the one before it.
     *
     * <p>Answers "which change mattered", which a single headline percentage cannot.
     */
    public Map<String, Double> ablationDeltas(List<String> orderedLabels) {
        Map<String, Double> deltas = new LinkedHashMap<>();
        BenchmarkStatistics previous = null;
        for (String label : orderedLabels) {
            ConfigSummary summary = summaries.get(label);
            if (summary == null) {
                continue;
            }
            if (previous != null) {
                deltas.put(label, summary.statistics().improvementOver(previous));
            }
            previous = summary.statistics();
        }
        return deltas;
    }
}
