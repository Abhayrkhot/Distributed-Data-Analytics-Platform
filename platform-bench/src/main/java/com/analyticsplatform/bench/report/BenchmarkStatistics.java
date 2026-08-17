package com.analyticsplatform.bench.report;

import java.util.ArrayList;
import java.util.List;
import java.util.OptionalDouble;

/**
 * Descriptive statistics over measured durations.
 *
 * <p>Kept separate from anything that touches Spark or a database so it can be tested with fake
 * durations. A bug here does not crash anything — it manufactures a false number that goes on a
 * résumé, which is a worse failure mode than an exception.
 *
 * <h2>Median is reported alongside mean</h2>
 *
 * <p>Benchmarks here run in Docker on a laptop, where an occasional run is disturbed by something
 * unrelated. A single outlier moves the mean and leaves the median alone, so reporting both makes
 * the distortion visible instead of hiding it in one averaged number.
 */
public record BenchmarkStatistics(
        int observations,
        double meanMillis,
        double medianMillis,
        long minMillis,
        long maxMillis,
        double stddevMillis) {

    /**
     * Computes statistics over measured durations.
     *
     * @throws IllegalArgumentException on an empty sample, or any non-positive duration. A zero or
     *         negative duration is a broken measurement, not a very fast run, and silently
     *         averaging it in would understate every configuration containing it.
     */
    public static BenchmarkStatistics of(List<Long> durationsMillis) {
        if (durationsMillis == null || durationsMillis.isEmpty()) {
            throw new IllegalArgumentException("cannot compute statistics over an empty sample");
        }
        for (Long duration : durationsMillis) {
            if (duration == null) {
                throw new IllegalArgumentException("sample contains a null duration");
            }
            if (duration <= 0) {
                throw new IllegalArgumentException(
                        "non-positive duration " + duration + "ms is a broken measurement");
            }
        }

        List<Long> sorted = new ArrayList<>(durationsMillis);
        sorted.sort(Long::compareTo);

        double mean = sorted.stream().mapToLong(Long::longValue).average().orElseThrow();
        double median = medianOf(sorted);
        long min = sorted.get(0);
        long max = sorted.get(sorted.size() - 1);

        // Sample standard deviation (n-1). A single observation has no spread to speak of, so it
        // reports 0 rather than dividing by zero.
        double variance = 0.0;
        if (sorted.size() > 1) {
            double sumSquares = 0.0;
            for (long duration : sorted) {
                double delta = duration - mean;
                sumSquares += delta * delta;
            }
            variance = sumSquares / (sorted.size() - 1);
        }

        return new BenchmarkStatistics(sorted.size(), mean, median, min, max, Math.sqrt(variance));
    }

    /** Even counts average the middle pair; odd counts take the middle value. */
    private static double medianOf(List<Long> sorted) {
        int size = sorted.size();
        int mid = size / 2;
        return size % 2 == 1
                ? sorted.get(mid)
                : (sorted.get(mid - 1) + sorted.get(mid)) / 2.0;
    }

    /**
     * Improvement of this configuration over a baseline, as a fraction.
     *
     * <p>Computed from medians rather than means, for the outlier reason above. A negative result
     * means the "optimized" configuration was slower, and is returned as-is rather than clamped —
     * a regression is a finding, not something to hide.
     */
    public double improvementOver(BenchmarkStatistics baseline) {
        if (baseline == null) {
            throw new IllegalArgumentException("baseline is required");
        }
        if (baseline.medianMillis <= 0) {
            throw new IllegalArgumentException("baseline median must be positive");
        }
        return (baseline.medianMillis - medianMillis) / baseline.medianMillis;
    }

    /** Rows per second, given the input size. */
    public OptionalDouble rowsPerSecond(long inputRows) {
        if (inputRows <= 0 || medianMillis <= 0) {
            return OptionalDouble.empty();
        }
        return OptionalDouble.of(inputRows / (medianMillis / 1000.0));
    }

    /**
     * Whether the sample is large enough for the spread to mean anything.
     *
     * <p>Not a hard gate — a small sample is still worth reporting — but the report labels it, so a
     * reader is not invited to trust a percentage derived from two runs.
     */
    public boolean isStatisticallyMeaningful() {
        return observations >= 5;
    }

    /** Relative spread. A high value means the runs disagreed and the median is doing real work. */
    public double coefficientOfVariation() {
        return meanMillis <= 0 ? 0.0 : stddevMillis / meanMillis;
    }

    public String describe() {
        return String.format(
                "n=%d mean=%.0fms median=%.0fms min=%dms max=%dms sd=%.1fms cv=%.1f%%",
                observations, meanMillis, medianMillis, minMillis, maxMillis,
                stddevMillis, coefficientOfVariation() * 100);
    }
}
