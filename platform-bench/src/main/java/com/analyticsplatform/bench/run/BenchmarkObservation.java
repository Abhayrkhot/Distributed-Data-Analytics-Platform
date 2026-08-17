package com.analyticsplatform.bench.run;

import com.analyticsplatform.bench.config.BenchmarkConfig;
import java.time.Instant;

/**
 * One measured (or warm-up) execution.
 *
 * <p>Carries everything needed to decide whether the measurement is <em>admissible</em>, not just
 * how long it took. A duration without its input fingerprint and correctness verdict is a number
 * with no claim attached to it.
 */
public record BenchmarkObservation(
        BenchmarkConfig config,
        int iterationIndex,
        int sequencePosition,
        boolean warmup,
        Instant startedAt,
        Instant finishedAt,
        long durationMillis,
        String inputFingerprint,
        long inputRowCount,
        long inputBytes,
        int inputFileCount,
        String outputFingerprint,
        boolean correctnessPassed,
        long filesScanned,
        long bytesScanned) {

    public BenchmarkObservation {
        if (config == null) {
            throw new IllegalArgumentException("config is required");
        }
        if (iterationIndex < 0 || sequencePosition < 0) {
            throw new IllegalArgumentException("indices must not be negative");
        }
        // A zero or negative duration is a broken measurement. Rejecting it here means it can
        // never reach the statistics, where it would silently drag a mean downward.
        if (durationMillis <= 0) {
            throw new IllegalArgumentException(
                    "duration " + durationMillis + "ms is not a valid measurement");
        }
        if (inputFingerprint == null || inputFingerprint.isBlank()) {
            throw new IllegalArgumentException(
                    "inputFingerprint is required: without it the comparison cannot be validated");
        }
    }

    /**
     * Whether this observation may contribute to a reported statistic.
     *
     * <p>Warm-ups are excluded because they measure JIT and class loading rather than the workload.
     * Correctness failures are excluded because a configuration that produced different output was
     * not doing the same work — its speed is meaningless.
     */
    public boolean isMeasured() {
        return !warmup && correctnessPassed;
    }
}
