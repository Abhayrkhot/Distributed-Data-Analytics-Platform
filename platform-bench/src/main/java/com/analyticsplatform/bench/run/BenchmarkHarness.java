package com.analyticsplatform.bench.run;

import com.analyticsplatform.bench.config.BenchmarkConfig;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs configurations and collects observations.
 *
 * <h2>Measurement discipline</h2>
 *
 * <ul>
 *   <li><strong>Warm-up first.</strong> The first execution of anything on the JVM pays for class
 *       loading, JIT and connector initialization. With five iterations that overhead lands almost
 *       entirely on whichever configuration happened to run first, which is an ordering artefact
 *       masquerading as a result.
 *   <li><strong>Alternating order.</strong> Running all of A then all of B lets filesystem cache,
 *       JIT state and thermal conditions drift monotonically across the run and be attributed to
 *       the configuration rather than to time. Interleaving spreads that drift evenly.
 *   <li><strong>Recorded sequence.</strong> The actual execution position is stored, so the order
 *       can be audited afterwards instead of taken on trust.
 *   <li><strong>Correctness before timing.</strong> Every measured run is compared against the
 *       baseline's output. A faster run that computed something else is discarded, not reported.
 * </ul>
 *
 * <p>Cache policy is <strong>warm</strong> and stated as such: the first configuration is not
 * penalised for a cold page cache. What matters is that every configuration receives equivalent
 * treatment, not that the cache is in any particular state.
 */
public final class BenchmarkHarness {

    private static final Logger log = LoggerFactory.getLogger(BenchmarkHarness.class);

    /** A workload to measure. Must be a pure function of its inputs. */
    @FunctionalInterface
    public interface Workload {
        Dataset<Row> execute(SparkSession spark, BenchmarkConfig config);
    }

    /** How many times to run each configuration. */
    public record Plan(int warmupRuns, int measuredIterations) {

        public Plan {
            if (warmupRuns < 0) {
                throw new IllegalArgumentException("warmupRuns must not be negative");
            }
            if (measuredIterations < 1) {
                throw new IllegalArgumentException("at least one measured iteration is required");
            }
        }

        /** The default: one warm-up, five measured. Five is where the median starts to mean much. */
        public static Plan standard() {
            return new Plan(1, 5);
        }

        public static Plan quick() {
            return new Plan(1, 2);
        }
    }

    private final SparkSession spark;
    private final CorrectnessGate.InputProfile inputProfile;

    public BenchmarkHarness(SparkSession spark, CorrectnessGate.InputProfile inputProfile) {
        this.spark = spark;
        this.inputProfile = inputProfile;
    }

    /**
     * Runs every configuration and returns the observations.
     *
     * @param baselineLabel the configuration whose output defines correctness for the rest
     */
    public List<BenchmarkObservation> run(
            List<BenchmarkConfig> configs, Workload workload, String baselineLabel, Plan plan) {

        List<BenchmarkObservation> observations = new ArrayList<>();
        int sequence = 0;

        // Warm-ups for every configuration before any measurement, so no configuration pays the
        // JIT cost on a run that counts.
        for (int w = 0; w < plan.warmupRuns(); w++) {
            for (BenchmarkConfig config : configs) {
                observations.add(execute(config, workload, w, sequence++, true, null));
            }
        }

        // The baseline's output, computed once, defines correctness for every other configuration.
        String expectedOutput = null;

        // Alternating: iteration by iteration rather than configuration by configuration.
        for (int iteration = 0; iteration < plan.measuredIterations(); iteration++) {
            List<BenchmarkConfig> order = new ArrayList<>(configs);
            // Reverse on odd iterations so no configuration is always first.
            if (iteration % 2 == 1) {
                java.util.Collections.reverse(order);
            }

            for (BenchmarkConfig config : order) {
                BenchmarkObservation observation =
                        execute(config, workload, iteration, sequence++, false, expectedOutput);
                if (expectedOutput == null && config.label().equals(baselineLabel)) {
                    expectedOutput = observation.outputFingerprint();
                }
                observations.add(observation);
            }
        }

        // A configuration measured before the baseline had no expected output to compare against.
        // Rather than leave it unverified, re-check it now that the baseline is known.
        return reconcileAgainstBaseline(observations, expectedOutput);
    }

    private BenchmarkObservation execute(
            BenchmarkConfig config, Workload workload, int iteration, int sequence,
            boolean warmup, String expectedOutput) {

        applySettings(config);

        Instant start = Instant.now();
        Dataset<Row> output = workload.execute(spark, config);
        String outputFingerprint = CorrectnessGate.contentHash(output);
        Instant finish = Instant.now();

        long millis = Math.max(1, Duration.between(start, finish).toMillis());
        boolean correct = expectedOutput == null || expectedOutput.equals(outputFingerprint);

        if (!correct) {
            log.error("correctness gate FAILED for {}: expected {} but produced {}",
                    config.label(), expectedOutput, outputFingerprint);
        }

        return new BenchmarkObservation(
                config, iteration, sequence, warmup, start, finish, millis,
                inputProfile.fingerprint(), inputProfile.rowCount(), inputProfile.bytes(),
                inputProfile.fileCount(), outputFingerprint, correct,
                lastFilesScanned(), lastBytesScanned());
    }

    /**
     * Marks observations recorded before the baseline was known.
     *
     * <p>Without this an alternating order would leave the first iteration's non-baseline runs
     * unverified — and an unverified run silently counted as correct is exactly the gap the gate
     * exists to close.
     */
    private List<BenchmarkObservation> reconcileAgainstBaseline(
            List<BenchmarkObservation> observations, String expectedOutput) {

        if (expectedOutput == null) {
            return observations;
        }
        List<BenchmarkObservation> reconciled = new ArrayList<>(observations.size());
        for (BenchmarkObservation observation : observations) {
            boolean correct = expectedOutput.equals(observation.outputFingerprint());
            reconciled.add(observation.correctnessPassed() == correct
                    ? observation
                    : new BenchmarkObservation(
                            observation.config(), observation.iterationIndex(),
                            observation.sequencePosition(), observation.warmup(),
                            observation.startedAt(), observation.finishedAt(),
                            observation.durationMillis(), observation.inputFingerprint(),
                            observation.inputRowCount(), observation.inputBytes(),
                            observation.inputFileCount(), observation.outputFingerprint(),
                            correct, observation.filesScanned(), observation.bytesScanned()));
        }
        return reconciled;
    }

    private void applySettings(BenchmarkConfig config) {
        for (Map.Entry<String, String> setting : config.sparkSettings().entrySet()) {
            spark.conf().set(setting.getKey(), setting.getValue());
        }
    }

    /**
     * Scan metrics for the last execution.
     *
     * <p>Zero when unavailable. Reporting zero rather than guessing matters: a partition-pruning
     * claim is only made when scan evidence actually supports it, and an invented number would let
     * it be claimed regardless.
     */
    private long lastFilesScanned() {
        return 0;
    }

    private long lastBytesScanned() {
        return 0;
    }
}
