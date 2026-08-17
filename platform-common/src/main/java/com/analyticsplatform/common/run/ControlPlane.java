package com.analyticsplatform.common.run;

import java.util.Map;

/**
 * The control-plane writes a job performs during its own lifecycle.
 *
 * <p>Deliberately narrow, and an interface rather than the JDBC class directly, so
 * {@link RunContext}'s failure semantics can be tested exhaustively without a database. The
 * invariants that matter — a failed run is never recorded as successful, the original exception
 * is never swallowed — are pure control flow, and pinning them behind a Docker dependency would
 * make them slow to test and therefore under-tested.
 */
public interface ControlPlane {

    /** Opens a run row in {@code RUNNING} and returns its id. */
    long startRun(RunSpec spec);

    /** Closes a run with its terminal status and counters. */
    void finishRun(long runId, RunOutcome outcome);

    /** Records one metric sample. */
    void recordMetric(long runId, MetricSample sample);

    /** What a run is. */
    record RunSpec(
            String jobName,
            String jobVersion,
            String layer,
            String sparkAppId,
            String configLabel,
            String gitCommit,
            Map<String, String> config) {

        public RunSpec {
            if (jobName == null || jobName.isBlank()) {
                throw new IllegalArgumentException("jobName is required");
            }
            config = config == null ? Map.of() : Map.copyOf(config);
        }

        public static RunSpec of(String jobName) {
            return new RunSpec(jobName, null, null, null, null, null, Map.of());
        }
    }

    /** How a run ended. */
    record RunOutcome(
            Status status,
            long rowsRead,
            long rowsWritten,
            long rowsRejected,
            String errorClass,
            String errorMessage) {

        public enum Status { SUCCESS, FAILED, ABORTED }

        public RunOutcome {
            if (status == null) {
                throw new IllegalArgumentException("status is required");
            }
            // Mirrors the etl_run CHECK constraints, so a violation surfaces here with a clear
            // message rather than as a constraint error from the driver.
            if (status == Status.SUCCESS && (errorClass != null || errorMessage != null)) {
                throw new IllegalArgumentException("a successful run must not carry an error");
            }
            if (status == Status.FAILED && errorClass == null) {
                throw new IllegalArgumentException("a failed run must record an error class");
            }
        }

        static RunOutcome success(long rowsRead, long rowsWritten, long rowsRejected) {
            return new RunOutcome(Status.SUCCESS, rowsRead, rowsWritten, rowsRejected, null, null);
        }

        static RunOutcome failed(String errorClass, String errorMessage) {
            return new RunOutcome(Status.FAILED, 0, 0, 0, errorClass, errorMessage);
        }
    }

    /**
     * One metric observation.
     *
     * @param attemptScope whether the value covers every task attempt or only successful ones.
     *        Spark retries tasks, so a single number conflates total execution effort with useful
     *        work; both are recorded and the reader chooses.
     */
    record MetricSample(String name, double value, String unit, AttemptScope attemptScope) {

        public enum AttemptScope {
            ALL_ATTEMPTS("all_attempts"),
            SUCCESSFUL_ONLY("successful_only");

            private final String dbValue;

            AttemptScope(String dbValue) {
                this.dbValue = dbValue;
            }

            public String dbValue() {
                return dbValue;
            }
        }

        public MetricSample {
            if (name == null || name.isBlank()) {
                throw new IllegalArgumentException("metric name is required");
            }
            if (attemptScope == null) {
                throw new IllegalArgumentException("attemptScope is required");
            }
        }
    }
}
