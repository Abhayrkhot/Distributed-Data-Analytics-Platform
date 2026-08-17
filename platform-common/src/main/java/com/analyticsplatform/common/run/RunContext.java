package com.analyticsplatform.common.run;

import com.analyticsplatform.common.run.ControlPlane.MetricSample;
import com.analyticsplatform.common.run.ControlPlane.RunOutcome;
import com.analyticsplatform.common.run.ControlPlane.RunSpec;

/**
 * Tracks one job execution and guarantees it is recorded truthfully.
 *
 * <p>{@code AutoCloseable.close()} cannot see what threw inside the try block, so success is
 * never inferred from the absence of an exception — it must be claimed explicitly by calling
 * {@link #markSuccess}. Anything else is a failure. That inversion matters: a job that returns
 * early, or exits a loop without doing its work, would otherwise be recorded as successful.
 *
 * <pre>{@code
 * RunContext.execute(controlPlane, RunSpec.of("SilverTransformJob"), run -> {
 *     long written = transform();
 *     run.markSuccess(read, written, rejected);
 *     return written;
 * });
 * }</pre>
 *
 * <p>Three invariants, each unit-tested:
 * <ol>
 *   <li>{@code markSuccess} reached → {@code SUCCESS}
 *   <li>exception thrown → {@code FAILED} carrying the error class and message, and the
 *       <em>original</em> exception is rethrown, never swallowed or replaced
 *   <li>neither → {@code FAILED}
 * </ol>
 */
public final class RunContext implements AutoCloseable {

    /** What the body claimed. Null until {@link #markSuccess} or {@link #markFailed} is called. */
    private RunOutcome claimed;
    private boolean closed;

    private final ControlPlane controlPlane;
    private final long runId;

    private RunContext(ControlPlane controlPlane, long runId) {
        this.controlPlane = controlPlane;
        this.runId = runId;
    }

    /** The body of a run. */
    @FunctionalInterface
    public interface RunBody<T> {
        T run(RunContext run) throws Exception;
    }

    /**
     * Runs {@code body}, recording the outcome exactly once.
     *
     * <p>The centralized helper exists because {@code close()} alone has no access to the
     * exception; catching here is what lets the failure carry real error metadata.
     *
     * @throws Exception the original exception from {@code body}, unchanged
     */
    public static <T> T execute(ControlPlane controlPlane, RunSpec spec, RunBody<T> body)
            throws Exception {

        long runId = controlPlane.startRun(spec);
        RunContext run = new RunContext(controlPlane, runId);

        T result;
        try {
            result = body.run(run);
        } catch (Exception e) {
            run.markFailed(e);
            try {
                run.close();
            } catch (RuntimeException closeFailure) {
                // A control-plane write that fails while recording a failure must not mask the
                // real cause, and must never let the run look successful.
                e.addSuppressed(closeFailure);
            }
            throw e;
        }

        // Deliberately outside the try. If the terminal write fails here, that failure is its own
        // error and propagates as-is; routing it through the catch above would call markFailed on
        // an already-closed context and replace the real cause with "run is already closed".
        run.close();
        return result;
    }

    /** Declares the run successful. Must be reached, or the run is recorded as failed. */
    public void markSuccess(long rowsRead, long rowsWritten, long rowsRejected) {
        requireOpen();
        if (claimed != null && claimed.status() == RunOutcome.Status.FAILED) {
            throw new IllegalStateException("cannot mark a run successful after it has failed");
        }
        this.claimed = RunOutcome.success(rowsRead, rowsWritten, rowsRejected);
    }

    /** Records a failure and the error metadata that goes with it. */
    public void markFailed(Throwable cause) {
        requireOpen();
        String errorClass = cause == null ? "java.lang.Throwable" : cause.getClass().getName();
        String message = cause == null ? null : cause.getMessage();
        // A failure claim always wins: once failed, the run cannot be talked back into success.
        this.claimed = RunOutcome.failed(errorClass, truncate(message));
    }

    public void recordMetric(MetricSample sample) {
        requireOpen();
        controlPlane.recordMetric(runId, sample);
    }

    public long runId() {
        return runId;
    }

    /**
     * Closes the run.
     *
     * <p>Idempotent, so an explicit close inside a try-with-resources block does not produce a
     * second terminal write.
     */
    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        RunOutcome outcome = claimed != null ? claimed : RunOutcome.failed(
                "com.analyticsplatform.common.run.IncompleteRun",
                "run body finished without calling markSuccess");
        controlPlane.finishRun(runId, outcome);
    }

    private void requireOpen() {
        if (closed) {
            throw new IllegalStateException("run " + runId + " is already closed");
        }
    }

    /** Keeps a pathological stack-trace-in-message from bloating the control plane. */
    private static String truncate(String message) {
        if (message == null) {
            return null;
        }
        return message.length() <= 2000 ? message : message.substring(0, 2000) + "...[truncated]";
    }
}
