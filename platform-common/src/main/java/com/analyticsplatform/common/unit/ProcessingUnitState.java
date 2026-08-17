package com.analyticsplatform.common.unit;

import java.util.EnumSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * The processing-unit state machine.
 *
 * <p>Modelled explicitly rather than left implicit in ingest code, because the illegal
 * transitions are the dangerous ones and they are easy to reach by accident:
 * {@code COMPLETE -> RUNNING} silently reprocesses committed data, and
 * {@code FAILED -> COMPLETE} marks a unit done that never actually ran.
 *
 * <p>Transitions are expressed as <em>operations</em>, not as arbitrary status pairs. Asking
 * "may this unit go from FAILED to COMPLETE?" invites a caller to answer it themselves; asking
 * "may I commit this unit?" does not. The property tests then have something meaningful to
 * generate: no sequence of permitted operations may reach an impossible state.
 *
 * <pre>
 *   PENDING --claim--> RUNNING --commit--> COMPLETE
 *                        |                    |
 *                        |--fail------> FAILED |
 *                        |--expire----> FAILED |
 *                                         |    |
 *                                  claim--+    |
 *                                              |
 *                        force_rebuild---------+--> PENDING
 * </pre>
 *
 * <p>{@code COMPLETE} is terminal under normal operation. Reprocessing a committed unit requires
 * {@link Operation#FORCE_REBUILD}, which exists so that discarding committed data is always a
 * deliberate act rather than the result of a retry loop.
 */
public final class ProcessingUnitState {

    /** Mirrors the {@code status} CHECK constraint on {@code control.processing_unit}. */
    public enum Status {
        PENDING,
        RUNNING,
        COMPLETE,
        FAILED
    }

    /** The operations ingest code is allowed to perform. */
    public enum Operation {
        /** Take ownership and begin work. Requires acquiring a lease. */
        CLAIM,
        /** Record success. Only legal once the manifest has been written. */
        COMMIT,
        /** Record failure. */
        FAIL,
        /** Reclaim a unit whose owner died without releasing its lease. */
        EXPIRE_LEASE,
        /** Deliberately discard committed output so the unit can be rebuilt. */
        FORCE_REBUILD
    }

    /**
     * Permitted (operation, from) pairs and the status each produces.
     *
     * <p>Anything absent is refused. Fail-closed: a new status or operation added later is
     * rejected until someone states explicitly what it should do.
     */
    private static final Map<Operation, Map<Status, Status>> TRANSITIONS = Map.of(
            Operation.CLAIM, Map.of(
                    Status.PENDING, Status.RUNNING,
                    // A previously failed unit is retryable; attempt_count increments.
                    Status.FAILED, Status.RUNNING),
            Operation.COMMIT, Map.of(
                    Status.RUNNING, Status.COMPLETE),
            Operation.FAIL, Map.of(
                    Status.RUNNING, Status.FAILED),
            Operation.EXPIRE_LEASE, Map.of(
                    Status.RUNNING, Status.FAILED),
            Operation.FORCE_REBUILD, Map.of(
                    Status.COMPLETE, Status.PENDING,
                    Status.FAILED, Status.PENDING));

    /** Statuses that hold a lease. The DB enforces the same rule as a CHECK constraint. */
    private static final Set<Status> LEASE_HOLDING = EnumSet.of(Status.RUNNING);

    private ProcessingUnitState() {
    }

    /** True when {@code operation} may be applied to a unit currently in {@code from}. */
    public static boolean isPermitted(Status from, Operation operation) {
        return resolve(from, operation).isPresent();
    }

    /**
     * Applies an operation.
     *
     * @return the resulting status
     * @throws IllegalStateException if the operation is not permitted from {@code from}. Throwing
     *         rather than returning the unchanged status matters: a silent no-op would let a
     *         caller believe it had committed a unit that stayed FAILED.
     */
    public static Status apply(Status from, Operation operation) {
        return resolve(from, operation).orElseThrow(() -> new IllegalStateException(
                "illegal transition: cannot " + operation + " a unit in state " + from));
    }

    /** Whether a unit in this status is expected to hold a lease. */
    public static boolean holdsLease(Status status) {
        return LEASE_HOLDING.contains(status);
    }

    /**
     * Whether a unit in this status is safe to skip on a rerun.
     *
     * <p>Only {@code COMPLETE} qualifies, and even then the manifest is the authority — this
     * answers "does the bookkeeping say done", not "was it actually committed".
     */
    public static boolean isTerminal(Status status) {
        return status == Status.COMPLETE;
    }

    private static Optional<Status> resolve(Status from, Operation operation) {
        if (from == null || operation == null) {
            throw new IllegalArgumentException("status and operation must not be null");
        }
        return Optional.ofNullable(TRANSITIONS.get(operation)).map(m -> m.get(from));
    }
}
