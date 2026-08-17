package com.analyticsplatform.common.unit;

import static com.analyticsplatform.common.unit.ProcessingUnitState.Operation;
import static com.analyticsplatform.common.unit.ProcessingUnitState.Status;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Label;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;

/**
 * Generated transition sequences for the processing-unit state machine (§21).
 *
 * <p>The invariant under test is not "each individual transition is correct" — the enumerated
 * tests cover that — but that <em>no sequence</em> of permitted operations can walk the unit into
 * an impossible state. Bugs in state machines tend to live in reachable-but-unconsidered paths
 * rather than in single edges.
 */
@Label("ProcessingUnit state machine")
class ProcessingUnitStateProperties {

    @Provide
    Arbitrary<List<Operation>> operationSequences() {
        return Arbitraries.of(Operation.class).list().ofMinSize(1).ofMaxSize(30);
    }

    /** Applying only permitted operations always leaves the unit in a real status. */
    @Property
    void permittedOperationsAlwaysYieldAValidStatus(
            @ForAll("operationSequences") List<Operation> operations) {

        Status status = Status.PENDING;
        for (Operation op : operations) {
            if (ProcessingUnitState.isPermitted(status, op)) {
                status = ProcessingUnitState.apply(status, op);
            }
            assertThat(status).isIn((Object[]) Status.values());
        }
    }

    /**
     * The core safety property: COMPLETE is only ever reached by committing a RUNNING unit.
     * If any other path reaches it, committed data can be claimed without work having happened.
     */
    @Property
    void completeIsOnlyEverReachedByCommittingARunningUnit(
            @ForAll("operationSequences") List<Operation> operations) {

        Status status = Status.PENDING;
        for (Operation op : operations) {
            if (!ProcessingUnitState.isPermitted(status, op)) {
                continue;
            }
            Status before = status;
            status = ProcessingUnitState.apply(status, op);

            if (status == Status.COMPLETE) {
                assertThat(op).isEqualTo(Operation.COMMIT);
                assertThat(before).isEqualTo(Status.RUNNING);
            }
        }
    }

    /**
     * Committed output can only be left behind deliberately. Without this, a retry loop could
     * quietly reprocess a unit that was already published.
     */
    @Property
    void leavingCompleteAlwaysRequiresAnExplicitForceRebuild(
            @ForAll("operationSequences") List<Operation> operations) {

        Status status = Status.PENDING;
        for (Operation op : operations) {
            if (!ProcessingUnitState.isPermitted(status, op)) {
                continue;
            }
            Status before = status;
            status = ProcessingUnitState.apply(status, op);

            if (before == Status.COMPLETE && status != Status.COMPLETE) {
                assertThat(op).isEqualTo(Operation.FORCE_REBUILD);
            }
        }
    }

    /** The lease invariant the database enforces as a CHECK constraint, held in code too. */
    @Property
    void onlyRunningUnitsEverHoldALease(
            @ForAll("operationSequences") List<Operation> operations) {

        Status status = Status.PENDING;
        for (Operation op : operations) {
            if (ProcessingUnitState.isPermitted(status, op)) {
                status = ProcessingUnitState.apply(status, op);
            }
            assertThat(ProcessingUnitState.holdsLease(status))
                    .isEqualTo(status == Status.RUNNING);
        }
    }

    /** isPermitted and apply must never disagree — one guarding the other is the usual pattern. */
    @Property
    void isPermittedAgreesWithApply(@ForAll Status from, @ForAll Operation operation) {
        if (ProcessingUnitState.isPermitted(from, operation)) {
            assertThat(ProcessingUnitState.apply(from, operation)).isNotNull();
        } else {
            assertThatThrownBy(() -> ProcessingUnitState.apply(from, operation))
                    .isInstanceOf(IllegalStateException.class);
        }
    }

    /**
     * Every status must remain reachable from PENDING. A transition table that accidentally
     * stranded FAILED or COMPLETE would pass every "illegal transitions are refused" test while
     * making the pipeline unable to record those outcomes at all.
     */
    @Property(tries = 1)
    void everyStatusIsReachableFromPending() {
        List<Status> reached = new ArrayList<>();
        reached.add(Status.PENDING);

        Status running = ProcessingUnitState.apply(Status.PENDING, Operation.CLAIM);
        reached.add(running);
        reached.add(ProcessingUnitState.apply(running, Operation.COMMIT));
        reached.add(ProcessingUnitState.apply(running, Operation.FAIL));

        assertThat(reached).containsExactlyInAnyOrder(
                Status.PENDING, Status.RUNNING, Status.COMPLETE, Status.FAILED);
    }
}
