package com.analyticsplatform.common.unit;

import static com.analyticsplatform.common.unit.ProcessingUnitState.Operation;
import static com.analyticsplatform.common.unit.ProcessingUnitState.Status;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;

class ProcessingUnitStateTest {

    @Nested
    @DisplayName("permitted transitions")
    class Permitted {

        @ParameterizedTest(name = "{1} from {0} yields {2}")
        @CsvSource({
            "PENDING,  CLAIM,         RUNNING",
            "FAILED,   CLAIM,         RUNNING",
            "RUNNING,  COMMIT,        COMPLETE",
            "RUNNING,  FAIL,          FAILED",
            "RUNNING,  EXPIRE_LEASE,  FAILED",
            "COMPLETE, FORCE_REBUILD, PENDING",
            "FAILED,   FORCE_REBUILD, PENDING",
        })
        void transitionProducesExpectedStatus(Status from, Operation op, Status expected) {
            assertThat(ProcessingUnitState.isPermitted(from, op)).isTrue();
            assertThat(ProcessingUnitState.apply(from, op)).isEqualTo(expected);
        }

        /** The normal lifecycle, end to end. */
        @Test
        @DisplayName("claim then commit reaches COMPLETE")
        void happyPath() {
            Status s = ProcessingUnitState.apply(Status.PENDING, Operation.CLAIM);
            assertThat(s).isEqualTo(Status.RUNNING);

            s = ProcessingUnitState.apply(s, Operation.COMMIT);
            assertThat(s).isEqualTo(Status.COMPLETE);
            assertThat(ProcessingUnitState.isTerminal(s)).isTrue();
        }

        /** A crashed owner's unit is reclaimable: expire, then claim again. */
        @Test
        @DisplayName("expired lease returns the unit to the retry path")
        void expiredLeaseIsRetryable() {
            Status s = ProcessingUnitState.apply(Status.RUNNING, Operation.EXPIRE_LEASE);
            assertThat(s).isEqualTo(Status.FAILED);

            assertThat(ProcessingUnitState.apply(s, Operation.CLAIM)).isEqualTo(Status.RUNNING);
        }

        @Test
        @DisplayName("repeated failure cycles without reaching COMPLETE")
        void repeatedRetryCycle() {
            Status s = Status.PENDING;
            for (int attempt = 0; attempt < 3; attempt++) {
                s = ProcessingUnitState.apply(s, Operation.CLAIM);
                s = ProcessingUnitState.apply(s, Operation.FAIL);
                assertThat(s).isEqualTo(Status.FAILED);
            }
            s = ProcessingUnitState.apply(s, Operation.CLAIM);
            assertThat(ProcessingUnitState.apply(s, Operation.COMMIT)).isEqualTo(Status.COMPLETE);
        }
    }

    @Nested
    @DisplayName("refused transitions")
    class Refused {

        /**
         * The two transitions that cause real damage: reprocessing committed data, and marking a
         * unit done that never ran.
         */
        @ParameterizedTest(name = "{1} from {0} is refused")
        @CsvSource({
            "COMPLETE, CLAIM",          // would silently reprocess committed data
            "COMPLETE, COMMIT",         // double commit
            "COMPLETE, FAIL",           // committed output cannot retroactively fail
            "COMPLETE, EXPIRE_LEASE",   // COMPLETE holds no lease
            "FAILED,   COMMIT",         // done without ever running
            "FAILED,   FAIL",
            "FAILED,   EXPIRE_LEASE",   // FAILED holds no lease
            "PENDING,  COMMIT",         // done without ever running
            "PENDING,  FAIL",
            "PENDING,  EXPIRE_LEASE",   // PENDING holds no lease
            "PENDING,  FORCE_REBUILD",  // nothing committed to rebuild
            "RUNNING,  CLAIM",          // already owned by someone
            "RUNNING,  FORCE_REBUILD",  // must fail or expire first
        })
        void refusedTransitionsThrow(Status from, Operation op) {
            assertThat(ProcessingUnitState.isPermitted(from, op)).isFalse();

            assertThatThrownBy(() -> ProcessingUnitState.apply(from, op))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("illegal transition");
        }

        /**
         * A refused operation must throw rather than return the unchanged status: a silent no-op
         * would let a caller believe it had committed a unit that is still FAILED.
         */
        @Test
        @DisplayName("a refused commit throws instead of quietly doing nothing")
        void refusalIsNotASilentNoOp() {
            assertThatThrownBy(() -> ProcessingUnitState.apply(Status.FAILED, Operation.COMMIT))
                    .isInstanceOf(IllegalStateException.class);
        }

        @Test
        @DisplayName("reprocessing a committed unit requires an explicit force rebuild")
        void committedUnitsAreProtected() {
            assertThat(ProcessingUnitState.isPermitted(Status.COMPLETE, Operation.CLAIM)).isFalse();

            // The deliberate escape hatch, and only this one.
            Status rebuilt = ProcessingUnitState.apply(Status.COMPLETE, Operation.FORCE_REBUILD);
            assertThat(rebuilt).isEqualTo(Status.PENDING);
            assertThat(ProcessingUnitState.apply(rebuilt, Operation.CLAIM)).isEqualTo(Status.RUNNING);
        }
    }

    @Nested
    @DisplayName("lease and terminality")
    class LeaseAndTerminality {

        @ParameterizedTest
        @EnumSource(Status.class)
        void onlyRunningHoldsALease(Status status) {
            assertThat(ProcessingUnitState.holdsLease(status))
                    .isEqualTo(status == Status.RUNNING);
        }

        @ParameterizedTest
        @EnumSource(Status.class)
        void onlyCompleteIsTerminal(Status status) {
            assertThat(ProcessingUnitState.isTerminal(status))
                    .isEqualTo(status == Status.COMPLETE);
        }

        @Test
        @DisplayName("null arguments are rejected")
        void nullsAreRejected() {
            assertThatThrownBy(() -> ProcessingUnitState.apply(null, Operation.CLAIM))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> ProcessingUnitState.apply(Status.PENDING, null))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }
}
