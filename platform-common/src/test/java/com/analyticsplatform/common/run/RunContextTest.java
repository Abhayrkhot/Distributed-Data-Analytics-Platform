package com.analyticsplatform.common.run;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.analyticsplatform.common.run.ControlPlane.MetricSample;
import com.analyticsplatform.common.run.ControlPlane.MetricSample.AttemptScope;
import com.analyticsplatform.common.run.ControlPlane.RunOutcome;
import com.analyticsplatform.common.run.ControlPlane.RunSpec;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * The three failure-recording invariants, plus the cases where an honest implementation is most
 * likely to accidentally report success.
 */
class RunContextTest {

    /** Recording fake: fast, deterministic, and needs no database. */
    private static final class RecordingControlPlane implements ControlPlane {
        final List<RunOutcome> outcomes = new ArrayList<>();
        final List<MetricSample> metrics = new ArrayList<>();
        RuntimeException finishFailure;
        long nextRunId = 42;

        @Override
        public long startRun(RunSpec spec) {
            return nextRunId;
        }

        @Override
        public void finishRun(long runId, RunOutcome outcome) {
            outcomes.add(outcome);
            if (finishFailure != null) {
                throw finishFailure;
            }
        }

        @Override
        public void recordMetric(long runId, MetricSample sample) {
            metrics.add(sample);
        }

        RunOutcome only() {
            assertThat(outcomes).hasSize(1);
            return outcomes.get(0);
        }
    }

    private final RecordingControlPlane controlPlane = new RecordingControlPlane();

    @Nested
    @DisplayName("invariant 1: markSuccess reached")
    class SuccessPath {

        @Test
        void recordsSuccessWithCounters() throws Exception {
            String result = RunContext.execute(controlPlane, RunSpec.of("J"), run -> {
                run.markSuccess(100, 90, 10);
                return "done";
            });

            assertThat(result).isEqualTo("done");
            RunOutcome outcome = controlPlane.only();
            assertThat(outcome.status()).isEqualTo(RunOutcome.Status.SUCCESS);
            assertThat(outcome.rowsRead()).isEqualTo(100);
            assertThat(outcome.rowsWritten()).isEqualTo(90);
            assertThat(outcome.rowsRejected()).isEqualTo(10);
            assertThat(outcome.errorClass()).isNull();
        }
    }

    @Nested
    @DisplayName("invariant 2: exception thrown")
    class FailurePath {

        @Test
        @DisplayName("records FAILED with error metadata and rethrows the original exception")
        void recordsFailureAndRethrows() {
            IOException original = new IOException("disk gone");

            Throwable thrown = catchThrowable(() ->
                    RunContext.execute(controlPlane, RunSpec.of("J"), run -> {
                        throw original;
                    }));

            // The original exception, not a wrapper: callers upstream match on type.
            assertThat(thrown).isSameAs(original);

            RunOutcome outcome = controlPlane.only();
            assertThat(outcome.status()).isEqualTo(RunOutcome.Status.FAILED);
            assertThat(outcome.errorClass()).isEqualTo("java.io.IOException");
            assertThat(outcome.errorMessage()).isEqualTo("disk gone");
        }

        /** An exception after markSuccess must still fail the run. */
        @Test
        @DisplayName("a throw after markSuccess still records FAILED")
        void failureAfterSuccessClaimWins() {
            catchThrowable(() -> RunContext.execute(controlPlane, RunSpec.of("J"), run -> {
                run.markSuccess(10, 10, 0);
                throw new IllegalStateException("late failure");
            }));

            assertThat(controlPlane.only().status()).isEqualTo(RunOutcome.Status.FAILED);
        }

        @Test
        @DisplayName("markSuccess after markFailed is refused")
        void cannotTalkAFailedRunBackIntoSuccess() {
            catchThrowable(() -> RunContext.execute(controlPlane, RunSpec.of("J"), run -> {
                run.markFailed(new IOException("boom"));
                assertThatThrownBy(() -> run.markSuccess(1, 1, 0))
                        .isInstanceOf(IllegalStateException.class);
                return null;
            }));

            assertThat(controlPlane.only().status()).isEqualTo(RunOutcome.Status.FAILED);
        }

        @Test
        @DisplayName("a null message is tolerated")
        void nullMessageIsHandled() {
            catchThrowable(() -> RunContext.execute(controlPlane, RunSpec.of("J"), run -> {
                throw new IllegalStateException();
            }));

            assertThat(controlPlane.only().errorClass())
                    .isEqualTo("java.lang.IllegalStateException");
            assertThat(controlPlane.only().errorMessage()).isNull();
        }
    }

    @Nested
    @DisplayName("invariant 3: neither reached")
    class NeitherPath {

        /**
         * The quiet one. A body that returns without claiming success — an early return, a loop
         * that never executed — must not be recorded as successful just because nothing threw.
         */
        @Test
        @DisplayName("a body that returns without markSuccess records FAILED")
        void silentlyIncompleteRunIsAFailure() throws Exception {
            RunContext.execute(controlPlane, RunSpec.of("J"), run -> "returned early");

            RunOutcome outcome = controlPlane.only();
            assertThat(outcome.status()).isEqualTo(RunOutcome.Status.FAILED);
            assertThat(outcome.errorClass()).contains("IncompleteRun");
            assertThat(outcome.errorMessage()).contains("markSuccess");
        }
    }

    @Nested
    @DisplayName("metadata write failures")
    class MetadataFailures {

        /**
         * If recording the failure itself fails, the original cause must still reach the caller.
         * Losing it would leave an operator debugging a control-plane error while the real fault
         * went unreported.
         */
        @Test
        @DisplayName("a failing finishRun does not mask the original exception")
        void controlPlaneFailureDoesNotMaskTheCause() {
            controlPlane.finishFailure = new RuntimeException("control plane unreachable");
            IOException original = new IOException("the real problem");

            Throwable thrown = catchThrowable(() ->
                    RunContext.execute(controlPlane, RunSpec.of("J"), run -> {
                        throw original;
                    }));

            assertThat(thrown).isSameAs(original);
            assertThat(thrown.getSuppressed())
                    .anyMatch(s -> s.getMessage().contains("control plane unreachable"));
        }

        /** A failing finishRun on the success path must surface, not be swallowed. */
        @Test
        @DisplayName("a failing finishRun on the success path propagates")
        void controlPlaneFailureOnSuccessPathPropagates() {
            controlPlane.finishFailure = new RuntimeException("control plane unreachable");

            assertThatThrownBy(() -> RunContext.execute(controlPlane, RunSpec.of("J"), run -> {
                run.markSuccess(1, 1, 0);
                return null;
            })).isInstanceOf(RuntimeException.class).hasMessageContaining("unreachable");
        }
    }

    @Nested
    @DisplayName("lifecycle")
    class Lifecycle {

        /** Exactly one terminal write, even if close happens more than once. */
        @Test
        @DisplayName("closing twice records a single outcome")
        void closeIsIdempotent() throws Exception {
            RunContext.execute(controlPlane, RunSpec.of("J"), run -> {
                run.markSuccess(1, 1, 0);
                run.close();
                run.close();
                return null;
            });

            assertThat(controlPlane.outcomes).hasSize(1);
        }

        @Test
        @DisplayName("metrics are recorded against the run id")
        void metricsAreForwarded() throws Exception {
            RunContext.execute(controlPlane, RunSpec.of("J"), run -> {
                run.recordMetric(new MetricSample(
                        "shuffle_read_bytes", 1024, "bytes", AttemptScope.ALL_ATTEMPTS));
                run.markSuccess(1, 1, 0);
                return null;
            });

            assertThat(controlPlane.metrics).hasSize(1);
            assertThat(controlPlane.metrics.get(0).name()).isEqualTo("shuffle_read_bytes");
        }
    }

    @Nested
    @DisplayName("outcome validation mirrors the database constraints")
    class OutcomeValidation {

        @Test
        void successCannotCarryAnError() {
            assertThatThrownBy(() -> new RunOutcome(
                    RunOutcome.Status.SUCCESS, 0, 0, 0, "java.io.IOException", "x"))
                    .isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        void failureMustCarryAnErrorClass() {
            assertThatThrownBy(() -> new RunOutcome(
                    RunOutcome.Status.FAILED, 0, 0, 0, null, "x"))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }
}
