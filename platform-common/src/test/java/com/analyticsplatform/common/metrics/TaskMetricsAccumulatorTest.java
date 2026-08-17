package com.analyticsplatform.common.metrics;

import static com.analyticsplatform.common.metrics.TaskMetricsAccumulator.TaskMetricSnapshot;
import static org.assertj.core.api.Assertions.assertThat;

import com.analyticsplatform.common.run.ControlPlane.MetricSample;
import com.analyticsplatform.common.run.ControlPlane.MetricSample.AttemptScope;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * Aggregation semantics, especially the retry-scoping that a naive listener gets wrong.
 */
class TaskMetricsAccumulatorTest {

    private final TaskMetricsAccumulator accumulator = new TaskMetricsAccumulator();

    private static double valueOf(List<MetricSample> samples, String name) {
        return samples.stream()
                .filter(s -> s.name().equals(name))
                .findFirst()
                .orElseThrow(() -> new AssertionError("no metric named " + name))
                .value();
    }

    private List<MetricSample> all() {
        return accumulator.samples(AttemptScope.ALL_ATTEMPTS);
    }

    private List<MetricSample> successful() {
        return accumulator.samples(AttemptScope.SUCCESSFUL_ONLY);
    }

    @Nested
    @DisplayName("retry scoping")
    class RetryScoping {

        /**
         * The case a single-number listener gets wrong: a failed attempt and its successful retry
         * both report shuffle bytes, but only one produced output.
         */
        @Test
        @DisplayName("a failed attempt counts toward effort but not toward useful work")
        void failedAttemptsAreScopedOut() {
            accumulator.record(TaskMetricSnapshot.failed(100, 50, 512));
            accumulator.record(TaskMetricSnapshot.successful(100, 50, 512));

            // Total cluster effort: both attempts ran and both consumed resources.
            assertThat(valueOf(all(), "shuffle_read_bytes")).isEqualTo(200);
            assertThat(valueOf(all(), "task_count")).isEqualTo(2);

            // Useful work: only the retry produced output.
            assertThat(valueOf(successful(), "shuffle_read_bytes")).isEqualTo(100);
            assertThat(valueOf(successful(), "task_count")).isEqualTo(1);
        }

        @Test
        @DisplayName("failedAttemptCount reports the retry cost")
        void failedAttemptCountIsTheDifference() {
            accumulator.record(TaskMetricSnapshot.failed(10, 10, 1));
            accumulator.record(TaskMetricSnapshot.failed(10, 10, 1));
            accumulator.record(TaskMetricSnapshot.successful(10, 10, 1));

            assertThat(accumulator.failedAttemptCount()).isEqualTo(2);
            assertThat(accumulator.taskCount(AttemptScope.ALL_ATTEMPTS)).isEqualTo(3);
            assertThat(accumulator.taskCount(AttemptScope.SUCCESSFUL_ONLY)).isEqualTo(1);
        }

        @Test
        @DisplayName("with no retries the two scopes agree")
        void scopesAgreeWithoutRetries() {
            accumulator.record(TaskMetricSnapshot.successful(100, 50, 512));
            accumulator.record(TaskMetricSnapshot.successful(200, 60, 256));

            assertThat(valueOf(all(), "shuffle_read_bytes"))
                    .isEqualTo(valueOf(successful(), "shuffle_read_bytes"));
            assertThat(accumulator.failedAttemptCount()).isZero();
        }

        @Test
        @DisplayName("an all-failed run reports effort but zero useful work")
        void allFailedRun() {
            accumulator.record(TaskMetricSnapshot.failed(100, 50, 512));
            accumulator.record(TaskMetricSnapshot.failed(100, 50, 512));

            assertThat(valueOf(all(), "shuffle_read_bytes")).isEqualTo(200);
            assertThat(valueOf(successful(), "shuffle_read_bytes")).isZero();
            assertThat(valueOf(successful(), "task_count")).isZero();
        }
    }

    @Nested
    @DisplayName("aggregation")
    class Aggregation {

        @Test
        @DisplayName("byte and time metrics sum across tasks")
        void additiveMetricsSum() {
            accumulator.record(new TaskMetricSnapshot(true, 10, 20, 30, 40, 50, 60, 70, 80, 90));
            accumulator.record(new TaskMetricSnapshot(true, 1, 2, 3, 4, 5, 6, 7, 8, 9));

            List<MetricSample> samples = all();
            assertThat(valueOf(samples, "shuffle_read_bytes")).isEqualTo(11);
            assertThat(valueOf(samples, "shuffle_write_bytes")).isEqualTo(22);
            assertThat(valueOf(samples, "memory_spill_bytes")).isEqualTo(33);
            assertThat(valueOf(samples, "disk_spill_bytes")).isEqualTo(44);
            assertThat(valueOf(samples, "executor_cpu_time_ns")).isEqualTo(55);
            assertThat(valueOf(samples, "executor_run_time_ms")).isEqualTo(66);
            assertThat(valueOf(samples, "input_bytes_read")).isEqualTo(77);
            assertThat(valueOf(samples, "input_records_read")).isEqualTo(88);
        }

        /**
         * Summing per-task peaks would report memory no executor ever held, and would grow with
         * parallelism rather than describing pressure.
         */
        @Test
        @DisplayName("peak execution memory is a maximum, not a sum")
        void peakMemoryTakesTheMaximum() {
            accumulator.record(TaskMetricSnapshot.successful(0, 0, 500));
            accumulator.record(TaskMetricSnapshot.successful(0, 0, 900));
            accumulator.record(TaskMetricSnapshot.successful(0, 0, 300));

            assertThat(valueOf(all(), "peak_execution_memory")).isEqualTo(900);
        }

        @Test
        @DisplayName("task_count counts task ends, not bytes")
        void taskCountCountsTasks() {
            for (int i = 0; i < 7; i++) {
                accumulator.record(TaskMetricSnapshot.successful(0, 0, 0));
            }
            assertThat(valueOf(all(), "task_count")).isEqualTo(7);
        }

        @Test
        @DisplayName("an accumulator with no tasks reports zeros, not nulls")
        void emptyAccumulatorReportsZeros() {
            assertThat(all()).isNotEmpty().allMatch(s -> s.value() == 0.0);
            assertThat(accumulator.failedAttemptCount()).isZero();
        }
    }

    @Nested
    @DisplayName("samples")
    class Samples {

        @Test
        @DisplayName("every sample carries its scope, so the two never merge on write")
        void samplesCarryTheirScope() {
            accumulator.record(TaskMetricSnapshot.successful(1, 1, 1));

            assertThat(accumulator.samples(AttemptScope.ALL_ATTEMPTS))
                    .allMatch(s -> s.attemptScope() == AttemptScope.ALL_ATTEMPTS);
            assertThat(accumulator.samples(AttemptScope.SUCCESSFUL_ONLY))
                    .allMatch(s -> s.attemptScope() == AttemptScope.SUCCESSFUL_ONLY);
        }

        /**
         * The control plane's unique key is (run_id, metric_name, attempt_scope), so emitting the
         * same name under both scopes must produce two distinct rows rather than a conflict.
         */
        @Test
        @DisplayName("allSamples emits each name once per scope")
        void allSamplesCoversBothScopes() {
            accumulator.record(TaskMetricSnapshot.successful(1, 1, 1));

            List<MetricSample> samples = accumulator.allSamples();

            assertThat(samples).hasSize(20);
            assertThat(samples.stream().filter(s -> s.name().equals("shuffle_read_bytes")))
                    .hasSize(2);
            assertThat(samples.stream().map(s -> s.name() + "/" + s.attemptScope()).distinct())
                    .hasSize(20);
        }

        @Test
        @DisplayName("units are declared on every metric")
        void unitsArePresent() {
            assertThat(accumulator.allSamples()).allMatch(s -> s.unit() != null && !s.unit().isBlank());
        }
    }
}
