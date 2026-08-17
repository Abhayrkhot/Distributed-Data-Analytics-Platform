package com.analyticsplatform.common.metrics;

import com.analyticsplatform.common.metrics.TaskMetricsAccumulator.TaskMetricSnapshot;
import com.analyticsplatform.common.run.ControlPlane.MetricSample;
import java.util.List;
import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerTaskEnd;

/**
 * Captures real execution metrics off Spark's listener bus.
 *
 * <p>Intentionally a thin adapter: it converts Spark's {@code TaskMetrics} into a plain snapshot
 * and hands it to {@link TaskMetricsAccumulator}, which owns all the arithmetic. Spark's metric
 * types cannot be constructed from Java test code, so any logic living here would be untestable
 * without a running cluster — and untested aggregation is how a benchmark ends up reporting a
 * number nobody can defend.
 *
 * <p>Register before the work being measured:
 * <pre>{@code
 * SparkMetricsListener listener = new SparkMetricsListener();
 * spark.sparkContext().addSparkListener(listener);
 * ... run the job ...
 * listener.samples().forEach(run::recordMetric);
 * }</pre>
 */
public final class SparkMetricsListener extends SparkListener {

    private final TaskMetricsAccumulator accumulator = new TaskMetricsAccumulator();

    @Override
    public void onTaskEnd(SparkListenerTaskEnd taskEnd) {
        TaskMetrics metrics = taskEnd.taskMetrics();
        if (metrics == null) {
            // Spark omits metrics for some task ends (e.g. a task killed before it ran). The task
            // still happened, so it is counted; treating absent metrics as zero keeps task_count
            // honest without inventing byte counts.
            accumulator.record(new TaskMetricSnapshot(
                    isSuccessful(taskEnd), 0, 0, 0, 0, 0, 0, 0, 0, 0));
            return;
        }

        accumulator.record(new TaskMetricSnapshot(
                isSuccessful(taskEnd),
                metrics.shuffleReadMetrics().totalBytesRead(),
                metrics.shuffleWriteMetrics().bytesWritten(),
                metrics.memoryBytesSpilled(),
                metrics.diskBytesSpilled(),
                metrics.executorCpuTime(),
                metrics.executorRunTime(),
                metrics.inputMetrics().bytesRead(),
                metrics.inputMetrics().recordsRead(),
                metrics.peakExecutionMemory()));
    }

    /**
     * Whether this attempt produced usable output.
     *
     * <p>Read from {@code TaskInfo}, not from the end reason, because a task can be marked failed
     * while still reporting metrics for the work it did before dying.
     */
    private static boolean isSuccessful(SparkListenerTaskEnd taskEnd) {
        return taskEnd.taskInfo() != null && taskEnd.taskInfo().successful();
    }

    /** Every metric under both attempt scopes. */
    public List<MetricSample> samples() {
        return accumulator.allSamples();
    }

    /** Attempts that were thrown away — the retry cost this run paid. */
    public long failedAttemptCount() {
        return accumulator.failedAttemptCount();
    }

    /** Exposed so callers can query a single scope without re-deriving it. */
    public TaskMetricsAccumulator accumulator() {
        return accumulator;
    }
}
