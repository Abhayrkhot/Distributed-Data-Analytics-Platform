package com.analyticsplatform.common.metrics;

import com.analyticsplatform.common.run.ControlPlane.MetricSample;
import com.analyticsplatform.common.run.ControlPlane.MetricSample.AttemptScope;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

/**
 * Aggregates per-task metrics under two attempt scopes.
 *
 * <p>Split out from {@link SparkMetricsListener} deliberately. Spark's {@code TaskMetrics} cannot
 * be constructed from Java test code, so leaving the arithmetic inside the listener would make the
 * aggregation semantics — which are the part that can silently be wrong — effectively untestable.
 * Here the same logic runs against plain records.
 *
 * <h2>Why two scopes</h2>
 *
 * <p>Spark retries failed tasks. Summing every {@code onTaskEnd} conflates a failed attempt with
 * the successful retry that replaced it, so shuffle bytes get counted twice for work that only
 * produced output once. Neither number is wrong, they answer different questions:
 *
 * <ul>
 *   <li>{@code ALL_ATTEMPTS} — total execution effort, including work thrown away. This is what
 *       actually consumed cluster time, so it is the honest denominator for resource cost.
 *   <li>{@code SUCCESSFUL_ONLY} — the work that produced the result. Comparable across runs even
 *       when one of them happened to hit a retry.
 * </ul>
 *
 * <p>A benchmark that reported only {@code ALL_ATTEMPTS} would show a config as slower purely
 * because it hit a flaky retry; one that reported only {@code SUCCESSFUL_ONLY} would hide a config
 * that triggers retries constantly. Both are recorded and the reader chooses.
 *
 * <h2>Aggregation, per metric</h2>
 *
 * <table>
 *   <caption>Metric aggregation semantics</caption>
 *   <tr><th>Metric</th><th>Unit</th><th>Aggregation</th></tr>
 *   <tr><td>shuffle_read_bytes, shuffle_write_bytes</td><td>bytes</td><td>sum</td></tr>
 *   <tr><td>memory_spill_bytes, disk_spill_bytes</td><td>bytes</td><td>sum</td></tr>
 *   <tr><td>executor_cpu_time_ns, executor_run_time_ms</td><td>ns / ms</td><td>sum</td></tr>
 *   <tr><td>input_bytes_read, input_records_read</td><td>bytes / count</td><td>sum</td></tr>
 *   <tr><td>task_count</td><td>count</td><td>count of task ends</td></tr>
 *   <tr><td>peak_execution_memory</td><td>bytes</td><td><b>max</b> observed per task</td></tr>
 * </table>
 *
 * <p>{@code peak_execution_memory} is a maximum, not a sum: summing per-task peaks would report a
 * number no single executor ever held, growing with parallelism rather than describing memory
 * pressure.
 */
public final class TaskMetricsAccumulator {

    /** One task's metrics, independent of Spark's types. */
    public record TaskMetricSnapshot(
            boolean successful,
            long shuffleReadBytes,
            long shuffleWriteBytes,
            long memorySpillBytes,
            long diskSpillBytes,
            long executorCpuTimeNanos,
            long executorRunTimeMillis,
            long inputBytesRead,
            long inputRecordsRead,
            long peakExecutionMemory) {

        /** A successful task with only the fields a test cares about. */
        public static TaskMetricSnapshot successful(long shuffleRead, long shuffleWrite, long peak) {
            return new TaskMetricSnapshot(
                    true, shuffleRead, shuffleWrite, 0, 0, 0, 0, 0, 0, peak);
        }

        /** A failed attempt: work that consumed resources but produced no output. */
        public static TaskMetricSnapshot failed(long shuffleRead, long shuffleWrite, long peak) {
            return new TaskMetricSnapshot(
                    false, shuffleRead, shuffleWrite, 0, 0, 0, 0, 0, 0, peak);
        }
    }

    /** Running totals for one scope. */
    private static final class Totals {
        long shuffleReadBytes;
        long shuffleWriteBytes;
        long memorySpillBytes;
        long diskSpillBytes;
        long executorCpuTimeNanos;
        long executorRunTimeMillis;
        long inputBytesRead;
        long inputRecordsRead;
        long peakExecutionMemory;
        long taskCount;

        void add(TaskMetricSnapshot task) {
            shuffleReadBytes += task.shuffleReadBytes();
            shuffleWriteBytes += task.shuffleWriteBytes();
            memorySpillBytes += task.memorySpillBytes();
            diskSpillBytes += task.diskSpillBytes();
            executorCpuTimeNanos += task.executorCpuTimeNanos();
            executorRunTimeMillis += task.executorRunTimeMillis();
            inputBytesRead += task.inputBytesRead();
            inputRecordsRead += task.inputRecordsRead();
            peakExecutionMemory = Math.max(peakExecutionMemory, task.peakExecutionMemory());
            taskCount++;
        }
    }

    private final Map<AttemptScope, Totals> totals = new EnumMap<>(AttemptScope.class);

    public TaskMetricsAccumulator() {
        totals.put(AttemptScope.ALL_ATTEMPTS, new Totals());
        totals.put(AttemptScope.SUCCESSFUL_ONLY, new Totals());
    }

    /**
     * Records one task end.
     *
     * <p>Synchronized because Spark's listener bus delivers on its own thread while a job may read
     * a snapshot from the driver thread.
     */
    public synchronized void record(TaskMetricSnapshot task) {
        totals.get(AttemptScope.ALL_ATTEMPTS).add(task);
        if (task.successful()) {
            totals.get(AttemptScope.SUCCESSFUL_ONLY).add(task);
        }
    }

    /** Number of task ends seen under a scope. */
    public synchronized long taskCount(AttemptScope scope) {
        return totals.get(scope).taskCount;
    }

    /** How many attempts were thrown away: the retry cost this run paid. */
    public synchronized long failedAttemptCount() {
        return totals.get(AttemptScope.ALL_ATTEMPTS).taskCount
                - totals.get(AttemptScope.SUCCESSFUL_ONLY).taskCount;
    }

    /** All metrics under one scope, ready to persist. */
    public synchronized List<MetricSample> samples(AttemptScope scope) {
        Totals t = totals.get(scope);
        List<MetricSample> out = new ArrayList<>(10);
        out.add(new MetricSample("shuffle_read_bytes", t.shuffleReadBytes, "bytes", scope));
        out.add(new MetricSample("shuffle_write_bytes", t.shuffleWriteBytes, "bytes", scope));
        out.add(new MetricSample("memory_spill_bytes", t.memorySpillBytes, "bytes", scope));
        out.add(new MetricSample("disk_spill_bytes", t.diskSpillBytes, "bytes", scope));
        out.add(new MetricSample("executor_cpu_time_ns", t.executorCpuTimeNanos, "ns", scope));
        out.add(new MetricSample("executor_run_time_ms", t.executorRunTimeMillis, "ms", scope));
        out.add(new MetricSample("input_bytes_read", t.inputBytesRead, "bytes", scope));
        out.add(new MetricSample("input_records_read", t.inputRecordsRead, "count", scope));
        out.add(new MetricSample("peak_execution_memory", t.peakExecutionMemory, "bytes", scope));
        out.add(new MetricSample("task_count", t.taskCount, "count", scope));
        return out;
    }

    /** Every metric under both scopes. */
    public synchronized List<MetricSample> allSamples() {
        List<MetricSample> out = new ArrayList<>(20);
        out.addAll(samples(AttemptScope.ALL_ATTEMPTS));
        out.addAll(samples(AttemptScope.SUCCESSFUL_ONLY));
        return out;
    }
}
