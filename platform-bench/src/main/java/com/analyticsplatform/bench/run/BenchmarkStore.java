package com.analyticsplatform.bench.run;

import com.analyticsplatform.common.dao.ConnectionSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;

/**
 * Persists observations to {@code control.benchmark_run}.
 *
 * <p>Environment metadata is stored with every row, not once per report. A measurement without the
 * conditions that produced it cannot be reproduced or compared against a later one — and "we ran it
 * on the laptop that week" is not a condition anyone can check.
 */
public final class BenchmarkStore {

    private static final String INSERT = """
            INSERT INTO control.benchmark_run
                (run_id, experiment, config_label, ablation_step, iteration_index,
                 sequence_position, is_warmup, cache_policy, input_fingerprint,
                 input_row_count, input_bytes, input_file_count, output_fingerprint,
                 correctness_passed, spark_version, java_version, clickhouse_version,
                 docker_memory_bytes, worker_count, total_cores, executor_memory,
                 executor_overhead, git_commit, started_at, finished_at, duration_ms)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """;

    /** The conditions a measurement was taken under. */
    public record Environment(
            String sparkVersion,
            String javaVersion,
            String clickhouseVersion,
            Long dockerMemoryBytes,
            Integer workerCount,
            Integer totalCores,
            String executorMemory,
            String executorOverhead,
            String gitCommit,
            String cachePolicy) {

        /**
         * Captures what can be determined from the running JVM and session.
         *
         * <p>Values that cannot be determined are left null rather than guessed. A plausible-looking
         * default here would be worse than a gap: it would make an unreproducible measurement look
         * reproducible.
         */
        public static Environment capture(
                org.apache.spark.sql.SparkSession spark, String gitCommit) {
            return new Environment(
                    spark.version(),
                    System.getProperty("java.version"),
                    null,
                    null,
                    null,
                    Runtime.getRuntime().availableProcessors(),
                    spark.conf().getOption("spark.executor.memory").getOrElse(() -> null),
                    spark.conf().getOption("spark.executor.memoryOverhead").getOrElse(() -> null),
                    gitCommit,
                    "warm");
        }
    }

    private final ConnectionSource connections;

    public BenchmarkStore(ConnectionSource connections) {
        this.connections = connections;
    }

    /** Writes every observation, warm-ups included. */
    public void record(
            long runId, List<BenchmarkObservation> observations, Environment environment) {

        try (Connection connection = connections.open()) {
            connection.setAutoCommit(false);
            try (PreparedStatement statement = connection.prepareStatement(INSERT)) {
                for (BenchmarkObservation observation : observations) {
                    bind(statement, runId, observation, environment);
                    statement.addBatch();
                }
                statement.executeBatch();
            }
            connection.commit();
        } catch (SQLException e) {
            throw new IllegalStateException("failed to record benchmark observations", e);
        }
    }

    private void bind(
            PreparedStatement statement, long runId,
            BenchmarkObservation observation, Environment environment) throws SQLException {

        String label = observation.config().label();
        // Ladder and leave-one-out steps carry their own label; the three headline configs do not.
        boolean isAblationStep = label.matches("^[BL]\\d+_.*");

        int index = 1;
        statement.setLong(index++, runId);
        statement.setString(index++, observation.config().experiment().dbValue());
        statement.setString(index++, label);
        statement.setString(index++, isAblationStep ? label : null);
        statement.setInt(index++, observation.iterationIndex());
        statement.setInt(index++, observation.sequencePosition());
        statement.setBoolean(index++, observation.warmup());
        statement.setString(index++, environment.cachePolicy());
        statement.setString(index++, observation.inputFingerprint());
        statement.setLong(index++, observation.inputRowCount());
        statement.setLong(index++, observation.inputBytes());
        statement.setInt(index++, observation.inputFileCount());
        statement.setString(index++, observation.outputFingerprint());
        statement.setBoolean(index++, observation.correctnessPassed());
        statement.setString(index++, environment.sparkVersion());
        statement.setString(index++, environment.javaVersion());
        statement.setString(index++, environment.clickhouseVersion());
        setNullableLong(statement, index++, environment.dockerMemoryBytes());
        setNullableInt(statement, index++, environment.workerCount());
        setNullableInt(statement, index++, environment.totalCores());
        statement.setString(index++, environment.executorMemory());
        statement.setString(index++, environment.executorOverhead());
        statement.setString(index++, environment.gitCommit());
        statement.setTimestamp(index++, Timestamp.from(observation.startedAt()));
        statement.setTimestamp(index++, Timestamp.from(observation.finishedAt()));
        statement.setLong(index, observation.durationMillis());
    }

    private static void setNullableLong(PreparedStatement statement, int index, Long value)
            throws SQLException {
        if (value == null) {
            statement.setNull(index, java.sql.Types.BIGINT);
        } else {
            statement.setLong(index, value);
        }
    }

    private static void setNullableInt(PreparedStatement statement, int index, Integer value)
            throws SQLException {
        if (value == null) {
            statement.setNull(index, java.sql.Types.INTEGER);
        } else {
            statement.setInt(index, value);
        }
    }
}
