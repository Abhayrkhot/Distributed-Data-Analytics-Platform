package com.analyticsplatform.stream.epoch;

import com.analyticsplatform.common.dao.ConnectionSource;
import com.analyticsplatform.common.stream.StreamVersion;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;

/**
 * Allocates monotonic epochs for streaming queries.
 *
 * <h2>Why an epoch exists at all</h2>
 *
 * <p>Spark's {@code foreachBatch} batch id is scoped to one checkpoint lineage. A fresh checkpoint
 * restarts numbering at 0, so {@code batch_id} alone is not globally unique — and worse, a new query
 * writing {@code batch_id = 0} would <em>lose</em> the {@code ReplacingMergeTree} version comparison
 * against an existing {@code batch_id = 42} and silently fail to replace it. The row would look
 * updated and would not be.
 *
 * <p>The epoch comes from a Postgres sequence, so it is monotonic by construction rather than by
 * anyone remembering to increment it. Allocation is a single {@code INSERT ... ON CONFLICT DO
 * NOTHING} keyed on the checkpoint location: a genuinely fresh checkpoint gets exactly one epoch, and
 * a restart against the same checkpoint reuses it.
 */
public final class StreamEpochStore {

    /**
     * Allocation and lookup in one statement.
     *
     * <p>{@code ON CONFLICT DO NOTHING} makes concurrent allocation safe, but it also returns no row
     * when the checkpoint already exists — which is why the read follows rather than relying on
     * {@code RETURNING} alone.
     */
    private static final String ALLOCATE = """
            INSERT INTO control.stream_epoch (checkpoint_id, stream_query_id)
            VALUES (?, ?)
            ON CONFLICT (checkpoint_id) DO NOTHING
            """;

    private static final String SELECT = """
            SELECT epoch, stream_query_id FROM control.stream_epoch WHERE checkpoint_id = ?
            """;

    private static final String MAX_EPOCH = "SELECT coalesce(max(epoch), 0) FROM control.stream_epoch";

    /** An allocated epoch and the query it belongs to. */
    public record Allocation(String checkpointId, long epoch, String streamQueryId, boolean fresh) {

        /**
         * Packs a batch id into a globally-ordered version.
         *
         * <p>This is the value {@code ReplacingMergeTree} compares, so it must increase across
         * checkpoint resets as well as within a query.
         */
        public long versionFor(long batchId) {
            return StreamVersion.of(epoch, batchId);
        }
    }

    private final ConnectionSource connections;

    public StreamEpochStore(ConnectionSource connections) {
        this.connections = connections;
    }

    /**
     * Returns the epoch for a checkpoint, allocating one if this is its first use.
     *
     * @param checkpointId the checkpoint location; identity of the lineage, not of the process
     * @param streamQueryId Spark's query id, recorded as provenance
     */
    public Allocation allocate(String checkpointId, String streamQueryId) {
        if (checkpointId == null || checkpointId.isBlank()) {
            throw new IllegalArgumentException("checkpointId is required");
        }

        try (Connection connection = connections.open()) {
            int inserted;
            try (PreparedStatement statement = connection.prepareStatement(ALLOCATE)) {
                statement.setString(1, checkpointId);
                statement.setString(2, streamQueryId);
                inserted = statement.executeUpdate();
            }

            try (PreparedStatement statement = connection.prepareStatement(SELECT)) {
                statement.setString(1, checkpointId);
                try (ResultSet rows = statement.executeQuery()) {
                    if (!rows.next()) {
                        throw new EpochException(
                                "epoch vanished immediately after allocation for " + checkpointId);
                    }
                    return new Allocation(
                            checkpointId, rows.getLong(1), rows.getString(2), inserted == 1);
                }
            }
        } catch (SQLException e) {
            throw new EpochException("failed to allocate epoch for " + checkpointId, e);
        }
    }

    /** The epoch for a checkpoint, if one was ever allocated. */
    public Optional<Long> epochOf(String checkpointId) {
        try (Connection connection = connections.open();
             PreparedStatement statement = connection.prepareStatement(SELECT)) {
            statement.setString(1, checkpointId);
            try (ResultSet rows = statement.executeQuery()) {
                return rows.next() ? Optional.of(rows.getLong(1)) : Optional.empty();
            }
        } catch (SQLException e) {
            throw new EpochException("failed to read epoch for " + checkpointId, e);
        }
    }

    /** Highest epoch allocated so far; used to assert monotonicity in tests. */
    public long highestEpoch() {
        try (Connection connection = connections.open();
             PreparedStatement statement = connection.prepareStatement(MAX_EPOCH);
             ResultSet rows = statement.executeQuery()) {
            rows.next();
            return rows.getLong(1);
        } catch (SQLException e) {
            throw new EpochException("failed to read the highest epoch", e);
        }
    }

    /** Wraps SQL failures from epoch allocation. */
    public static final class EpochException extends RuntimeException {
        EpochException(String message) {
            super(message);
        }

        EpochException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
