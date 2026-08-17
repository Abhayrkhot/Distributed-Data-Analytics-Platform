package com.analyticsplatform.ingest.publish;

import com.analyticsplatform.common.dao.ConnectionSource;
import com.analyticsplatform.common.unit.ProcessingUnitState.Status;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Locale;
import java.util.Optional;

/**
 * Lease-based ownership and commit records for processing units.
 *
 * <p>Claiming is a <strong>single atomic statement</strong>. The obvious implementation — read the
 * status, decide, then write — has a window between the read and the write in which another worker
 * can claim the same unit, and both then believe they own it. That is not a rare race: two workers
 * starting together hit it almost every time. The upsert below folds the decision into the
 * {@code WHERE} clause so the database arbitrates.
 *
 * <p>One statement covers four cases: a unit that does not exist yet, one that is {@code PENDING},
 * one that {@code FAILED} and is retryable, and one stuck {@code RUNNING} whose owner died and
 * whose lease has expired. It refuses a {@code COMPLETE} unit and one whose lease is still valid.
 */
public final class ProcessingUnitStore {

    /** Identifies a unit of work. */
    public record UnitKey(String datasetName, String pipelineStage, String processingUnit) {
        public UnitKey {
            if (datasetName == null || pipelineStage == null || processingUnit == null) {
                throw new IllegalArgumentException("unit key components must not be null");
            }
        }

        @Override
        public String toString() {
            return datasetName + "/" + pipelineStage + "/" + processingUnit;
        }
    }

    /** A successful claim. */
    public record Claim(UnitKey key, String owner, int attemptCount, String stagingPath) {
    }

    /** A committed unit, read back from the manifest. */
    public record Manifest(
            UnitKey key,
            long runId,
            String schemaHash,
            long rowCount,
            int fileCount,
            long totalBytes,
            String sourceFingerprint,
            String targetPath) {

        /** The fingerprint this manifest asserts about the published target. */
        public UnitFingerprint fingerprint() {
            return new UnitFingerprint(rowCount, fileCount, totalBytes, schemaHash, sourceFingerprint);
        }
    }

    /**
     * Claim, expressed so the database decides. The WHERE clause on the DO UPDATE branch is what
     * makes this safe: a COMPLETE unit, or a RUNNING one whose lease is still valid, matches
     * nothing and the statement affects zero rows.
     */
    private static final String CLAIM = """
            INSERT INTO control.processing_unit
                (dataset_name, pipeline_stage, processing_unit, status,
                 lease_owner, lease_expires_at, attempt_count, staging_path, last_run_id)
            VALUES (?, ?, ?, 'RUNNING', ?, now() + make_interval(secs => ?), 1, ?, ?)
            ON CONFLICT (dataset_name, pipeline_stage, processing_unit) DO UPDATE
               SET status           = 'RUNNING',
                   lease_owner      = EXCLUDED.lease_owner,
                   lease_expires_at = EXCLUDED.lease_expires_at,
                   attempt_count    = control.processing_unit.attempt_count + 1,
                   staging_path     = EXCLUDED.staging_path,
                   last_run_id      = EXCLUDED.last_run_id,
                   error_message    = NULL,
                   updated_at       = now()
             WHERE control.processing_unit.status IN ('PENDING', 'FAILED')
                OR (control.processing_unit.status = 'RUNNING'
                    AND control.processing_unit.lease_expires_at < now())
            RETURNING attempt_count
            """;

    /** Completion requires still holding the lease: a timed-out worker must not finish. */
    private static final String MARK_COMPLETE = """
            UPDATE control.processing_unit
               SET status = 'COMPLETE', lease_owner = NULL, lease_expires_at = NULL,
                   rows_processed = ?, error_message = NULL, updated_at = now()
             WHERE dataset_name = ? AND pipeline_stage = ? AND processing_unit = ?
               AND status = 'RUNNING' AND lease_owner = ?
            """;

    /**
     * Recovery-only. Repairs status for a unit whose manifest exists — the commit already
     * happened, only the bookkeeping was lost — so it deliberately does not require a lease.
     */
    private static final String REPAIR_COMPLETE = """
            UPDATE control.processing_unit p
               SET status = 'COMPLETE', lease_owner = NULL, lease_expires_at = NULL,
                   error_message = NULL, updated_at = now()
             WHERE p.dataset_name = ? AND p.pipeline_stage = ? AND p.processing_unit = ?
               AND p.status <> 'COMPLETE'
               AND EXISTS (SELECT 1 FROM control.unit_manifest m
                            WHERE m.dataset_name = p.dataset_name
                              AND m.pipeline_stage = p.pipeline_stage
                              AND m.processing_unit = p.processing_unit)
            """;

    private static final String MARK_FAILED = """
            UPDATE control.processing_unit
               SET status = 'FAILED', lease_owner = NULL, lease_expires_at = NULL,
                   error_message = ?, updated_at = now()
             WHERE dataset_name = ? AND pipeline_stage = ? AND processing_unit = ?
               AND status = 'RUNNING'
            """;

    /** The commit point: one atomic insert. */
    private static final String INSERT_MANIFEST = """
            INSERT INTO control.unit_manifest
                (dataset_name, pipeline_stage, processing_unit, run_id, schema_hash,
                 row_count, file_count, total_bytes, source_fingerprint, target_path)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """;

    private static final String SELECT_MANIFEST = """
            SELECT run_id, schema_hash, row_count, file_count, total_bytes,
                   source_fingerprint, target_path
              FROM control.unit_manifest
             WHERE dataset_name = ? AND pipeline_stage = ? AND processing_unit = ?
            """;

    private static final String SELECT_STATUS = """
            SELECT status FROM control.processing_unit
             WHERE dataset_name = ? AND pipeline_stage = ? AND processing_unit = ?
            """;

    private static final String DELETE_MANIFEST = """
            DELETE FROM control.unit_manifest
             WHERE dataset_name = ? AND pipeline_stage = ? AND processing_unit = ?
            """;

    /**
     * The explicit escape hatch out of COMPLETE.
     *
     * <p>Reconciliation needs this. Discarding a contradictory manifest without also resetting the
     * status leaves the unit COMPLETE, which {@link #CLAIM} refuses — so the corruption gets
     * detected and then becomes impossible to repair. Deliberately excludes a RUNNING unit holding
     * a valid lease, so this can never yank work away from a live worker.
     */
    private static final String FORCE_REBUILD = """
            UPDATE control.processing_unit
               SET status = 'PENDING', lease_owner = NULL, lease_expires_at = NULL,
                   error_message = ?, updated_at = now()
             WHERE dataset_name = ? AND pipeline_stage = ? AND processing_unit = ?
               AND (status <> 'RUNNING' OR lease_expires_at < now())
            """;

    private final ConnectionSource connections;

    public ProcessingUnitStore(ConnectionSource connections) {
        this.connections = connections;
    }

    /**
     * Attempts to take ownership.
     *
     * @return the claim, or empty when the unit is committed or actively owned by someone else
     */
    public Optional<Claim> claim(
            UnitKey key, String owner, int leaseSeconds, String stagingPath, Long runId) {

        try (Connection connection = connections.open();
             PreparedStatement statement = connection.prepareStatement(CLAIM)) {

            statement.setString(1, key.datasetName());
            statement.setString(2, key.pipelineStage());
            statement.setString(3, key.processingUnit());
            statement.setString(4, owner);
            statement.setInt(5, leaseSeconds);
            statement.setString(6, stagingPath);
            if (runId == null) {
                statement.setNull(7, java.sql.Types.BIGINT);
            } else {
                statement.setLong(7, runId);
            }

            try (ResultSet rows = statement.executeQuery()) {
                if (!rows.next()) {
                    return Optional.empty();
                }
                return Optional.of(new Claim(key, owner, rows.getInt(1), stagingPath));
            }
        } catch (SQLException e) {
            throw new PublishException("failed to claim " + key, e);
        }
    }

    /** Marks a claimed unit complete. Fails if the lease was lost in the meantime. */
    public void markComplete(UnitKey key, String owner, long rowsProcessed) {
        int updated = update(MARK_COMPLETE, statement -> {
            statement.setLong(1, rowsProcessed);
            statement.setString(2, key.datasetName());
            statement.setString(3, key.pipelineStage());
            statement.setString(4, key.processingUnit());
            statement.setString(5, owner);
        }, key);

        if (updated == 0) {
            throw new PublishException(
                    "cannot complete " + key + ": no longer RUNNING under owner " + owner
                            + " (lease lost, or another worker took over)");
        }
    }

    /**
     * Repairs a committed unit whose status write was lost.
     *
     * @return true if a repair happened
     */
    public boolean repairToComplete(UnitKey key) {
        return update(REPAIR_COMPLETE, statement -> {
            statement.setString(1, key.datasetName());
            statement.setString(2, key.pipelineStage());
            statement.setString(3, key.processingUnit());
        }, key) > 0;
    }

    public void markFailed(UnitKey key, String errorMessage) {
        update(MARK_FAILED, statement -> {
            statement.setString(1, truncate(errorMessage));
            statement.setString(2, key.datasetName());
            statement.setString(3, key.pipelineStage());
            statement.setString(4, key.processingUnit());
        }, key);
    }

    /**
     * Writes the commit record. This, not the presence of files, is what commits a unit.
     *
     * @throws PublishException if the unit is already committed
     */
    public void commit(UnitKey key, long runId, UnitFingerprint fingerprint, String targetPath) {
        try (Connection connection = connections.open();
             PreparedStatement statement = connection.prepareStatement(INSERT_MANIFEST)) {

            statement.setString(1, key.datasetName());
            statement.setString(2, key.pipelineStage());
            statement.setString(3, key.processingUnit());
            statement.setLong(4, runId);
            statement.setString(5, fingerprint.schemaHash());
            statement.setLong(6, fingerprint.rowCount());
            statement.setInt(7, fingerprint.fileCount());
            statement.setLong(8, fingerprint.totalBytes());
            statement.setString(9, fingerprint.contentHash());
            statement.setString(10, targetPath);
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new PublishException("failed to commit " + key + " (already committed?)", e);
        }
    }

    public Optional<Manifest> findManifest(UnitKey key) {
        try (Connection connection = connections.open();
             PreparedStatement statement = connection.prepareStatement(SELECT_MANIFEST)) {

            statement.setString(1, key.datasetName());
            statement.setString(2, key.pipelineStage());
            statement.setString(3, key.processingUnit());

            try (ResultSet rows = statement.executeQuery()) {
                if (!rows.next()) {
                    return Optional.empty();
                }
                return Optional.of(new Manifest(
                        key, rows.getLong(1), rows.getString(2), rows.getLong(3),
                        rows.getInt(4), rows.getLong(5), rows.getString(6), rows.getString(7)));
            }
        } catch (SQLException e) {
            throw new PublishException("failed to read manifest for " + key, e);
        }
    }

    /** Discards a commit record. Used only when reconciliation finds it contradicts the target. */
    public void discardManifest(UnitKey key) {
        update(DELETE_MANIFEST, statement -> {
            statement.setString(1, key.datasetName());
            statement.setString(2, key.pipelineStage());
            statement.setString(3, key.processingUnit());
        }, key);
    }

    /**
     * Resets a unit so it can be rebuilt, deliberately discarding committed state.
     *
     * <p>Always paired with {@link #discardManifest}: dropping the commit record alone would leave
     * the unit COMPLETE and therefore unclaimable forever.
     *
     * @return true if the unit was reset; false when a live worker still holds a valid lease
     */
    public boolean forceRebuild(UnitKey key, String reason) {
        return update(FORCE_REBUILD, statement -> {
            statement.setString(1, truncate(reason));
            statement.setString(2, key.datasetName());
            statement.setString(3, key.pipelineStage());
            statement.setString(4, key.processingUnit());
        }, key) > 0;
    }

    public Optional<Status> status(UnitKey key) {
        try (Connection connection = connections.open();
             PreparedStatement statement = connection.prepareStatement(SELECT_STATUS)) {

            statement.setString(1, key.datasetName());
            statement.setString(2, key.pipelineStage());
            statement.setString(3, key.processingUnit());

            try (ResultSet rows = statement.executeQuery()) {
                return rows.next()
                        ? Optional.of(Status.valueOf(rows.getString(1).toUpperCase(Locale.ROOT)))
                        : Optional.empty();
            }
        } catch (SQLException e) {
            throw new PublishException("failed to read status for " + key, e);
        }
    }

    private interface Binder {
        void bind(PreparedStatement statement) throws SQLException;
    }

    private int update(String sql, Binder binder, UnitKey key) {
        try (Connection connection = connections.open();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            binder.bind(statement);
            return statement.executeUpdate();
        } catch (SQLException e) {
            throw new PublishException("control-plane update failed for " + key, e);
        }
    }

    private static String truncate(String message) {
        if (message == null) {
            return null;
        }
        return message.length() <= 2000 ? message : message.substring(0, 2000) + "...[truncated]";
    }

    /** Wraps control-plane failures from the publish path. */
    public static final class PublishException extends RuntimeException {
        public PublishException(String message) {
            super(message);
        }

        public PublishException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
