package com.analyticsplatform.common.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Locale;

/**
 * Records the lineage graph in {@code control.lineage_node} / {@code control.lineage_edge}.
 *
 * <p><strong>Call only after a unit is committed.</strong> The invariant the verification plan
 * asserts is that a failed or unpublished transformation leaves no lineage behind — a graph
 * claiming {@code silver.trip_clean} derives from {@code bronze.trip_raw} when that run aborted is
 * worse than no graph at all, because it is confidently wrong. This class does not enforce that
 * ordering (it cannot see the publish protocol), so callers place these writes after the manifest
 * insert, and {@code verify-platform.sh} checks the invariant globally.
 *
 * <p>A run typically writes three edges: job {@code reads} source, job {@code writes} target, and
 * source {@code derives} target. The first two answer "what did this execution touch"; the third
 * answers "what breaks if this source changes", which is the question anyone actually asks.
 */
public final class LineageRecorder {

    /** Node kinds, mirroring the CHECK constraint. */
    public enum NodeType {
        DATASET, JOB, EXTERNAL;

        String dbValue() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    /** Edge kinds, mirroring the CHECK constraint. */
    public enum EdgeType {
        READS, WRITES, DERIVES;

        String dbValue() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    /**
     * Nodes are seeded for known datasets and jobs, but a run may reference one that is not.
     * ON CONFLICT DO NOTHING makes registration idempotent without a read-then-write race.
     */
    private static final String UPSERT_NODE = """
            INSERT INTO control.lineage_node (node_name, node_type, node_layer)
            VALUES (?, ?, ?)
            ON CONFLICT (node_name, node_type) DO NOTHING
            """;

    private static final String SELECT_NODE = """
            SELECT node_id FROM control.lineage_node
             WHERE node_name = ? AND node_type = ?
            """;

    /**
     * Edges are deduplicated per run. Without this, a retried job appends a second identical edge
     * every attempt and the graph slowly fills with noise that looks like real structure.
     */
    private static final String INSERT_EDGE = """
            INSERT INTO control.lineage_edge
                (run_id, source_node_id, target_node_id, edge_type, column_mapping)
            SELECT ?, ?, ?, ?, ?::jsonb
             WHERE NOT EXISTS (
                   SELECT 1 FROM control.lineage_edge
                    WHERE run_id = ? AND source_node_id = ? AND target_node_id = ?
                      AND edge_type = ?)
            """;

    private final ConnectionSource connections;

    public LineageRecorder(ConnectionSource connections) {
        this.connections = connections;
    }

    /** One dataset feeding another, with the job that did it. */
    public void recordTransformation(
            long runId, String jobName, String sourceDataset, String targetDataset) {
        recordTransformation(runId, jobName, List.of(sourceDataset), targetDataset);
    }

    /**
     * Records a full transformation: the job reads each source, writes the target, and each source
     * derives the target.
     */
    public void recordTransformation(
            long runId, String jobName, List<String> sourceDatasets, String targetDataset) {

        try (Connection connection = connections.open()) {
            connection.setAutoCommit(false);
            try {
                long jobNode = nodeId(connection, jobName, NodeType.JOB, null);
                long targetNode = nodeId(connection, targetDataset, NodeType.DATASET, null);

                for (String source : sourceDatasets) {
                    long sourceNode = nodeId(connection, source, NodeType.DATASET, null);
                    edge(connection, runId, sourceNode, jobNode, EdgeType.READS);
                    edge(connection, runId, sourceNode, targetNode, EdgeType.DERIVES);
                }
                edge(connection, runId, jobNode, targetNode, EdgeType.WRITES);

                connection.commit();
            } catch (RuntimeException | SQLException e) {
                connection.rollback();
                throw e;
            } finally {
                connection.setAutoCommit(true);
            }
        } catch (SQLException e) {
            throw new LineageException(
                    "failed to record lineage for run " + runId + " -> " + targetDataset, e);
        }
    }

    /** A single edge, for callers that need finer control than a whole transformation. */
    public void recordEdge(
            long runId, String source, NodeType sourceType,
            String target, NodeType targetType, EdgeType edgeType) {

        try (Connection connection = connections.open()) {
            long sourceNode = nodeId(connection, source, sourceType, null);
            long targetNode = nodeId(connection, target, targetType, null);
            edge(connection, runId, sourceNode, targetNode, edgeType);
        } catch (SQLException e) {
            throw new LineageException("failed to record lineage edge for run " + runId, e);
        }
    }

    /** Registers a node if absent and returns its id. */
    private static long nodeId(Connection connection, String name, NodeType type, String layer)
            throws SQLException {

        try (PreparedStatement insert = connection.prepareStatement(UPSERT_NODE)) {
            insert.setString(1, name);
            insert.setString(2, type.dbValue());
            insert.setString(3, layer);
            insert.executeUpdate();
        }
        try (PreparedStatement select = connection.prepareStatement(SELECT_NODE)) {
            select.setString(1, name);
            select.setString(2, type.dbValue());
            try (ResultSet rows = select.executeQuery()) {
                if (!rows.next()) {
                    throw new LineageException("lineage node vanished after upsert: " + name);
                }
                return rows.getLong(1);
            }
        }
    }

    private static void edge(
            Connection connection, long runId, long source, long target, EdgeType type)
            throws SQLException {

        if (source == target) {
            // The schema rejects self-edges; catching it here gives a message that names the
            // node rather than surfacing a raw constraint violation.
            throw new LineageException(
                    "refusing self-referential lineage edge (node " + source + ")");
        }
        try (PreparedStatement statement = connection.prepareStatement(INSERT_EDGE)) {
            statement.setLong(1, runId);
            statement.setLong(2, source);
            statement.setLong(3, target);
            statement.setString(4, type.dbValue());
            statement.setString(5, null);
            statement.setLong(6, runId);
            statement.setLong(7, source);
            statement.setLong(8, target);
            statement.setString(9, type.dbValue());
            statement.executeUpdate();
        }
    }

    /** Wraps SQL failures so callers need not handle checked {@link SQLException}. */
    public static final class LineageException extends RuntimeException {
        public LineageException(String message) {
            super(message);
        }

        public LineageException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
