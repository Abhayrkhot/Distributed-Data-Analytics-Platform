package com.analyticsplatform.common.dao;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.analyticsplatform.common.config.PlatformConfig;
import com.analyticsplatform.common.dao.LineageRecorder.EdgeType;
import com.analyticsplatform.common.dao.LineageRecorder.LineageException;
import com.analyticsplatform.common.dao.LineageRecorder.NodeType;
import com.analyticsplatform.common.run.ControlPlane.RunSpec;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * {@link LineageRecorder} against the real control plane.
 *
 * <p>Asserts the graph programmatically rather than eyeballing {@code control.v_lineage}, which is
 * the Tier 1 evidence for the lineage claim.
 */
class LineageRecorderIT {

    private static ConnectionSource connections;

    private LineageRecorder recorder;
    private JdbcControlPlane controlPlane;
    private long runId;
    private String suffix;
    private String bronze;
    private String silver;
    private String job;

    @BeforeAll
    static void connect() {
        connections = ConnectionSource.postgres(PlatformConfig.fromEnvironment());
    }

    @BeforeEach
    void setUp() {
        suffix = UUID.randomUUID().toString();
        bronze = "it.bronze." + suffix;
        silver = "it.silver." + suffix;
        job = "ITJob-" + suffix;

        recorder = new LineageRecorder(connections);
        controlPlane = new JdbcControlPlane(connections);
        runId = controlPlane.startRun(RunSpec.of("IT-" + suffix));
    }

    @AfterEach
    void cleanUp() throws SQLException {
        try (Connection connection = connections.open()) {
            // Edges reference nodes, so remove edges before the nodes they point at.
            try (PreparedStatement statement = connection.prepareStatement(
                    "DELETE FROM control.lineage_edge WHERE run_id = ?")) {
                statement.setLong(1, runId);
                statement.executeUpdate();
            }
            try (PreparedStatement statement = connection.prepareStatement(
                    "DELETE FROM control.lineage_node WHERE node_name LIKE ?")) {
                statement.setString(1, "%" + suffix);
                statement.executeUpdate();
            }
            try (PreparedStatement statement = connection.prepareStatement(
                    "DELETE FROM control.etl_run WHERE run_id = ?")) {
                statement.setLong(1, runId);
                statement.executeUpdate();
            }
        }
    }

    private List<String> edges() throws SQLException {
        List<String> out = new ArrayList<>();
        try (Connection connection = connections.open();
             PreparedStatement statement = connection.prepareStatement("""
                     SELECT source, edge_type, target FROM control.v_lineage
                      WHERE run_id = ? ORDER BY source, edge_type, target
                     """)) {
            statement.setLong(1, runId);
            try (ResultSet rows = statement.executeQuery()) {
                while (rows.next()) {
                    out.add(rows.getString(1) + " -" + rows.getString(2) + "-> " + rows.getString(3));
                }
            }
        }
        return out;
    }

    @Test
    @DisplayName("a transformation records reads, writes and derives")
    void transformationRecordsAllThreeEdges() throws SQLException {
        recorder.recordTransformation(runId, job, bronze, silver);

        assertThat(edges()).containsExactlyInAnyOrder(
                bronze + " -reads-> " + job,
                bronze + " -derives-> " + silver,
                job + " -writes-> " + silver);
    }

    /**
     * The question lineage is actually asked: what breaks if this source changes. The derives edge
     * is what answers it without walking through the job node.
     */
    @Test
    @DisplayName("multiple sources each derive the target")
    void multipleSourcesEachDeriveTheTarget() throws SQLException {
        String otherBronze = "it.bronze2." + suffix;
        recorder.recordTransformation(runId, job, List.of(bronze, otherBronze), silver);

        assertThat(edges())
                .contains(bronze + " -derives-> " + silver)
                .contains(otherBronze + " -derives-> " + silver)
                .contains(job + " -writes-> " + silver)
                .hasSize(5);
    }

    /** A retried job must not fill the graph with duplicate edges that look like structure. */
    @Test
    @DisplayName("re-recording the same transformation does not duplicate edges")
    void recordingIsIdempotentWithinARun() throws SQLException {
        recorder.recordTransformation(runId, job, bronze, silver);
        recorder.recordTransformation(runId, job, bronze, silver);
        recorder.recordTransformation(runId, job, bronze, silver);

        assertThat(edges()).hasSize(3);
    }

    @Test
    @DisplayName("nodes are created on demand")
    void unknownNodesAreRegistered() throws SQLException {
        recorder.recordTransformation(runId, job, bronze, silver);

        try (Connection connection = connections.open();
             PreparedStatement statement = connection.prepareStatement(
                     "SELECT node_type FROM control.lineage_node WHERE node_name = ?")) {
            statement.setString(1, job);
            try (ResultSet rows = statement.executeQuery()) {
                assertThat(rows.next()).isTrue();
                assertThat(rows.getString(1)).isEqualTo("job");
            }
        }
    }

    @Test
    @DisplayName("a self-referential edge is refused with a useful message")
    void selfEdgeIsRefused() {
        assertThatThrownBy(() -> recorder.recordEdge(
                runId, bronze, NodeType.DATASET, bronze, NodeType.DATASET, EdgeType.DERIVES))
                .isInstanceOf(LineageException.class)
                .hasMessageContaining("self-referential");
    }

    /**
     * The global invariant: a run that published nothing must leave no lineage. Callers place
     * lineage writes after the manifest insert, so a failed run never reaches them.
     */
    @Test
    @DisplayName("a run that records nothing leaves an empty graph")
    void aRunThatPublishesNothingHasNoLineage() throws SQLException {
        assertThat(edges()).isEmpty();
    }

    @Test
    @DisplayName("edges from different runs are distinguishable")
    void edgesAreAttributedToTheirRun() throws SQLException {
        recorder.recordTransformation(runId, job, bronze, silver);

        long otherRun = controlPlane.startRun(RunSpec.of("IT-" + suffix));
        try {
            recorder.recordTransformation(otherRun, job, bronze, silver);

            assertThat(edges()).hasSize(3);   // scoped to runId only
        } finally {
            try (Connection connection = connections.open();
                 PreparedStatement statement = connection.prepareStatement(
                         "DELETE FROM control.lineage_edge WHERE run_id = ?")) {
                statement.setLong(1, otherRun);
                statement.executeUpdate();
            }
            try (Connection connection = connections.open();
                 PreparedStatement statement = connection.prepareStatement(
                         "DELETE FROM control.etl_run WHERE run_id = ?")) {
                statement.setLong(1, otherRun);
                statement.executeUpdate();
            }
        }
    }
}
