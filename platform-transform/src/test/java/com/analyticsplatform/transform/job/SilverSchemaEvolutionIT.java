package com.analyticsplatform.transform.job;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.analyticsplatform.common.config.PlatformConfig;
import com.analyticsplatform.common.dao.ConnectionSource;
import com.analyticsplatform.common.dao.JdbcControlPlane;
import com.analyticsplatform.common.dao.LineageRecorder;
import com.analyticsplatform.common.run.ControlPlane.RunSpec;
import com.analyticsplatform.common.schema.CanonicalSchema;
import com.analyticsplatform.common.schema.SchemaRegistry;
import com.analyticsplatform.common.schema.SchemaRegistry.SchemaEvolutionException;
import com.analyticsplatform.common.testing.Fixtures;
import com.analyticsplatform.common.testing.SparkTestSupport;
import com.analyticsplatform.ingest.publish.ProcessingUnitStore;
import com.analyticsplatform.ingest.publish.ProcessingUnitStore.UnitKey;
import com.analyticsplatform.ingest.publish.StagedPublisher;
import com.analyticsplatform.ingest.source.SourceNormalizer;
import com.analyticsplatform.transform.dq.DqEngine;
import com.analyticsplatform.transform.dq.DqRuleStore;
import com.analyticsplatform.transform.job.SilverTransformJob.Inputs;
import com.analyticsplatform.transform.silver.SilverTransform;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tier 1 evidence that a breaking schema change prevents publication.
 *
 * <p>Note a property of the design worth being explicit about: silver's schema is <strong>pinned by
 * its explicit {@code select()}</strong>, so it cannot drift because bronze gained a column. That is
 * deliberate — silver is a published contract and should not change shape because an upstream file
 * did — and it means the evolution the registry guards against here is a change to the
 * transformation itself, not to its input.
 *
 * <p>The real 2024→2025 additive case (the {@code cbd_congestion_fee} column NYC added) is exercised
 * against actual TLC schemas in {@code SchemaRegistryIT}. This suite covers the consequence that
 * matters operationally: when the registry refuses, does any data reach the warehouse?
 */
class SilverSchemaEvolutionIT {

    private static ConnectionSource connections;
    private static JdbcControlPlane controlPlane;

    private String dataset;
    private String suffix;
    private long runId;
    private ProcessingUnitStore store;
    private Dataset<Row> bronze;
    private UnitKey key;

    @TempDir
    Path root;

    private Path target;

    @BeforeAll
    static void connect() {
        connections = ConnectionSource.postgres(PlatformConfig.fromEnvironment());
        controlPlane = new JdbcControlPlane(connections);
    }

    @BeforeEach
    void setUp() {
        suffix = UUID.randomUUID().toString();
        dataset = "it.silverschema." + suffix;
        runId = controlPlane.startRun(RunSpec.of("IT-schema-" + suffix));
        target = root.resolve("warehouse/silver");
        store = new ProcessingUnitStore(connections);
        key = new UnitKey(dataset, SilverTransformJob.STAGE, "yellow/2024-01");

        SilverTransform.registerUdfs(SparkTestSupport.spark());
        bronze = SourceNormalizer.normalizeYellow(Fixtures.yellow2024())
                .union(SourceNormalizer.normalizeYellow(Fixtures.yellow2025()))
                .union(SourceNormalizer.normalizeGreen(Fixtures.green2024()));
    }

    @AfterEach
    void cleanUp() throws Exception {
        try (Connection connection = connections.open()) {
            for (String sql : new String[] {
                "DELETE FROM control.unit_manifest WHERE dataset_name = ?",
                "DELETE FROM control.processing_unit WHERE dataset_name = ?",
                "DELETE FROM control.schema_version WHERE dataset_name = ?"}) {
                try (PreparedStatement statement = connection.prepareStatement(sql)) {
                    statement.setString(1, dataset);
                    statement.executeUpdate();
                }
            }
            try (PreparedStatement statement = connection.prepareStatement(
                    "DELETE FROM control.lineage_edge WHERE run_id = ?")) {
                statement.setLong(1, runId);
                statement.executeUpdate();
            }
            try (PreparedStatement statement = connection.prepareStatement(
                    "DELETE FROM control.lineage_node WHERE node_name = ?")) {
                statement.setString(1, dataset);
                statement.executeUpdate();
            }
            try (PreparedStatement statement = connection.prepareStatement(
                    "DELETE FROM control.etl_run WHERE run_id = ?")) {
                statement.setLong(1, runId);
                statement.executeUpdate();
            }
        }
    }

    private SilverTransformJob job(SchemaRegistry.Policy policy) {
        return new SilverTransformJob(
                SparkTestSupport.spark(), dataset,
                new SchemaRegistry(connections, policy),
                new DqRuleStore(connections), new DqEngine(),
                new StagedPublisher(store, root.resolve("staging"), 900),
                new LineageRecorder(connections));
    }

    private Inputs inputs() {
        return new Inputs(bronze, Fixtures.taxiZones(),
                "yellow/2024-01", target, runId, "owner-" + suffix);
    }

    /** The schema silver actually produces, without running the job. */
    private StructType producedSchema() {
        return SilverTransform.transform(bronze, Fixtures.taxiZones()).schema();
    }

    /**
     * Pre-registers version 1 with one field's type replaced, so the job's real schema becomes a
     * transition from it.
     */
    private void preRegister(String field, org.apache.spark.sql.types.DataType type)
            throws Exception {
        StructType produced = producedSchema();
        List<StructField> fields = new ArrayList<>();
        for (StructField existing : produced.fields()) {
            fields.add(existing.name().equals(field)
                    ? new StructField(existing.name(), type, existing.nullable(), Metadata.empty())
                    : existing);
        }
        StructType altered = new StructType(fields.toArray(new StructField[0]));

        try (Connection connection = connections.open();
             PreparedStatement statement = connection.prepareStatement("""
                     INSERT INTO control.schema_version
                         (dataset_name, version, schema_json, schema_hash, change_type)
                     VALUES (?, 1, ?::jsonb, ?, 'initial')
                     """)) {
            statement.setString(1, dataset);
            statement.setString(2, altered.json());
            statement.setString(3, CanonicalSchema.hash(altered));
            statement.executeUpdate();
        }
    }

    private void preRegisterWithout(String field) throws Exception {
        StructType produced = producedSchema();
        List<StructField> fields = new ArrayList<>();
        for (StructField existing : produced.fields()) {
            if (!existing.name().equals(field)) {
                fields.add(existing);
            }
        }
        StructType reduced = new StructType(fields.toArray(new StructField[0]));

        try (Connection connection = connections.open();
             PreparedStatement statement = connection.prepareStatement("""
                     INSERT INTO control.schema_version
                         (dataset_name, version, schema_json, schema_hash, change_type)
                     VALUES (?, 1, ?::jsonb, ?, 'initial')
                     """)) {
            statement.setString(1, dataset);
            statement.setString(2, reduced.json());
            statement.setString(3, CanonicalSchema.hash(reduced));
            statement.executeUpdate();
        }
    }

    private int versionCount() throws Exception {
        try (Connection connection = connections.open();
             PreparedStatement statement = connection.prepareStatement(
                     "SELECT count(*) FROM control.schema_version WHERE dataset_name = ?")) {
            statement.setString(1, dataset);
            try (ResultSet rows = statement.executeQuery()) {
                rows.next();
                return rows.getInt(1);
            }
        }
    }

    private String changeTypeOf(int version) throws Exception {
        try (Connection connection = connections.open();
             PreparedStatement statement = connection.prepareStatement(
                     "SELECT change_type FROM control.schema_version "
                             + "WHERE dataset_name = ? AND version = ?")) {
            statement.setString(1, dataset);
            statement.setInt(2, version);
            try (ResultSet rows = statement.executeQuery()) {
                return rows.next() ? rows.getString(1) : null;
            }
        }
    }

    @Test
    @DisplayName("a first run registers version 1 and publishes")
    void firstRunRegistersAndPublishes() throws Exception {
        assertThat(job(SchemaRegistry.Policy.ALLOW_WIDENING).run(inputs()).published()).isTrue();

        assertThat(versionCount()).isEqualTo(1);
        assertThat(changeTypeOf(1)).isEqualTo("initial");
        assertThat(Files.isDirectory(target)).isTrue();
    }

    @Test
    @DisplayName("an unchanged schema on rerun adds no version")
    void rerunAddsNoVersion() throws Exception {
        job(SchemaRegistry.Policy.ALLOW_WIDENING).run(inputs());
        job(SchemaRegistry.Policy.ALLOW_WIDENING).run(inputs());

        assertThat(versionCount()).isEqualTo(1);
    }

    /** A new nullable column is additive and must proceed. */
    @Test
    @DisplayName("an added nullable column registers as additive and publishes")
    void additiveChangePublishes() throws Exception {
        preRegisterWithout("cbd_congestion_fee");

        assertThat(job(SchemaRegistry.Policy.ALLOW_WIDENING).run(inputs()).published()).isTrue();

        assertThat(versionCount()).isEqualTo(2);
        assertThat(changeTypeOf(2)).isEqualTo("additive");
    }

    /** int → long is widening: lossless, therefore permitted. */
    @Test
    @DisplayName("a widening type change registers as widening and publishes")
    void wideningChangePublishes() throws Exception {
        // Registered as short; silver produces int. short -> int loses nothing.
        preRegister("vendor_id", DataTypes.ShortType);

        assertThat(job(SchemaRegistry.Policy.ALLOW_WIDENING).run(inputs()).published()).isTrue();

        assertThat(versionCount()).isEqualTo(2);
        assertThat(changeTypeOf(2)).isEqualTo("widening");
    }

    /**
     * The gate. Registered as long, silver produces int — a narrowing that loses values above
     * 2^31. The run must abort and, critically, publish nothing.
     */
    @Test
    @DisplayName("a narrowing type change aborts and publishes nothing")
    void breakingChangePublishesNothing() throws Exception {
        preRegister("vendor_id", DataTypes.LongType);

        Throwable thrown = catchThrowable(() ->
                job(SchemaRegistry.Policy.ALLOW_WIDENING).run(inputs()));

        assertThat(thrown).isInstanceOf(SchemaEvolutionException.class);
        assertThat(thrown).hasMessageContaining("breaking schema change refused");

        assertThat(Files.isDirectory(target)).as("NOTHING may be published").isFalse();
        assertThat(store.findManifest(key)).isEmpty();
        assertThat(versionCount()).as("no new version recorded").isEqualTo(1);
    }

    /** The abort happens before any staging is written, not after. */
    @Test
    @DisplayName("a breaking change aborts before the unit is even claimed")
    void breakingChangeAbortsBeforeClaiming() throws Exception {
        preRegister("vendor_id", DataTypes.LongType);

        catchThrowable(() -> job(SchemaRegistry.Policy.ALLOW_WIDENING).run(inputs()));

        assertThat(store.status(key))
                .as("the unit was never claimed, so it has no processing state").isEmpty();
    }

    /** A dropped column is breaking too, and equally must not publish. */
    @Test
    @DisplayName("a dropped column aborts and publishes nothing")
    void droppedColumnPublishesNothing() throws Exception {
        StructType produced = producedSchema();
        List<StructField> extended = new ArrayList<>(List.of(produced.fields()));
        extended.add(new StructField("legacy_column", DataTypes.StringType, true, Metadata.empty()));
        StructType withExtra = new StructType(extended.toArray(new StructField[0]));

        try (Connection connection = connections.open();
             PreparedStatement statement = connection.prepareStatement("""
                     INSERT INTO control.schema_version
                         (dataset_name, version, schema_json, schema_hash, change_type)
                     VALUES (?, 1, ?::jsonb, ?, 'initial')
                     """)) {
            statement.setString(1, dataset);
            statement.setString(2, withExtra.json());
            statement.setString(3, CanonicalSchema.hash(withExtra));
            statement.executeUpdate();
        }

        assertThat(catchThrowable(() -> job(SchemaRegistry.Policy.ALLOW_WIDENING).run(inputs())))
                .isInstanceOf(SchemaEvolutionException.class);
        assertThat(Files.isDirectory(target)).isFalse();
    }

    /** STRICT refuses even an additive change, and still publishes nothing. */
    @Test
    @DisplayName("STRICT policy refuses an additive change and publishes nothing")
    void strictPolicyRefusesAdditive() throws Exception {
        preRegisterWithout("cbd_congestion_fee");

        assertThat(catchThrowable(() -> job(SchemaRegistry.Policy.STRICT).run(inputs())))
                .isInstanceOf(SchemaEvolutionException.class)
                .hasMessageContaining("STRICT");
        assertThat(Files.isDirectory(target)).isFalse();
    }
}
