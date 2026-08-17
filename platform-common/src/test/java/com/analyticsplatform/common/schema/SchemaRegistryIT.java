package com.analyticsplatform.common.schema;

import static com.analyticsplatform.common.schema.Schemas.f;
import static com.analyticsplatform.common.schema.Schemas.of;
import static com.analyticsplatform.common.schema.Schemas.type;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.analyticsplatform.common.config.PlatformConfig;
import com.analyticsplatform.common.dao.ConnectionSource;
import com.analyticsplatform.common.schema.SchemaCompatibility.ChangeType;
import com.analyticsplatform.common.schema.SchemaRegistry.Policy;
import com.analyticsplatform.common.schema.SchemaRegistry.RegisteredSchema;
import com.analyticsplatform.common.schema.SchemaRegistry.SchemaEvolutionException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * {@link SchemaRegistry} against the real control plane.
 *
 * <p>Covers the Tier 1 evidence for the schema-evolution claim: the genuine 2024 to 2025 TLC
 * transition, rejection of a narrowing change, and the guarantee that a rejected change writes
 * nothing.
 */
class SchemaRegistryIT {

    /** The 2024 yellow taxi shape, reduced to the columns that matter here. */
    private static final StructType YELLOW_2024 = of(
            f("vendor_id", type("int")),
            f("fare_amount", type("double")),
            f("passenger_count", type("long")));

    /** 2025 added cbd_congestion_fee. This is the real change, not a synthetic one. */
    private static final StructType YELLOW_2025 = of(
            f("vendor_id", type("int")),
            f("fare_amount", type("double")),
            f("passenger_count", type("long")),
            f("cbd_congestion_fee", type("decimal(10,2)")));

    private static ConnectionSource connections;

    private String dataset;
    private SchemaRegistry registry;

    @BeforeAll
    static void connect() {
        connections = ConnectionSource.postgres(PlatformConfig.fromEnvironment());
    }

    @BeforeEach
    void setUp() {
        dataset = "it.schema." + UUID.randomUUID();
        registry = new SchemaRegistry(connections, Policy.ALLOW_WIDENING);
    }

    @AfterEach
    void cleanUp() throws SQLException {
        try (Connection connection = connections.open();
             PreparedStatement statement = connection.prepareStatement(
                     "DELETE FROM control.schema_version WHERE dataset_name = ?")) {
            statement.setString(1, dataset);
            statement.executeUpdate();
        }
    }

    private int versionCount() throws SQLException {
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

    private List<String> addedColumnsOf(int version) throws SQLException {
        try (Connection connection = connections.open();
             PreparedStatement statement = connection.prepareStatement(
                     "SELECT added_columns FROM control.schema_version "
                             + "WHERE dataset_name = ? AND version = ?")) {
            statement.setString(1, dataset);
            statement.setInt(2, version);
            try (ResultSet rows = statement.executeQuery()) {
                rows.next();
                return List.of((String[]) rows.getArray(1).getArray());
            }
        }
    }

    @Test
    @DisplayName("the first registration is version 1")
    void firstRegistrationIsInitial() throws SQLException {
        RegisteredSchema result = registry.register(dataset, YELLOW_2024);

        assertThat(result.version()).isEqualTo(1);
        assertThat(result.changeType()).isEqualTo(ChangeType.INITIAL);
        assertThat(result.isNewVersion()).isTrue();
        assertThat(versionCount()).isEqualTo(1);
    }

    @Test
    @DisplayName("re-registering an identical schema writes nothing")
    void identicalSchemaIsNotVersionedAgain() throws SQLException {
        registry.register(dataset, YELLOW_2024);
        RegisteredSchema second = registry.register(dataset, YELLOW_2024);

        assertThat(second.version()).isEqualTo(1);
        assertThat(second.isNewVersion()).isFalse();
        assertThat(versionCount()).isEqualTo(1);
    }

    /** The payoff from canonicalizing before hashing: field order is not a schema change. */
    @Test
    @DisplayName("reordered fields are not a new version")
    void reorderedFieldsAreNotAChange() throws SQLException {
        registry.register(dataset, YELLOW_2024);

        StructType reordered = of(
                f("passenger_count", type("long")),
                f("vendor_id", type("int")),
                f("fare_amount", type("double")));

        assertThat(registry.register(dataset, reordered).isNewVersion()).isFalse();
        assertThat(versionCount()).isEqualTo(1);
    }

    /** Tier 1 evidence for "schema evolution": the actual TLC 2024 to 2025 transition. */
    @Test
    @DisplayName("the real 2024 to 2025 change registers as additive")
    void realWorldAdditiveChange() throws SQLException {
        registry.register(dataset, YELLOW_2024);
        RegisteredSchema evolved = registry.register(dataset, YELLOW_2025);

        assertThat(evolved.version()).isEqualTo(2);
        assertThat(evolved.changeType()).isEqualTo(ChangeType.ADDITIVE);
        assertThat(evolved.diff().addedColumns()).containsExactly("cbd_congestion_fee");
        assertThat(addedColumnsOf(2)).containsExactly("cbd_congestion_fee");
    }

    @Test
    @DisplayName("a widening change registers as widening")
    void wideningChangeIsAccepted() {
        registry.register(dataset, YELLOW_2024);

        StructType widened = of(
                f("vendor_id", type("long")),          // int -> long
                f("fare_amount", type("double")),
                f("passenger_count", type("long")));

        assertThat(registry.register(dataset, widened).changeType())
                .isEqualTo(ChangeType.WIDENING);
    }

    /** A refused change must leave the registry exactly as it was. */
    @Test
    @DisplayName("a narrowing change is refused and writes nothing")
    void narrowingIsRefusedAndNothingIsWritten() throws SQLException {
        registry.register(dataset, YELLOW_2024);

        StructType narrowed = of(
                f("vendor_id", type("int")),
                f("fare_amount", type("int")),          // double -> int
                f("passenger_count", type("long")));

        assertThatThrownBy(() -> registry.register(dataset, narrowed))
                .isInstanceOf(SchemaEvolutionException.class)
                .hasMessageContaining("breaking schema change refused");

        assertThat(versionCount()).isEqualTo(1);
    }

    @Test
    @DisplayName("dropping a column is refused")
    void droppedColumnIsRefused() throws SQLException {
        registry.register(dataset, YELLOW_2024);

        StructType dropped = of(
                f("vendor_id", type("int")),
                f("fare_amount", type("double")));

        assertThatThrownBy(() -> registry.register(dataset, dropped))
                .isInstanceOf(SchemaEvolutionException.class);
        assertThat(versionCount()).isEqualTo(1);
    }

    @Test
    @DisplayName("STRICT policy refuses even an additive change")
    void strictPolicyRefusesAdditiveChange() throws SQLException {
        SchemaRegistry strict = new SchemaRegistry(connections, Policy.STRICT);
        strict.register(dataset, YELLOW_2024);

        assertThatThrownBy(() -> strict.register(dataset, YELLOW_2025))
                .isInstanceOf(SchemaEvolutionException.class)
                .hasMessageContaining("STRICT");

        assertThat(versionCount()).isEqualTo(1);
    }

    @Test
    @DisplayName("latest reports the current version")
    void latestReportsCurrentVersion() {
        registry.register(dataset, YELLOW_2024);
        registry.register(dataset, YELLOW_2025);

        assertThat(registry.latest(dataset)).isPresent()
                .get().extracting(RegisteredSchema::version).isEqualTo(2);
        assertThat(registry.latest("it.schema.never-registered")).isEmpty();
    }

    /**
     * The advisory lock exists for this. Without it both threads read version 1 and both attempt
     * to insert version 2; one gets a unique-constraint violation that surfaces as an unrelated
     * database error rather than as the harmless no-op it should be.
     */
    @Test
    @DisplayName("concurrent registration of the same schema creates exactly one version")
    void concurrentRegistrationIsSerialized() throws Exception {
        registry.register(dataset, YELLOW_2024);

        int threads = 8;
        CountDownLatch startGate = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
            List<Future<RegisteredSchema>> futures = new java.util.ArrayList<>();
            for (int i = 0; i < threads; i++) {
                futures.add(pool.submit(() -> {
                    startGate.await();
                    return registry.register(dataset, YELLOW_2025);
                }));
            }
            startGate.countDown();

            long created = 0;
            for (Future<RegisteredSchema> future : futures) {
                if (future.get(30, TimeUnit.SECONDS).isNewVersion()) {
                    created++;
                }
            }

            assertThat(created).as("exactly one thread should have inserted the new version")
                    .isEqualTo(1);
            assertThat(versionCount()).isEqualTo(2);
        } finally {
            pool.shutdownNow();
        }
    }
}
