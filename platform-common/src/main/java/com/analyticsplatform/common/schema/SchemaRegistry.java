package com.analyticsplatform.common.schema;

import com.analyticsplatform.common.dao.ConnectionSource;
import com.analyticsplatform.common.schema.SchemaCompatibility.ChangeType;
import com.analyticsplatform.common.schema.SchemaCompatibility.SchemaDiff;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;

/**
 * Versioned schema registry backed by {@code control.schema_version}.
 *
 * <p>Every ingest run registers the schema it actually read. The registry canonicalizes it, hashes
 * it, diffs it against the last registered version, and decides whether the run may proceed:
 *
 * <ul>
 *   <li>unchanged — returns the existing version, writes nothing
 *   <li>{@code ADDITIVE} / {@code WIDENING} — records a new version and proceeds
 *   <li>{@code BREAKING} — throws, so nothing is published
 * </ul>
 *
 * <p>The failure mode this exists to prevent is silent: an upstream drops a column or narrows a
 * type, the job keeps running, and the corruption is discovered weeks later in a dashboard.
 */
public final class SchemaRegistry {

    /** How much change a dataset tolerates. */
    public enum Policy {
        /** Any change after the initial version is rejected. */
        STRICT,
        /** Additive and widening changes are accepted; breaking changes are not. */
        ALLOW_WIDENING;

        public static Policy fromConfig(String value) {
            return switch (value.toLowerCase(Locale.ROOT)) {
                case "strict" -> STRICT;
                case "allow_widening" -> ALLOW_WIDENING;
                default -> throw new IllegalArgumentException("unknown schema policy: " + value);
            };
        }
    }

    /** The outcome of registering a schema. */
    public record RegisteredSchema(
            String datasetName,
            int version,
            String schemaHash,
            ChangeType changeType,
            SchemaDiff diff,
            boolean created) {

        /** True when this call inserted a new version rather than matching an existing one. */
        public boolean isNewVersion() {
            return created;
        }
    }

    /** Thrown when a schema transition is refused. Carries the diff so the message is actionable. */
    public static final class SchemaEvolutionException extends RuntimeException {
        private final transient SchemaDiff diff;

        SchemaEvolutionException(String message, SchemaDiff diff) {
            super(message);
            this.diff = diff;
        }

        public SchemaDiff diff() {
            return diff;
        }
    }

    private static final String SELECT_LATEST = """
            SELECT version, schema_json, schema_hash, change_type
              FROM control.schema_version
             WHERE dataset_name = ?
             ORDER BY version DESC
             LIMIT 1
            """;

    private static final String INSERT_VERSION = """
            INSERT INTO control.schema_version
                (dataset_name, version, schema_json, schema_hash, change_type,
                 added_columns, removed_columns, retyped_columns, change_note)
            VALUES (?, ?, ?::jsonb, ?, ?, ?, ?, ?, ?)
            """;

    private final ConnectionSource connections;
    private final Policy policy;

    public SchemaRegistry(ConnectionSource connections, Policy policy) {
        this.connections = connections;
        this.policy = policy;
    }

    /**
     * Registers the schema a run observed, returning the version it belongs to.
     *
     * @throws SchemaEvolutionException if the transition is refused. Thrown before any data is
     *         published, so a rejected change leaves the target untouched.
     */
    public RegisteredSchema register(String datasetName, StructType schema) {
        String incomingHash = CanonicalSchema.hash(schema);

        try (Connection connection = connections.open()) {
            connection.setAutoCommit(false);
            try {
                // Serializes concurrent registrations of the same dataset. Without it, two runs
                // can both read version N and both try to insert N+1; one gets a unique-violation
                // that looks like an unrelated database error. The lock is transaction-scoped, so
                // it releases on commit or rollback without any cleanup path.
                lockDataset(connection, datasetName);

                Optional<Existing> latest = selectLatest(connection, datasetName);

                if (latest.isPresent() && latest.get().schemaHash().equals(incomingHash)) {
                    connection.commit();
                    return new RegisteredSchema(
                            datasetName, latest.get().version(), incomingHash,
                            ChangeType.INITIAL,
                            new SchemaDiff(ChangeType.INITIAL, List.of(), List.of(), List.of(), List.of()),
                            false);
                }

                StructType previous = latest.map(e -> parse(e.schemaJson())).orElse(null);
                SchemaDiff diff = SchemaCompatibility.classify(previous, schema);

                rejectIfIncompatible(datasetName, diff);

                int nextVersion = latest.map(e -> e.version() + 1).orElse(1);
                insertVersion(connection, datasetName, nextVersion, schema, incomingHash, diff);
                connection.commit();

                return new RegisteredSchema(
                        datasetName, nextVersion, incomingHash, diff.changeType(), diff, true);
            } catch (RuntimeException | SQLException e) {
                connection.rollback();
                throw e;
            } finally {
                connection.setAutoCommit(true);
            }
        } catch (SQLException e) {
            throw new IllegalStateException(
                    "failed to register schema for " + datasetName, e);
        }
    }

    /** The latest registered version of a dataset, if any. */
    public Optional<RegisteredSchema> latest(String datasetName) {
        try (Connection connection = connections.open()) {
            return selectLatest(connection, datasetName).map(existing -> new RegisteredSchema(
                    datasetName, existing.version(), existing.schemaHash(),
                    ChangeType.valueOf(existing.changeType().toUpperCase(Locale.ROOT)),
                    new SchemaDiff(ChangeType.INITIAL, List.of(), List.of(), List.of(), List.of()),
                    false));
        } catch (SQLException e) {
            throw new IllegalStateException("failed to read schema for " + datasetName, e);
        }
    }

    private void rejectIfIncompatible(String datasetName, SchemaDiff diff) {
        if (diff.changeType() == ChangeType.BREAKING) {
            throw new SchemaEvolutionException(
                    "breaking schema change refused for " + datasetName + ":\n  - "
                            + String.join("\n  - ", diff.breakingReasons()), diff);
        }
        if (policy == Policy.STRICT && diff.changeType() != ChangeType.INITIAL) {
            throw new SchemaEvolutionException(
                    "schema policy is STRICT; " + datasetName + " changed ("
                            + diff.changeType() + "): added=" + diff.addedColumns()
                            + " retyped=" + diff.retypedColumns(), diff);
        }
    }

    private static void lockDataset(Connection connection, String datasetName) throws SQLException {
        try (PreparedStatement statement =
                     connection.prepareStatement("SELECT pg_advisory_xact_lock(hashtext(?))")) {
            statement.setString(1, datasetName);
            statement.execute();
        }
    }

    private static Optional<Existing> selectLatest(Connection connection, String datasetName)
            throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(SELECT_LATEST)) {
            statement.setString(1, datasetName);
            try (ResultSet rows = statement.executeQuery()) {
                if (!rows.next()) {
                    return Optional.empty();
                }
                return Optional.of(new Existing(
                        rows.getInt(1), rows.getString(2), rows.getString(3), rows.getString(4)));
            }
        }
    }

    private static void insertVersion(
            Connection connection, String datasetName, int version, StructType schema,
            String hash, SchemaDiff diff) throws SQLException {

        try (PreparedStatement statement = connection.prepareStatement(INSERT_VERSION)) {
            statement.setString(1, datasetName);
            statement.setInt(2, version);
            statement.setString(3, schema.json());
            statement.setString(4, hash);
            statement.setString(5, diff.changeType().name().toLowerCase(Locale.ROOT));
            statement.setArray(6, connection.createArrayOf("text", diff.addedColumns().toArray()));
            statement.setArray(7, connection.createArrayOf("text", diff.removedColumns().toArray()));
            statement.setArray(8, connection.createArrayOf("text", diff.retypedColumns().toArray()));
            statement.setString(9, describe(diff));
            statement.executeUpdate();
        }
    }

    private static String describe(SchemaDiff diff) {
        if (diff.changeType() == ChangeType.INITIAL) {
            return "initial schema";
        }
        return "added=" + diff.addedColumns()
                + " removed=" + diff.removedColumns()
                + " retyped=" + diff.retypedColumns();
    }

    private static StructType parse(String json) {
        return (StructType) DataType.fromJson(json);
    }

    private record Existing(int version, String schemaJson, String schemaHash, String changeType) {
    }
}
