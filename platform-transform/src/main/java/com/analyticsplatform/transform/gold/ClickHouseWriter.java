package com.analyticsplatform.transform.gold;

import com.analyticsplatform.common.config.PlatformConfig;
import java.util.Locale;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Writes gold aggregates into ClickHouse through the native Spark connector.
 *
 * <p>The connector rather than JDBC: it writes in ClickHouse's native format and parallelises across
 * executors, where a JDBC path funnels every row through batched INSERTs. On the trip fact table that
 * difference is the whole point of having a columnar store.
 *
 * <h2>Column alignment is explicit</h2>
 *
 * <p>Writes select the target table's columns by name before appending. ClickHouse's insert is
 * positional, so a DataFrame whose column order differs from the DDL would insert silently
 * mis-aligned values of compatible types — dropoff ids landing in pickup columns, with no error
 * anywhere. Selecting by name up front makes that impossible rather than unlikely.
 */
public final class ClickHouseWriter {

    private static final Logger log = LoggerFactory.getLogger(ClickHouseWriter.class);

    /** Spark catalog name the connector is registered under. */
    public static final String CATALOG = "clickhouse";

    private final SparkSession spark;
    private final String database;

    public ClickHouseWriter(SparkSession spark, PlatformConfig config) {
        this.spark = spark;
        this.database = config.clickhouseDatabase();
        configureCatalog(spark, config);
    }

    /**
     * Registers the ClickHouse catalog on the session.
     *
     * <p>Credentials come from {@link PlatformConfig}, which loads them from the gitignored
     * {@code .env} — never from a literal here, and never logged.
     */
    private static void configureCatalog(SparkSession spark, PlatformConfig config) {
        String prefix = "spark.sql.catalog." + CATALOG;
        spark.conf().set(prefix, "com.clickhouse.spark.ClickHouseCatalog");
        spark.conf().set(prefix + ".host", config.clickhouseHost());
        spark.conf().set(prefix + ".protocol", "http");
        spark.conf().set(prefix + ".http_port", String.valueOf(config.clickhouseHttpPort()));
        spark.conf().set(prefix + ".user", config.clickhouseUser());
        spark.conf().set(prefix + ".password", config.clickhousePassword());
        spark.conf().set(prefix + ".database", config.clickhouseDatabase());
        // ClickHouse rejects an INSERT whose column set does not match; letting Spark reorder to
        // the table's schema is what makes name-based alignment actually take effect.
        spark.conf().set("spark.clickhouse.write.format", "json");
    }

    /**
     * Appends a DataFrame to a ClickHouse table, aligning columns by name.
     *
     * @return the number of rows written
     */
    public long append(Dataset<Row> data, String table) {
        String qualified = CATALOG + "." + database + "." + table;
        Dataset<Row> aligned = alignToTarget(data, qualified);

        long rows = aligned.count();
        try {
            aligned.writeTo(qualified).append();
        } catch (org.apache.spark.sql.catalyst.analysis.NoSuchTableException e) {
            // A missing target table means the ClickHouse DDL was never applied, which is a
            // deployment problem rather than a data one. Say so, instead of surfacing a checked
            // exception that reads like a transient failure and invites a retry.
            throw new IllegalStateException(
                    "ClickHouse table " + qualified + " does not exist; apply "
                            + "docker/clickhouse/init/01_marts.sql", e);
        }
        log.info("wrote {} rows to {}", rows, qualified);
        return rows;
    }

    /**
     * Reorders and narrows the DataFrame to the target table's columns.
     *
     * <p>Fails loudly when the target expects a column the DataFrame does not have. The alternative
     * — inserting whatever happens to line up — is the failure mode this method exists to remove.
     */
    private Dataset<Row> alignToTarget(Dataset<Row> data, String qualifiedTable) {
        String[] targetColumns = spark.table(qualifiedTable).columns();
        java.util.List<String> available = java.util.Arrays.asList(data.columns());

        java.util.List<org.apache.spark.sql.Column> projection = new java.util.ArrayList<>();
        java.util.List<String> missing = new java.util.ArrayList<>();
        for (String column : targetColumns) {
            if (available.contains(column)) {
                projection.add(org.apache.spark.sql.functions.col(column));
            } else if (hasDefault(column)) {
                // Columns the DDL defaults (ingested_at, loaded_at) are intentionally left to
                // ClickHouse rather than stamped here.
                continue;
            } else {
                missing.add(column);
            }
        }

        if (!missing.isEmpty()) {
            throw new IllegalArgumentException(
                    "cannot write to " + qualifiedTable + ": DataFrame is missing "
                            + missing + "; it has " + available);
        }
        return data.select(projection.toArray(new org.apache.spark.sql.Column[0]));
    }

    /** Columns the ClickHouse DDL populates itself. */
    private static boolean hasDefault(String column) {
        String name = column.toLowerCase(Locale.ROOT);
        return name.equals("ingested_at") || name.equals("loaded_at") || name.equals("updated_at");
    }

    /** Row count of a ClickHouse table, for reconciliation checks. */
    public long countOf(String table) {
        return spark.table(CATALOG + "." + database + "." + table).count();
    }

    /**
     * Removes rows produced by a given run.
     *
     * <p>Gold rebuilds are idempotent by deleting the previous run's rows rather than truncating:
     * truncating would drop other partitions that this run never touched.
     */
    public void deleteRunRows(String table, long runId) {
        spark.sql("DELETE FROM " + CATALOG + "." + database + "." + table
                + " WHERE etl_run_id = " + runId);
    }
}
