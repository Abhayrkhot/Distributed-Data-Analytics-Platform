package com.analyticsplatform.transform.gold;

import static org.apache.spark.sql.functions.col;

import com.analyticsplatform.common.config.PlatformConfig;
import java.util.Properties;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Mirrors small curated extracts into Postgres for BI consumption.
 *
 * <p>Only the tables that are genuinely small live here. ClickHouse holds the analytical volume;
 * duplicating it into Postgres would give two answers to the same question and eventually two
 * different answers.
 *
 * <p>Writes are idempotent by replacing the run's own rows rather than truncating: a truncate would
 * delete days this run never touched, which turns a partial reload into data loss.
 */
public final class ServingWriter {

    private static final Logger log = LoggerFactory.getLogger(ServingWriter.class);

    private final String jdbcUrl;
    private final Properties connectionProperties;

    public ServingWriter(PlatformConfig config) {
        this.jdbcUrl = config.postgresUrl();
        this.connectionProperties = new Properties();
        connectionProperties.setProperty("user", config.postgresUser());
        connectionProperties.setProperty("password", config.postgresPassword());
        connectionProperties.setProperty("driver", "org.postgresql.Driver");
        // Batched inserts; the serving tables are small but this keeps a full reload quick.
        connectionProperties.setProperty("reWriteBatchedInserts", "true");
    }

    /**
     * Writes the daily KPI extract.
     *
     * <p>Uses a staging table plus an upsert rather than {@code SaveMode.Overwrite}, because
     * Overwrite drops and recreates the table — losing its primary key, constraints, and any grants
     * on it. Recreating a table as a side effect of loading it is rarely what anyone wants.
     */
    public long writeDailyKpi(Dataset<Row> dailyKpi) {
        Dataset<Row> shaped = dailyKpi.select(
                col("pickup_date").alias("kpi_date"),
                col("source").alias("vendor_name"),
                col("trip_count"),
                col("total_revenue"),
                col("avg_fare"),
                col("avg_distance_mi"),
                col("avg_duration_min"),
                col("avg_tip_pct"))
                // The serving grain is (date, vendor_name); gold's is (date, source, vendor_id), so
                // collapse to the serving grain before writing or the primary key rejects the load.
                .groupBy("kpi_date", "vendor_name")
                .agg(
                        org.apache.spark.sql.functions.sum("trip_count").alias("trip_count"),
                        org.apache.spark.sql.functions.sum("total_revenue").alias("total_revenue"),
                        org.apache.spark.sql.functions.round(
                                org.apache.spark.sql.functions.avg("avg_fare"), 4).alias("avg_fare"),
                        org.apache.spark.sql.functions.round(
                                org.apache.spark.sql.functions.avg("avg_distance_mi"), 4)
                                .alias("avg_distance_mi"),
                        org.apache.spark.sql.functions.round(
                                org.apache.spark.sql.functions.avg("avg_duration_min"), 4)
                                .alias("avg_duration_min"),
                        org.apache.spark.sql.functions.round(
                                org.apache.spark.sql.functions.avg("avg_tip_pct"), 6)
                                .alias("avg_tip_pct"));

        long rows = shaped.count();
        upsert(shaped, "serving.daily_kpi", """
                INSERT INTO serving.daily_kpi
                    (kpi_date, vendor_name, trip_count, total_revenue, avg_fare,
                     avg_distance_mi, avg_duration_min, avg_tip_pct)
                SELECT kpi_date, vendor_name, trip_count, total_revenue, avg_fare,
                       avg_distance_mi, avg_duration_min, avg_tip_pct
                  FROM %s
                ON CONFLICT (kpi_date, vendor_name) DO UPDATE
                   SET trip_count       = EXCLUDED.trip_count,
                       total_revenue    = EXCLUDED.total_revenue,
                       avg_fare         = EXCLUDED.avg_fare,
                       avg_distance_mi  = EXCLUDED.avg_distance_mi,
                       avg_duration_min = EXCLUDED.avg_duration_min,
                       avg_tip_pct      = EXCLUDED.avg_tip_pct,
                       loaded_at        = now()
                """);
        log.info("wrote {} rows to serving.daily_kpi", rows);
        return rows;
    }

    /** Writes the zone dimension. */
    public long writeZoneDimension(Dataset<Row> zones) {
        Dataset<Row> shaped = zones.select(
                col("LocationID").alias("location_id"),
                col("Borough").alias("borough"),
                col("Zone").alias("zone_name"),
                col("service_zone"));

        long rows = shaped.count();
        upsert(shaped, "serving.dim_taxi_zone", """
                INSERT INTO serving.dim_taxi_zone (location_id, borough, zone_name, service_zone)
                SELECT location_id, borough, zone_name, service_zone FROM %s
                ON CONFLICT (location_id) DO UPDATE
                   SET borough      = EXCLUDED.borough,
                       zone_name    = EXCLUDED.zone_name,
                       service_zone = EXCLUDED.service_zone
                """);
        log.info("wrote {} rows to serving.dim_taxi_zone", rows);
        return rows;
    }

    /**
     * Loads into a temporary table then merges, so the target's constraints survive the load.
     *
     * <p>Spark's JDBC writer has no upsert mode, and its Overwrite drops the table. Staging is the
     * only path that both replaces data and leaves the schema intact.
     */
    private void upsert(Dataset<Row> data, String target, String mergeSql) {
        String staging = target.replace('.', '_') + "_staging_"
                + Long.toHexString(System.nanoTime());

        data.write().mode(SaveMode.Overwrite)
                .jdbc(jdbcUrl, staging, connectionProperties);

        try (java.sql.Connection connection =
                     java.sql.DriverManager.getConnection(jdbcUrl, connectionProperties);
             java.sql.Statement statement = connection.createStatement()) {
            statement.executeUpdate(String.format(mergeSql, staging));
        } catch (java.sql.SQLException e) {
            throw new IllegalStateException("failed to merge " + staging + " into " + target, e);
        } finally {
            dropQuietly(staging);
        }
    }

    /**
     * Drops the staging table, tolerating failure.
     *
     * <p>A leaked staging table is untidy; letting its cleanup failure mask a successful merge would
     * be worse, since the caller would retry a load that already happened.
     */
    private void dropQuietly(String table) {
        try (java.sql.Connection connection =
                     java.sql.DriverManager.getConnection(jdbcUrl, connectionProperties);
             java.sql.Statement statement = connection.createStatement()) {
            statement.executeUpdate("DROP TABLE IF EXISTS " + table);
        } catch (java.sql.SQLException e) {
            log.warn("could not drop staging table {}: {}", table, e.getMessage());
        }
    }
}
