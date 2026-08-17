package com.analyticsplatform.transform.gold;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.hour;
import static org.apache.spark.sql.functions.sum;
import static org.apache.spark.sql.functions.to_date;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.types.DataTypes;

/**
 * The four gold aggregates.
 *
 * <p>Each was hand-computed into {@code tests/golden/} before this class existed, and all four
 * independently reconcile to silver's 14 trips and 525.10 in revenue. That cross-check is the point:
 * an aggregate that quietly dropped a group would still look entirely plausible on its own.
 *
 * <h2>Averages ignore nulls, deliberately</h2>
 *
 * <p>Spark's {@code avg} skips nulls rather than treating them as zero, which is what makes the
 * silver null-versus-zero decision pay off here. The zero-fare trip has a null {@code tip_pct}; in a
 * group of three it contributes nothing and the average of the other two stands (0.2, not 0.133),
 * and in a group of one the average is null rather than a fabricated 0.0. Encoding that null as zero
 * upstream would have silently biased every average in this file.
 */
public final class GoldAggregates {

    private GoldAggregates() {
    }

    /** Rounds to 4 decimals so output is comparable across platforms and stable in golden files. */
    private static Column round4(Column value) {
        return org.apache.spark.sql.functions.round(value, 4);
    }

    private static Column money(Column value) {
        return org.apache.spark.sql.functions.round(value, 2)
                .cast(DataTypes.createDecimalType(18, 2));
    }

    /** Adds the date and hour columns every aggregate groups on. */
    private static Dataset<Row> withDateParts(Dataset<Row> silver) {
        return silver
                .withColumn("pickup_date", to_date(col("pickup_ts")))
                .withColumn("pickup_hour", hour(col("pickup_ts")));
    }

    /** Revenue and volume per pickup zone per hour. */
    public static Dataset<Row> zoneHourly(Dataset<Row> silver) {
        return withDateParts(silver)
                .groupBy("pickup_date", "pickup_hour", "pickup_location_id",
                        "pickup_borough", "pickup_zone")
                .agg(
                        count("*").alias("trip_count"),
                        money(sum("total_amount")).alias("total_revenue"),
                        round4(avg("fare_amount")).alias("avg_fare"),
                        round4(avg("trip_distance_mi")).alias("avg_distance_mi"),
                        round4(avg("trip_duration_min")).alias("avg_duration_min"),
                        round4(avg("tip_pct")).alias("avg_tip_pct"));
    }

    /** Borough-to-borough origin/destination matrix. */
    public static Dataset<Row> boroughOd(Dataset<Row> silver) {
        return withDateParts(silver)
                .groupBy("pickup_date", "pickup_borough", "dropoff_borough")
                .agg(
                        count("*").alias("trip_count"),
                        money(sum("total_amount")).alias("total_revenue"),
                        round4(avg("trip_distance_mi")).alias("avg_distance_mi"),
                        round4(avg("trip_duration_min")).alias("avg_duration_min"));
    }

    /**
     * Payment mix per day and source, including each payment type's share of revenue.
     *
     * <p>The share is computed with a window over {@code (pickup_date, source)} rather than a
     * self-join, so it needs one pass and the shares provably sum to 1.0 within each partition —
     * which the golden test asserts to six decimal places.
     */
    public static Dataset<Row> paymentDaily(Dataset<Row> silver) {
        Dataset<Row> grouped = withDateParts(silver)
                .groupBy("pickup_date", "source", "payment_type")
                .agg(
                        count("*").alias("trip_count"),
                        money(sum("total_amount")).alias("total_revenue"),
                        money(sum("tip_amount")).alias("total_tips"),
                        round4(avg("tip_pct")).alias("avg_tip_pct"));

        Column dailyTotal = sum(col("total_revenue"))
                .over(Window.partitionBy(col("pickup_date"), col("source")));

        return grouped.withColumn("revenue_share",
                org.apache.spark.sql.functions.round(
                        col("total_revenue").divide(dailyTotal), 6));
    }

    /** Headline KPIs per day, source and vendor. */
    public static Dataset<Row> dailyKpi(Dataset<Row> silver) {
        return withDateParts(silver)
                .groupBy("pickup_date", "source", "vendor_id")
                .agg(
                        count("*").alias("trip_count"),
                        money(sum("total_amount")).alias("total_revenue"),
                        round4(avg("fare_amount")).alias("avg_fare"),
                        round4(avg("trip_distance_mi")).alias("avg_distance_mi"),
                        round4(avg("trip_duration_min")).alias("avg_duration_min"),
                        round4(avg("avg_speed_mph")).alias("avg_speed_mph"),
                        round4(avg("tip_pct")).alias("avg_tip_pct"));
    }

    /**
     * The trip-grain fact table that serves ad-hoc queries and the Phase 8 benchmark.
     *
     * <p>Adds the partition column ClickHouse orders on and stamps the producing run, so any row in
     * the warehouse can be traced back to {@code control.etl_run}.
     */
    public static Dataset<Row> factTrip(Dataset<Row> silver, long runId) {
        return withDateParts(silver)
                .withColumn("etl_run_id", org.apache.spark.sql.functions.lit(runId))
                .drop("pickup_hour");
    }

    /** Names of the aggregate datasets, for lineage and governance registration. */
    public static java.util.List<String> aggregateDatasets() {
        return java.util.List.of(
                "gold.agg_zone_hourly",
                "gold.agg_borough_od",
                "gold.agg_payment_daily",
                "gold.agg_daily_kpi",
                "gold.fact_trip");
    }
}
