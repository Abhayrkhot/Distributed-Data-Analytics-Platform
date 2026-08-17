package com.analyticsplatform.transform.silver;

import static org.apache.spark.sql.functions.broadcast;
import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.coalesce;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.row_number;
import static org.apache.spark.sql.functions.udf;
import static org.apache.spark.sql.functions.unix_timestamp;
import static org.apache.spark.sql.functions.when;

import com.analyticsplatform.common.key.TripKey;
import java.sql.Timestamp;
import java.time.Instant;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.types.DataTypes;

/**
 * Bronze to silver: reject, deduplicate, derive, enrich.
 *
 * <p>The rules here are the contract documented in {@code tests/fixtures/README.md}, and the golden
 * dataset was hand-computed against that contract before this class existed. If a change here
 * breaks a golden test, either the change is a bug or the contract genuinely moved — and that is a
 * decision for a human, not something to be settled by regenerating the expectations.
 *
 * <h2>Rejection is recorded, not silent</h2>
 *
 * <p>{@link #reject} returns the rows that failed and why, so a row that disappears between bronze
 * and silver always has an explanation attached. Filtering without recording is how datasets
 * quietly lose 3% of their volume for a year.
 */
public final class SilverTransform {

    /** Maximum plausible trip distance in miles; beyond this the meter is wrong, not the trip. */
    public static final double MAX_TRIP_DISTANCE_MI = 300.0;

    private static final String TRIP_KEY_UDF = "platform_trip_key";

    /** TLC payment type codes. */
    private static Column paymentTypeLabel(Column code) {
        return when(code.equalTo(1), "credit_card")
                .when(code.equalTo(2), "cash")
                .when(code.equalTo(3), "no_charge")
                .when(code.equalTo(4), "dispute")
                .when(code.equalTo(5), "unknown")
                .when(code.equalTo(6), "voided")
                .otherwise("unknown");
    }

    private SilverTransform() {
    }

    /**
     * Registers the trip-key UDF.
     *
     * <p>A UDF rather than an inline Spark expression so there is exactly <em>one</em>
     * implementation of the key, the one covered by {@code TripKeyTest} and its property tests. An
     * equivalent {@code sha2(concat_ws(...))} would be faster but would be a second implementation
     * of the canonicalization rules, free to drift from the first — and a dedup key that differs
     * between two implementations is worse than a slow one.
     */
    public static void registerUdfs(SparkSession spark) {
        spark.udf().register(TRIP_KEY_UDF, udf(
                (String source, Integer vendorId, Timestamp pickup, Timestamp dropoff,
                 Integer pickupLoc, Integer dropoffLoc, java.math.BigDecimal distance,
                 java.math.BigDecimal total) -> TripKey.of(
                        source,
                        vendorId == null ? null : String.valueOf(vendorId),
                        toInstant(pickup),
                        toInstant(dropoff),
                        pickupLoc,
                        dropoffLoc,
                        distance,
                        total),
                DataTypes.StringType));
    }

    private static Instant toInstant(Timestamp timestamp) {
        return timestamp == null ? null : timestamp.toInstant();
    }

    /** Why a bronze row did not reach silver. */
    public enum RejectReason {
        NULL_TIMESTAMP("null_timestamp"),
        DROPOFF_NOT_AFTER_PICKUP("dropoff_not_after_pickup"),
        NEGATIVE_FARE("negative_fare"),
        DISTANCE_OUT_OF_RANGE("distance_out_of_range");

        private final String code;

        RejectReason(String code) {
            this.code = code;
        }

        public String code() {
            return code;
        }
    }

    /**
     * The first rejection reason that applies to a row, or null if it is valid.
     *
     * <p>Evaluated in a fixed order so a row failing several rules reports one stable reason. A row
     * with both a null timestamp and a negative fare would otherwise be attributed differently
     * depending on evaluation order, making rejection counts non-reproducible.
     */
    public static Column rejectReason() {
        return when(col("pickup_ts").isNull().or(col("dropoff_ts").isNull()),
                        lit(RejectReason.NULL_TIMESTAMP.code()))
                .when(col("dropoff_ts").leq(col("pickup_ts")),
                        lit(RejectReason.DROPOFF_NOT_AFTER_PICKUP.code()))
                .when(col("fare_amount").lt(0).or(col("total_amount").lt(0)),
                        lit(RejectReason.NEGATIVE_FARE.code()))
                .when(col("trip_distance_mi").lt(0)
                                .or(col("trip_distance_mi").gt(MAX_TRIP_DISTANCE_MI)),
                        lit(RejectReason.DISTANCE_OUT_OF_RANGE.code()))
                .otherwise(lit(null).cast(DataTypes.StringType));
    }

    /** Rows that will not reach silver, each with its reason. */
    public static Dataset<Row> reject(Dataset<Row> bronze) {
        return bronze.withColumn("reject_reason", rejectReason())
                .filter(col("reject_reason").isNotNull())
                .select(col("source"), col("pickup_ts"), col("reject_reason"));
    }

    /**
     * The full transformation.
     *
     * @param zones the taxi-zone dimension; broadcast because it is ~265 rows and shuffling it
     *              would dwarf the join itself
     */
    public static Dataset<Row> transform(Dataset<Row> bronze, Dataset<Row> zones) {
        Dataset<Row> valid = bronze
                .withColumn("reject_reason", rejectReason())
                .filter(col("reject_reason").isNull())
                .drop("reject_reason");

        Dataset<Row> keyed = valid.withColumn("trip_key", callUDF(TRIP_KEY_UDF,
                col("source"), col("vendor_id"), col("pickup_ts"), col("dropoff_ts"),
                col("pickup_location_id"), col("dropoff_location_id"),
                col("trip_distance_mi").cast(DataTypes.createDecimalType(12, 4)),
                col("total_amount").cast(DataTypes.createDecimalType(12, 4))));

        Dataset<Row> deduplicated = deduplicate(keyed);
        Dataset<Row> derived = derive(deduplicated);
        return enrich(derived, zones);
    }

    /**
     * Keeps one row per {@code trip_key}.
     *
     * <p>Uses an explicit ordered window rather than {@code dropDuplicates}, which picks an
     * arbitrary survivor: with two rows sharing a key but differing elsewhere, the output would
     * depend on partitioning and would change between runs on identical input. The tiebreaker is
     * arbitrary but <em>fixed</em>, which is what matters for reproducibility.
     */
    static Dataset<Row> deduplicate(Dataset<Row> keyed) {
        return keyed
                .withColumn("__rn", row_number().over(Window
                        .partitionBy(col("trip_key"))
                        .orderBy(col("total_amount").desc_nulls_last(),
                                 col("tip_amount").desc_nulls_last(),
                                 col("payment_type_code").asc_nulls_last(),
                                 col("passenger_count").asc_nulls_last())))
                .filter(col("__rn").equalTo(1))
                .drop("__rn");
    }

    /** Adds duration, speed and tip percentage. */
    static Dataset<Row> derive(Dataset<Row> trips) {
        Column durationMin = unix_timestamp(col("dropoff_ts"))
                .minus(unix_timestamp(col("pickup_ts")))
                .cast(DataTypes.DoubleType).divide(lit(60.0));

        return trips
                .withColumn("trip_duration_min", durationMin)
                // Null rather than infinity for a zero-duration trip: dividing by zero would
                // produce Infinity, which then poisons every average it reaches.
                .withColumn("avg_speed_mph", when(col("trip_duration_min").gt(0),
                                col("trip_distance_mi").divide(col("trip_duration_min").divide(lit(60.0))))
                        .otherwise(lit(null).cast(DataTypes.DoubleType)))
                // Null rather than zero for a zero-fare trip. A zero-fare trip has no meaningful
                // tip percentage, and encoding it as 0.0 would drag down every average including
                // it - the silent corruption the null policy exists to prevent.
                .withColumn("tip_pct", when(col("fare_amount").notEqual(0),
                                col("tip_amount").divide(col("fare_amount")))
                        .otherwise(lit(null).cast(DataTypes.DoubleType)))
                .withColumn("payment_type", paymentTypeLabel(col("payment_type_code")));
    }

    /**
     * Joins the zone dimension for pickup and dropoff.
     *
     * <p>Left joins with an {@code Unknown} fallback: a trip whose zone id is not in the lookup is
     * still a real trip. Dropping it would lose revenue from the warehouse to avoid admitting a
     * gap in a reference table, which is the wrong trade.
     */
    static Dataset<Row> enrich(Dataset<Row> trips, Dataset<Row> zones) {
        Dataset<Row> pickupZones = zones.select(
                col("LocationID").alias("__pu_id"),
                col("Borough").alias("__pu_borough"),
                col("Zone").alias("__pu_zone"));
        Dataset<Row> dropoffZones = zones.select(
                col("LocationID").alias("__do_id"),
                col("Borough").alias("__do_borough"),
                col("Zone").alias("__do_zone"));

        return trips
                .join(broadcast(pickupZones),
                        col("pickup_location_id").equalTo(col("__pu_id")), "left")
                .join(broadcast(dropoffZones),
                        col("dropoff_location_id").equalTo(col("__do_id")), "left")
                .withColumn("pickup_borough", coalesce(col("__pu_borough"), lit("Unknown")))
                .withColumn("pickup_zone", coalesce(col("__pu_zone"), lit("Unknown")))
                .withColumn("dropoff_borough", coalesce(col("__do_borough"), lit("Unknown")))
                .withColumn("dropoff_zone", coalesce(col("__do_zone"), lit("Unknown")))
                .drop("__pu_id", "__pu_borough", "__pu_zone",
                      "__do_id", "__do_borough", "__do_zone")
                .select(
                        col("trip_key"),
                        col("source"),
                        col("vendor_id"),
                        col("pickup_ts"),
                        col("dropoff_ts"),
                        col("passenger_count"),
                        col("trip_distance_mi"),
                        col("trip_duration_min"),
                        col("avg_speed_mph"),
                        col("pickup_location_id"),
                        col("dropoff_location_id"),
                        col("pickup_borough"),
                        col("pickup_zone"),
                        col("dropoff_borough"),
                        col("dropoff_zone"),
                        col("payment_type"),
                        col("fare_amount"),
                        col("tip_amount"),
                        col("total_amount"),
                        col("tip_pct"),
                        col("cbd_congestion_fee"));
    }
}
