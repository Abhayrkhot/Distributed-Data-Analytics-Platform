package com.analyticsplatform.stream.job;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.sum;
import static org.apache.spark.sql.functions.window;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;

/**
 * Five-minute tumbling windows over trip events, grouped by pickup borough.
 *
 * <p>Separated from {@link StreamIngestJob} so the window semantics — boundaries, lateness, ordering
 * — can be tested against static data. A streaming test that has to start a query, feed a socket and
 * wait for a trigger is slow and flaky; those are the tests that get deleted.
 *
 * <h2>Determinism is a requirement, not a consequence</h2>
 *
 * <p>Nothing here may call {@code now()}, {@code current_timestamp()}, {@code rand()}, or generate a
 * UUID. Replay correctness depends on it: an identical batch id only implies identical output if the
 * computation is a pure function of its input. A processing-time value anywhere in this path would
 * make every replay produce different rows, and the {@code ReplacingMergeTree} version would then be
 * choosing between two <em>different</em> answers rather than deduplicating one.
 */
public final class WindowAggregator {

    /** Tumbling window width. Matches the ClickHouse partition granularity. */
    public static final String WINDOW_DURATION = "5 minutes";

    /**
     * How late an event may arrive and still be counted.
     *
     * <p>Ten minutes, i.e. two windows. Longer holds more state; shorter drops more real events.
     * The choice is arbitrary but it is <em>declared</em>, and the boundary is tested in both
     * directions so its effect is observable rather than assumed.
     */
    public static final String WATERMARK_DELAY = "10 minutes";

    private WindowAggregator() {
    }

    /**
     * Aggregates events into windows.
     *
     * @param withWatermark whether to apply an event-time watermark. Streaming needs it to bound
     *        state; a batch computation over a finite set must not have it, because a watermark
     *        would drop trailing events and the batch reference would then disagree with the stream
     *        for reasons that have nothing to do with correctness.
     */
    public static Dataset<Row> aggregate(Dataset<Row> events, boolean withWatermark) {
        Dataset<Row> source = withWatermark
                ? events.withWatermark("event_time", WATERMARK_DELAY)
                : events;

        return source
                .groupBy(window(col("event_time"), WINDOW_DURATION), col("pickup_borough"))
                .agg(
                        count("*").alias("trip_count"),
                        sum("total_amount").alias("total_revenue_raw"),
                        avg("fare_amount").alias("avg_fare_raw"),
                        avg("trip_distance_mi").alias("avg_distance_raw"))
                .select(
                        col("window.start").alias("window_start"),
                        col("window.end").alias("window_end"),
                        col("pickup_borough"),
                        col("trip_count").cast(DataTypes.LongType).alias("trip_count"),
                        // Rounded to the target column's scale here rather than relying on the
                        // ClickHouse cast: an implicit narrowing would round differently between
                        // the streaming path and the batch reference, making them disagree by cents.
                        org.apache.spark.sql.functions.round(col("total_revenue_raw"), 2)
                                .cast(DataTypes.createDecimalType(18, 2)).alias("total_revenue"),
                        org.apache.spark.sql.functions.round(col("avg_fare_raw"), 4)
                                .cast(DataTypes.createDecimalType(12, 4)).alias("avg_fare"),
                        org.apache.spark.sql.functions.round(col("avg_distance_raw"), 4)
                                .cast(DataTypes.FloatType).alias("avg_distance_mi"));
    }

    /** The batch reference implementation: same aggregation, no watermark. */
    public static Dataset<Row> aggregateBatch(Dataset<Row> events) {
        return aggregate(events, false);
    }
}
