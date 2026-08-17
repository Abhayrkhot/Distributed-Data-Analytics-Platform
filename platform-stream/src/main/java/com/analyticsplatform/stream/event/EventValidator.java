package com.analyticsplatform.stream.event;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.when;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;

/**
 * Classifies every incoming event as usable or rejected, with a reason.
 *
 * <p>Every malformed class gets an <strong>explicit policy</strong> (§38). The failure being avoided
 * is a silent one: a stream that drops unparseable messages produces aggregates that are quietly
 * wrong, and nothing anywhere says how many events went missing. Rejections are counted and
 * attributed so a drop in volume has an explanation attached.
 *
 * <p>Nothing here throws. One bad message must not stop a stream — that would let a single
 * malformed producer take down the pipeline, which is a worse failure than dropping the message.
 */
public final class EventValidator {

    /** Why an event cannot be aggregated. */
    public enum RejectReason {
        /** The Kafka value was not parseable JSON at all. */
        UNPARSEABLE("unparseable"),
        MISSING_EVENT_ID("missing_event_id"),
        MISSING_TRIP_KEY("missing_trip_key"),
        MISSING_EVENT_TIME("missing_event_time"),
        UNKNOWN_SCHEMA_VERSION("unknown_schema_version"),
        UNKNOWN_SOURCE("unknown_source"),
        MISSING_AMOUNT("missing_amount");

        private final String code;

        RejectReason(String code) {
            this.code = code;
        }

        public String code() {
            return code;
        }
    }

    private EventValidator() {
    }

    /**
     * The first reason an event is unusable, or null when it is fine.
     *
     * <p>Evaluated in a fixed order so an event failing several checks reports one stable reason.
     * Without a fixed order, rejection counts would shift between runs on identical input and could
     * not be compared.
     */
    public static Column rejectReason() {
        return when(col("event_id").isNull().and(col("trip_key").isNull())
                        .and(col("event_time").isNull()),
                        lit(RejectReason.UNPARSEABLE.code()))
                .when(col("event_id").isNull().or(col("event_id").equalTo("")),
                        lit(RejectReason.MISSING_EVENT_ID.code()))
                .when(col("trip_key").isNull().or(col("trip_key").equalTo("")),
                        lit(RejectReason.MISSING_TRIP_KEY.code()))
                .when(col("event_time").isNull(),
                        lit(RejectReason.MISSING_EVENT_TIME.code()))
                // An unrecognized version means the producer is ahead of this consumer. Rejecting
                // is the safe read: the alternative is interpreting unknown fields positionally.
                .when(col("schema_version").isNull()
                                .or(col("schema_version").notEqual(
                                        EventEnvelope.CURRENT_SCHEMA_VERSION)),
                        lit(RejectReason.UNKNOWN_SCHEMA_VERSION.code()))
                .when(col("source").isNull()
                                .or(col("source").isin("yellow", "green").unary_$bang()),
                        lit(RejectReason.UNKNOWN_SOURCE.code()))
                // A null amount would make revenue silently understate rather than fail.
                .when(col("total_amount").isNull().or(col("fare_amount").isNull()),
                        lit(RejectReason.MISSING_AMOUNT.code()))
                .otherwise(lit(null).cast(DataTypes.StringType));
    }

    /** Adds a {@code reject_reason} column. */
    public static Dataset<Row> classify(Dataset<Row> events) {
        return events.withColumn("reject_reason", rejectReason());
    }

    /** Events safe to aggregate. */
    public static Dataset<Row> valid(Dataset<Row> classified) {
        return classified.filter(col("reject_reason").isNull()).drop("reject_reason");
    }

    /** Events that were dropped, with their reason, for counting. */
    public static Dataset<Row> rejected(Dataset<Row> classified) {
        return classified.filter(col("reject_reason").isNotNull());
    }
}
