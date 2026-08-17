package com.analyticsplatform.stream.job;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.date_format;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.when;

import com.analyticsplatform.common.config.PlatformConfig;
import com.analyticsplatform.stream.event.EventEnvelope;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Replays silver trips onto Kafka as JSON events.
 *
 * <p>Built from silver rather than from synthetic data so the stream carries realistic event-time
 * spread — trips crossing window boundaries, an unknown borough, a zero-fare trip. A generator
 * producing uniform events would exercise the happy path and nothing else.
 *
 * <h2>Event ids are derived, not random</h2>
 *
 * <p>{@code event_id} is {@code trip_key} plus the event time, so replaying the producer emits the
 * <em>same</em> ids. A {@code uuid()} here would make every replay look like new traffic, and the
 * whole point of the recovery test is comparing a replayed stream against a reference — which
 * requires the input to be reproducible, not just similar.
 */
public final class TripEventProducer {

    private static final Logger log = LoggerFactory.getLogger(TripEventProducer.class);

    private TripEventProducer() {
    }

    /**
     * Shapes silver rows into the Kafka envelope.
     *
     * <p>Serialization is a Spark expression rather than a call to {@link EventEnvelope#toJson()}
     * because the row never becomes a Java object — going through the record would mean collecting
     * to the driver. The two encodings are kept aligned by
     * {@code EventEnvelope.JSON_SCHEMA}, which the consumer parses with.
     */
    public static Dataset<Row> toEvents(Dataset<Row> silver) {
        Column eventId = concat(col("trip_key"), lit("-"),
                date_format(col("pickup_ts"), "yyyyMMddHHmmss"));

        return silver.select(
                col("trip_key").alias("key"),
                jsonObject(eventId).alias("value"));
    }

    /** Builds the JSON payload with Spark's own encoder, so escaping is Spark's problem. */
    private static Column jsonObject(Column eventId) {
        return org.apache.spark.sql.functions.to_json(
                org.apache.spark.sql.functions.struct(
                        eventId.alias("event_id"),
                        col("trip_key"),
                        col("pickup_ts").alias("event_time"),
                        col("pickup_ts").alias("producer_timestamp"),
                        lit(EventEnvelope.CURRENT_SCHEMA_VERSION).alias("schema_version"),
                        col("source"),
                        // A null borough would be rejected downstream; silver already coalesces
                        // unknown zones to "Unknown", and this keeps that guarantee explicit.
                        when(col("pickup_borough").isNull(), lit("Unknown"))
                                .otherwise(col("pickup_borough")).alias("pickup_borough"),
                        col("fare_amount"),
                        col("total_amount"),
                        col("trip_distance_mi")));
    }

    /** Publishes events to the topic. Returns the number sent. */
    public static long publish(Dataset<Row> silver, PlatformConfig config, String topic) {
        Dataset<Row> events = toEvents(silver);
        long count = events.count();

        events.write()
                .format("kafka")
                .option("kafka.bootstrap.servers", config.kafkaBootstrap())
                .option("topic", topic)
                .save();

        log.info("published {} events to {}", count, topic);
        return count;
    }
}
