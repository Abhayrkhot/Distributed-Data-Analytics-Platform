package com.analyticsplatform.stream.event;

import java.time.Instant;
import java.util.Locale;
import java.util.Optional;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * The envelope every trip event carries on Kafka.
 *
 * <p>Stable identifiers are not decoration. Without {@code event_id} and {@code trip_key} a
 * duplicate is indistinguishable from a genuinely repeated trip, replay cannot be verified, and
 * debugging a bad aggregate means guessing which message produced it.
 *
 * <p>{@code schema_version} is present so a consumer can reject a shape it does not understand
 * rather than silently reading garbage out of positionally-similar fields. That is the streaming
 * analogue of the column-alignment guard on the ClickHouse writer.
 */
public record EventEnvelope(
        String eventId,
        String tripKey,
        Instant eventTime,
        Instant producerTimestamp,
        int schemaVersion,
        String source,
        String pickupBorough,
        double fareAmount,
        double totalAmount,
        double tripDistanceMi) {

    /** The only version this consumer understands. */
    public static final int CURRENT_SCHEMA_VERSION = 1;

    /** Sources the platform ingests. Anything else is rejected rather than aggregated. */
    private static final java.util.Set<String> KNOWN_SOURCES = java.util.Set.of("yellow", "green");

    /**
     * The Kafka JSON shape.
     *
     * <p>All fields nullable so a malformed message parses into a row with nulls and can be
     * classified by {@link EventValidator}, rather than failing the whole microbatch. One bad
     * message must not stop a stream.
     */
    public static final StructType JSON_SCHEMA = new StructType(new StructField[] {
        field("event_id", DataTypes.StringType),
        field("trip_key", DataTypes.StringType),
        field("event_time", DataTypes.TimestampType),
        field("producer_timestamp", DataTypes.TimestampType),
        field("schema_version", DataTypes.IntegerType),
        field("source", DataTypes.StringType),
        field("pickup_borough", DataTypes.StringType),
        field("fare_amount", DataTypes.DoubleType),
        field("total_amount", DataTypes.DoubleType),
        field("trip_distance_mi", DataTypes.DoubleType),
    });

    private static StructField field(String name, org.apache.spark.sql.types.DataType type) {
        return new StructField(name, type, true, Metadata.empty());
    }

    public EventEnvelope {
        if (eventId == null || eventId.isBlank()) {
            throw new IllegalArgumentException("eventId is required");
        }
        if (tripKey == null || tripKey.isBlank()) {
            throw new IllegalArgumentException("tripKey is required");
        }
        if (eventTime == null) {
            throw new IllegalArgumentException("eventTime is required");
        }
        source = source == null ? null : source.toLowerCase(Locale.ROOT);
    }

    /** Whether a source name is one the platform recognizes. */
    public static boolean isKnownSource(String source) {
        return source != null && KNOWN_SOURCES.contains(source.toLowerCase(Locale.ROOT));
    }

    /**
     * Serializes to JSON.
     *
     * <p>Hand-rolled for the same reason as elsewhere in this project: Spark ships its own Jackson,
     * and a version clash surfaces as {@code NoSuchMethodError} on the cluster. The escaping is
     * tested directly.
     */
    public String toJson() {
        return "{"
                + "\"event_id\":" + quote(eventId)
                + ",\"trip_key\":" + quote(tripKey)
                + ",\"event_time\":" + quote(eventTime.toString())
                + ",\"producer_timestamp\":" + quote(
                        producerTimestamp == null ? null : producerTimestamp.toString())
                + ",\"schema_version\":" + schemaVersion
                + ",\"source\":" + quote(source)
                + ",\"pickup_borough\":" + quote(pickupBorough)
                + ",\"fare_amount\":" + number(fareAmount)
                + ",\"total_amount\":" + number(totalAmount)
                + ",\"trip_distance_mi\":" + number(tripDistanceMi)
                + "}";
    }

    private static String quote(String value) {
        if (value == null) {
            return "null";
        }
        StringBuilder out = new StringBuilder(value.length() + 2).append('"');
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            switch (c) {
                case '"' -> out.append("\\\"");
                case '\\' -> out.append("\\\\");
                case '\n' -> out.append("\\n");
                case '\r' -> out.append("\\r");
                case '\t' -> out.append("\\t");
                default -> {
                    if (c < 0x20) {
                        out.append(String.format("\\u%04x", (int) c));
                    } else {
                        out.append(c);
                    }
                }
            }
        }
        return out.append('"').toString();
    }

    /**
     * Renders a double as JSON.
     *
     * <p>NaN and Infinity are not valid JSON. Emitting them produces a message no parser accepts,
     * so they become null — which the validator then rejects explicitly rather than letting an
     * unparseable message poison a batch.
     */
    private static String number(double value) {
        return Double.isFinite(value) ? Double.toString(value) : "null";
    }

    /** Key events by trip so all events for one trip land on the same Kafka partition. */
    public String kafkaKey() {
        return tripKey;
    }

    public Optional<Instant> producer() {
        return Optional.ofNullable(producerTimestamp);
    }
}
