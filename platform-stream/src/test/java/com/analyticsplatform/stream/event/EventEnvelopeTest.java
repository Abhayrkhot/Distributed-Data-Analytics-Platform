package com.analyticsplatform.stream.event;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * The event envelope and its JSON encoding (§41 round-trips).
 *
 * <p>Serialization is hand-rolled to avoid a Jackson clash with Spark's own copy, which means the
 * escaping is this project's responsibility rather than a library's — so it gets tested directly.
 */
class EventEnvelopeTest {

    private static final Instant EVENT_TIME = Instant.parse("2024-01-15T08:30:00Z");
    private static final Instant PRODUCED = Instant.parse("2024-01-15T08:30:05Z");

    private static EventEnvelope envelope(String eventId, String tripKey) {
        return new EventEnvelope(eventId, tripKey, EVENT_TIME, PRODUCED,
                EventEnvelope.CURRENT_SCHEMA_VERSION, "yellow", "Manhattan",
                21.50, 28.30, 3.5);
    }

    @Nested
    @DisplayName("required identifiers")
    class RequiredFields {

        /** Without these a duplicate is indistinguishable from a repeated trip. */
        @ParameterizedTest(name = "a blank {0} is refused")
        @CsvSource({"eventId", "tripKey"})
        void identifiersAreRequired(String which) {
            String eventId = "eventId".equals(which) ? "  " : "e1";
            String tripKey = "tripKey".equals(which) ? "  " : "t1";

            assertThatThrownBy(() -> envelope(eventId, tripKey))
                    .isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        @DisplayName("a null event time is refused")
        void eventTimeIsRequired() {
            assertThatThrownBy(() -> new EventEnvelope("e1", "t1", null, PRODUCED, 1,
                    "yellow", "Manhattan", 1.0, 1.0, 1.0))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("eventTime");
        }

        /** A missing producer timestamp is tolerated — it is provenance, not identity. */
        @Test
        @DisplayName("a null producer timestamp is allowed")
        void producerTimestampIsOptional() {
            EventEnvelope event = new EventEnvelope("e1", "t1", EVENT_TIME, null, 1,
                    "yellow", "Manhattan", 1.0, 1.0, 1.0);

            assertThat(event.producer()).isEmpty();
            assertThat(event.toJson()).contains("\"producer_timestamp\":null");
        }
    }

    @Nested
    @DisplayName("JSON encoding")
    class Encoding {

        @Test
        @DisplayName("emits every envelope field")
        void emitsEveryField() {
            String json = envelope("e1", "t1").toJson();

            assertThat(json)
                    .contains("\"event_id\":\"e1\"")
                    .contains("\"trip_key\":\"t1\"")
                    .contains("\"schema_version\":1")
                    .contains("\"source\":\"yellow\"")
                    .contains("\"total_amount\":28.3");
        }

        /** A quote in a field must not terminate the JSON string. */
        @Test
        @DisplayName("quotes and backslashes are escaped")
        void quotesAreEscaped() {
            String json = envelope("e\"1", "t\\1").toJson();

            assertThat(json).contains("\"event_id\":\"e\\\"1\"");
            assertThat(json).contains("\"trip_key\":\"t\\\\1\"");
        }

        @ParameterizedTest(name = "a control character is escaped")
        @ValueSource(strings = {"\n", "\r", "\t"})
        void controlCharactersAreEscaped(String control) {
            String json = new EventEnvelope("e" + control + "x", "t1", EVENT_TIME, PRODUCED, 1,
                    "yellow", "Manhattan", 1.0, 1.0, 1.0).toJson();

            assertThat(json).doesNotContain(control);
            assertThat(json).contains("\\");
        }

        @Test
        @DisplayName("an unprintable character becomes a unicode escape")
        void unprintableBecomesUnicodeEscape() {
            String json = new EventEnvelope("e", "t1", EVENT_TIME, PRODUCED, 1,
                    "yellow", "Manhattan", 1.0, 1.0, 1.0).toJson();

            assertThat(json).contains("\\u0001");
        }

        /**
         * NaN and Infinity are not valid JSON. Emitting them literally would produce a message no
         * parser accepts, so they become null and the validator rejects the event explicitly rather
         * than an unparseable message poisoning a batch.
         */
        @ParameterizedTest(name = "a non-finite number is emitted as null")
        @ValueSource(doubles = {Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY})
        void nonFiniteNumbersBecomeNull(double value) {
            String json = new EventEnvelope("e1", "t1", EVENT_TIME, PRODUCED, 1,
                    "yellow", "Manhattan", value, value, value).toJson();

            assertThat(json)
                    .doesNotContain("NaN")
                    .doesNotContain("Infinity")
                    .contains("\"total_amount\":null");
        }

        @Test
        @DisplayName("a null borough is emitted as null")
        void nullBoroughIsNull() {
            String json = new EventEnvelope("e1", "t1", EVENT_TIME, PRODUCED, 1,
                    "yellow", null, 1.0, 1.0, 1.0).toJson();

            assertThat(json).contains("\"pickup_borough\":null");
        }
    }

    @Nested
    @DisplayName("normalization and keying")
    class Normalization {

        @Test
        @DisplayName("source is lowercased")
        void sourceIsLowercased() {
            EventEnvelope event = new EventEnvelope("e1", "t1", EVENT_TIME, PRODUCED, 1,
                    "YELLOW", "Manhattan", 1.0, 1.0, 1.0);

            assertThat(event.source()).isEqualTo("yellow");
        }

        /** Keying by trip puts every event for one trip on the same partition. */
        @Test
        @DisplayName("the Kafka key is the trip key")
        void kafkaKeyIsTripKey() {
            assertThat(envelope("e1", "t1").kafkaKey()).isEqualTo("t1");
        }

        @ParameterizedTest(name = "{0} recognized = {1}")
        @CsvSource({"yellow, true", "green, true", "YELLOW, true", "blue, false"})
        void knownSources(String source, boolean expected) {
            assertThat(EventEnvelope.isKnownSource(source)).isEqualTo(expected);
        }

        @Test
        @DisplayName("null and blank sources are not recognized")
        void nullSourceIsNotKnown() {
            assertThat(EventEnvelope.isKnownSource(null)).isFalse();
            assertThat(EventEnvelope.isKnownSource("")).isFalse();
        }
    }

    @Test
    @DisplayName("the JSON schema declares every field nullable")
    void schemaIsFullyNullable() {
        // A malformed message must parse into a row of nulls and be classified, not blow up the
        // microbatch. That requires every field to tolerate absence.
        assertThat(EventEnvelope.JSON_SCHEMA.fields()).hasSize(10);
        assertThat(EventEnvelope.JSON_SCHEMA.fields()).allMatch(
                org.apache.spark.sql.types.StructField::nullable);
    }
}
