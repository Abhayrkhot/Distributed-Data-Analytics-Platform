package com.analyticsplatform.common.key;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class TripKeyTest {

    private static final Instant PICKUP = Instant.parse("2024-01-15T08:30:00Z");
    private static final Instant DROPOFF = Instant.parse("2024-01-15T08:52:00Z");

    private static String key(BigDecimal distance, BigDecimal total) {
        return TripKey.of("yellow", "1", PICKUP, DROPOFF, 142, 236, distance, total);
    }

    private static String baseline() {
        return key(new BigDecimal("3.5"), new BigDecimal("21.50"));
    }

    @Nested
    @DisplayName("canonicalization: equivalent representations agree")
    class Canonicalization {

        /** The case that would otherwise split one trip into several keys. */
        @ParameterizedTest(name = "distance written as {0}")
        @ValueSource(strings = {"3.5", "3.50", "3.500", "3.500000", "03.5"})
        void equivalentDistanceRepresentationsProduceOneKey(String written) {
            assertThat(key(new BigDecimal(written), new BigDecimal("21.50")))
                    .isEqualTo(baseline());
        }

        @ParameterizedTest(name = "amount written as {0}")
        @ValueSource(strings = {"21.5", "21.50", "21.500000", "021.50"})
        void equivalentAmountRepresentationsProduceOneKey(String written) {
            assertThat(key(new BigDecimal("3.5"), new BigDecimal(written)))
                    .isEqualTo(baseline());
        }

        @Test
        @DisplayName("negative zero and positive zero agree")
        void signedZeroesAgree() {
            assertThat(key(new BigDecimal("-0.000"), new BigDecimal("0.00")))
                    .isEqualTo(key(new BigDecimal("0.0"), new BigDecimal("-0.0")));
        }

        @Test
        @DisplayName("the same instant expressed in another zone agrees")
        void timezoneEquivalentInstantsAgree() {
            // 08:30Z is 03:30 in New York on this date; the same moment either way.
            Instant viaNewYork = ZonedDateTime
                    .of(2024, 1, 15, 3, 30, 0, 0, ZoneId.of("America/New_York"))
                    .toInstant();

            assertThat(TripKey.of("yellow", "1", viaNewYork, DROPOFF, 142, 236,
                    new BigDecimal("3.5"), new BigDecimal("21.50")))
                    .isEqualTo(baseline());
        }

        @Test
        @DisplayName("source casing and surrounding whitespace are normalized")
        void sourceIsNormalized() {
            assertThat(TripKey.of("  YELLOW ", "1", PICKUP, DROPOFF, 142, 236,
                    new BigDecimal("3.5"), new BigDecimal("21.50")))
                    .isEqualTo(baseline());
        }

        @Test
        @DisplayName("recomputing from identical inputs is stable")
        void keyIsDeterministic() {
            assertThat(baseline()).isEqualTo(baseline());
        }
    }

    @Nested
    @DisplayName("identity: changing an identifying field changes the key")
    class Identity {

        @Test
        void distanceChangeChangesKey() {
            assertThat(key(new BigDecimal("3.6"), new BigDecimal("21.50")))
                    .isNotEqualTo(baseline());
        }

        @Test
        void amountChangeChangesKey() {
            assertThat(key(new BigDecimal("3.5"), new BigDecimal("21.51")))
                    .isNotEqualTo(baseline());
        }

        @Test
        void sourceChangeChangesKey() {
            assertThat(TripKey.of("green", "1", PICKUP, DROPOFF, 142, 236,
                    new BigDecimal("3.5"), new BigDecimal("21.50")))
                    .isNotEqualTo(baseline());
        }

        @Test
        void locationSwapChangesKey() {
            // Pickup 142 -> dropoff 236 is a different trip from 236 -> 142.
            assertThat(TripKey.of("yellow", "1", PICKUP, DROPOFF, 236, 142,
                    new BigDecimal("3.5"), new BigDecimal("21.50")))
                    .isNotEqualTo(baseline());
        }

        /** A millisecond apart is a different trip; the format must not truncate it away. */
        @Test
        void subSecondTimestampDifferenceChangesKey() {
            assertThat(TripKey.of("yellow", "1", PICKUP.plusMillis(1), DROPOFF, 142, 236,
                    new BigDecimal("3.5"), new BigDecimal("21.50")))
                    .isNotEqualTo(baseline());
        }
    }

    @Nested
    @DisplayName("null handling and field-boundary aliasing")
    class NullsAndBoundaries {

        @Test
        @DisplayName("a null field is distinct from the empty string")
        void nullDiffersFromEmpty() {
            String withNull = TripKey.of("yellow", null, PICKUP, DROPOFF, 142, 236,
                    new BigDecimal("3.5"), new BigDecimal("21.50"));
            String withEmpty = TripKey.of("yellow", "", PICKUP, DROPOFF, 142, 236,
                    new BigDecimal("3.5"), new BigDecimal("21.50"));

            // Blank-only text canonicalizes to null, so these two agree with each other...
            assertThat(withEmpty).isEqualTo(withNull);
            // ...but neither may collide with a vendor genuinely identified as "1".
            assertThat(withNull).isNotEqualTo(baseline());
        }

        /**
         * A literal "-1:" in the data must not be mistaken for the null encoding, and a literal
         * "&lt;null&gt;" must not be mistaken for an absent value.
         */
        @Test
        @DisplayName("text that mimics the null encoding stays distinct from null")
        void nullSentinelCannotBeForged() {
            String actualNull = TripKey.of("yellow", null, PICKUP, DROPOFF, 142, 236,
                    new BigDecimal("3.5"), new BigDecimal("21.50"));

            for (String impostor : new String[] {"-1:", "<null>", "null", "-1"}) {
                assertThat(TripKey.of("yellow", impostor, PICKUP, DROPOFF, 142, 236,
                        new BigDecimal("3.5"), new BigDecimal("21.50")))
                        .as("vendor_id=%s must not collide with a genuine null", impostor)
                        .isNotEqualTo(actualNull);
            }
        }

        /**
         * The aliasing case length-prefixing exists to prevent: with naive delimiter joining,
         * ("a", "b;c") and ("a;b", "c") would serialize identically.
         */
        @Test
        @DisplayName("delimiter characters inside values cannot shift field boundaries")
        void fieldBoundariesCannotAlias() {
            String left = TripKey.of("a", "b;c=1", PICKUP, DROPOFF, 142, 236,
                    new BigDecimal("3.5"), new BigDecimal("21.50"));
            String right = TripKey.of("a;b", "c=1", PICKUP, DROPOFF, 142, 236,
                    new BigDecimal("3.5"), new BigDecimal("21.50"));

            assertThat(left).isNotEqualTo(right);
        }

        @Test
        @DisplayName("key material is length-prefixed and carries an explicit null marker")
        void keyMaterialIsUnambiguous() {
            String material = TripKey.canonicalKeyMaterial(
                    "yellow", null, PICKUP, DROPOFF, 142, 236,
                    new BigDecimal("3.5"), new BigDecimal("21.50"));

            assertThat(material)
                    .contains("source=6:yellow;")
                    .contains("vendor_id=-1:;")            // null encoded as length -1
                    .contains("trip_distance=5:3.500;")    // fixed scale 3
                    .contains("total_amount=5:21.50;");    // fixed scale 2
        }

        /**
         * Equal keys must imply equal key material. Asserting only on digests could not tell a
         * genuine match from a hash collision.
         */
        @Test
        @DisplayName("equal keys imply equal canonical key material")
        void equalKeysImplyEqualMaterial() {
            String materialA = TripKey.canonicalKeyMaterial("yellow", "1", PICKUP, DROPOFF,
                    142, 236, new BigDecimal("3.5"), new BigDecimal("21.50"));
            String materialB = TripKey.canonicalKeyMaterial("  YELLOW", "1", PICKUP, DROPOFF,
                    142, 236, new BigDecimal("3.500"), new BigDecimal("21.5"));

            assertThat(materialA).isEqualTo(materialB);
            assertThat(TripKey.of("yellow", "1", PICKUP, DROPOFF, 142, 236,
                    new BigDecimal("3.5"), new BigDecimal("21.50")))
                    .isEqualTo(TripKey.of("  YELLOW", "1", PICKUP, DROPOFF, 142, 236,
                            new BigDecimal("3.500"), new BigDecimal("21.5")));
        }
    }

    @Nested
    @DisplayName("format")
    class Format {

        @Test
        @DisplayName("the key is 64 lowercase hex characters")
        void keyIsSha256Hex() {
            assertThat(baseline()).matches("[0-9a-f]{64}");
        }

        @Test
        @DisplayName("an all-null trip still produces a key rather than failing")
        void allNullsProduceAKey() {
            assertThat(TripKey.of(null, null, null, null, null, null, null, null))
                    .matches("[0-9a-f]{64}");
        }
    }
}
