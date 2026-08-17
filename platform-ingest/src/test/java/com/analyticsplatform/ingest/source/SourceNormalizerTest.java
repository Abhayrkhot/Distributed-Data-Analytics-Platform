package com.analyticsplatform.ingest.source;

import static org.apache.spark.sql.functions.col;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.analyticsplatform.common.schema.CanonicalSchema;
import com.analyticsplatform.common.testing.Fixtures;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * Component tests for normalization, run against the real fixtures on a local Spark session.
 *
 * <p>Assertions are on values, not row counts. A normalizer that produced the right number of rows
 * with every timestamp null would pass a count-only test.
 */
class SourceNormalizerTest {

    @Nested
    @DisplayName("schema conformance")
    class Conformance {

        /**
         * The point of the whole class: three structurally different inputs must produce one
         * schema, byte-identical by hash. If they did not, the union would need schema merging and
         * the schema registry would see a change on every source switch.
         */
        @Test
        @DisplayName("all three sources produce an identically-hashing schema")
        void allSourcesConformToOneSchema() {
            String yellow2024 = CanonicalSchema.hash(
                    SourceNormalizer.normalizeYellow(Fixtures.yellow2024()).schema());
            String yellow2025 = CanonicalSchema.hash(
                    SourceNormalizer.normalizeYellow(Fixtures.yellow2025()).schema());
            String green = CanonicalSchema.hash(
                    SourceNormalizer.normalizeGreen(Fixtures.green2024()).schema());

            assertThat(yellow2025).isEqualTo(yellow2024);
            assertThat(green).isEqualTo(yellow2024);
        }

        @Test
        @DisplayName("column order is canonical regardless of input order")
        void columnOrderIsCanonical() {
            List<String> fromYellow = List.of(
                    SourceNormalizer.normalizeYellow(Fixtures.yellow2024()).columns());
            List<String> fromGreen = List.of(
                    SourceNormalizer.normalizeGreen(Fixtures.green2024()).columns());

            assertThat(fromYellow).isEqualTo(SourceNormalizer.bronzeColumns());
            assertThat(fromGreen).isEqualTo(SourceNormalizer.bronzeColumns());
        }

        /** The dispatch path, which jobs use rather than calling the typed methods directly. */
        @Test
        @DisplayName("dispatch by source name matches the typed methods")
        void dispatchMatchesTypedMethods() {
            assertThat(CanonicalSchema.hash(
                    SourceNormalizer.normalize(Fixtures.yellow2024(), "yellow").schema()))
                    .isEqualTo(CanonicalSchema.hash(
                            SourceNormalizer.normalizeYellow(Fixtures.yellow2024()).schema()));
            assertThat(SourceNormalizer.normalize(Fixtures.green2024(), "GREEN").count())
                    .isEqualTo(SourceNormalizer.normalizeGreen(Fixtures.green2024()).count());
        }

        @Test
        @DisplayName("an unknown source is refused")
        void unknownSourceIsRefused() {
            assertThatThrownBy(() -> SourceNormalizer.normalize(Fixtures.yellow2024(), "blue"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("unknown source");
        }
    }

    @Nested
    @DisplayName("timestamp mapping")
    class Timestamps {

        /** tpep_* versus lpep_* is the divergence most likely to be silently mismapped. */
        @Test
        @DisplayName("yellow tpep and green lpep both land in pickup_ts")
        void bothTimestampConventionsMap() {
            Row yellow = SourceNormalizer.normalizeYellow(Fixtures.yellow2024())
                    .orderBy("pickup_ts").first();
            Row green = SourceNormalizer.normalizeGreen(Fixtures.green2024())
                    .orderBy("pickup_ts").first();

            // Compare instants, not Timestamp.toString(): that renders in the JVM's default zone,
            // so the same correct value reads as 08:30 in UTC and 03:30 in New York. Asserting on
            // the rendering would make this test pass or fail depending on the developer's laptop.
            assertThat(yellow.<java.sql.Timestamp>getAs("pickup_ts").toInstant())
                    .isEqualTo(java.time.Instant.parse("2024-01-15T08:30:00Z"));
            assertThat(green.<java.sql.Timestamp>getAs("pickup_ts").toInstant())
                    .isEqualTo(java.time.Instant.parse("2024-01-15T14:00:00Z"));
        }

        /** The null dropoff must survive normalization, so silver can reject it for the right reason. */
        @Test
        @DisplayName("a null dropoff timestamp is preserved, not defaulted")
        void nullDropoffSurvives() {
            Dataset<Row> bronze = SourceNormalizer.normalizeYellow(Fixtures.yellow2024());

            assertThat(bronze.filter(col("dropoff_ts").isNull()).count()).isEqualTo(1);
        }
    }

    @Nested
    @DisplayName("source-specific columns")
    class SourceSpecific {

        /** Green has no airport fee and no CBD fee; they must be typed nulls, not absent. */
        @Test
        @DisplayName("green gets typed nulls for yellow-only columns")
        void greenHasNullsForYellowColumns() {
            Dataset<Row> green = SourceNormalizer.normalizeGreen(Fixtures.green2024());

            assertThat(green.filter(col("airport_fee").isNotNull()).count()).isZero();
            assertThat(green.filter(col("cbd_congestion_fee").isNotNull()).count()).isZero();
            assertThat(List.of(green.columns())).contains("airport_fee", "cbd_congestion_fee");
        }

        @Test
        @DisplayName("yellow gets typed nulls for green-only columns")
        void yellowHasNullsForGreenColumns() {
            Dataset<Row> yellow = SourceNormalizer.normalizeYellow(Fixtures.yellow2024());

            assertThat(yellow.filter(col("ehail_fee").isNotNull()).count()).isZero();
            assertThat(yellow.filter(col("trip_type").isNotNull()).count()).isZero();
        }

        @Test
        @DisplayName("green trip_type is carried through")
        void greenTripTypeIsPreserved() {
            Dataset<Row> green = SourceNormalizer.normalizeGreen(Fixtures.green2024());

            assertThat(green.filter(col("trip_type").isNotNull()).count()).isEqualTo(4);
            assertThat(green.filter(col("trip_type").equalTo(2)).count()).isEqualTo(1);
        }
    }

    @Nested
    @DisplayName("schema evolution across years")
    class Evolution {

        /**
         * 2024 lacks the column entirely, so it normalizes to null; 2025 has real values. Both
         * still produce the same schema, which is what lets one job read both.
         */
        @Test
        @DisplayName("2024 yields nulls for cbd_congestion_fee, 2025 yields values")
        void cbdFeeAppearsOnlyIn2025() {
            Dataset<Row> from2024 = SourceNormalizer.normalizeYellow(Fixtures.yellow2024());
            Dataset<Row> from2025 = SourceNormalizer.normalizeYellow(Fixtures.yellow2025());

            assertThat(from2024.filter(col("cbd_congestion_fee").isNotNull()).count()).isZero();
            assertThat(from2025.filter(col("cbd_congestion_fee").isNotNull()).count()).isEqualTo(3);
            assertThat(from2025.filter(col("cbd_congestion_fee").equalTo(0.75)).count()).isEqualTo(2);
        }

        /** Both years must union without schema merging. */
        @Test
        @DisplayName("2024 and 2025 union cleanly")
        void yearsUnionWithoutMerging() {
            Dataset<Row> union = SourceNormalizer.normalizeYellow(Fixtures.yellow2024())
                    .union(SourceNormalizer.normalizeYellow(Fixtures.yellow2025()));

            assertThat(union.count()).isEqualTo(15);
        }
    }

    @Nested
    @DisplayName("bronze rejects nothing")
    class NoFiltering {

        /**
         * Bronze is a faithful record of what arrived. Filtering here would let a row vanish
         * between the file and the warehouse with nothing recording why.
         */
        @Test
        @DisplayName("invalid rows survive into bronze")
        void invalidRowsAreRetained() {
            Dataset<Row> bronze = SourceNormalizer.normalizeYellow(Fixtures.yellow2024())
                    .union(SourceNormalizer.normalizeYellow(Fixtures.yellow2025()))
                    .union(SourceNormalizer.normalizeGreen(Fixtures.green2024()));

            assertThat(bronze.count()).as("all 19 rows").isEqualTo(19);
            assertThat(bronze.filter(col("fare_amount").lt(0)).count())
                    .as("negative fare retained").isEqualTo(1);
            assertThat(bronze.filter(col("trip_distance_mi").gt(300)).count())
                    .as("500-mile trip retained").isEqualTo(1);
            assertThat(bronze.filter(col("dropoff_ts").lt(col("pickup_ts"))).count())
                    .as("inverted timestamps retained").isEqualTo(1);
        }

        @Test
        @DisplayName("the duplicate row survives into bronze")
        void duplicateIsRetained() {
            Dataset<Row> bronze = SourceNormalizer.normalizeYellow(Fixtures.yellow2024());

            assertThat(bronze.count() - bronze.distinct().count()).isEqualTo(1);
        }
    }

    @Nested
    @DisplayName("values, not just shapes")
    class Values {

        @Test
        @DisplayName("a known row normalizes to exactly the expected values")
        void knownRowIsExact() {
            Row row = SourceNormalizer.normalizeYellow(Fixtures.yellow2024())
                    .filter(col("pickup_ts").equalTo("2024-01-15 08:30:00"))
                    .orderBy("total_amount")
                    .first();

            assertThat(row.<String>getAs("source")).isEqualTo("yellow");
            assertThat(row.<Integer>getAs("vendor_id")).isEqualTo(1);
            assertThat(row.<Long>getAs("passenger_count")).isEqualTo(1L);
            assertThat(row.<Double>getAs("trip_distance_mi")).isEqualTo(3.5);
            assertThat(row.<Integer>getAs("pickup_location_id")).isEqualTo(142);
            assertThat(row.<Integer>getAs("dropoff_location_id")).isEqualTo(236);
            assertThat(row.<Long>getAs("payment_type_code")).isEqualTo(1L);
            assertThat(row.<Double>getAs("fare_amount")).isEqualTo(21.5);
            assertThat(row.<Double>getAs("tip_amount")).isEqualTo(4.3);
            assertThat(row.<Double>getAs("total_amount")).isEqualTo(28.3);
        }

        @Test
        @DisplayName("a null passenger_count stays null rather than becoming zero")
        void nullPassengerCountIsNotCoerced() {
            Dataset<Row> bronze = SourceNormalizer.normalizeYellow(Fixtures.yellow2024());

            assertThat(bronze.filter(col("passenger_count").isNull()).count()).isEqualTo(1);
            assertThat(bronze.filter(col("passenger_count").equalTo(0)).count()).isZero();
        }
    }
}
