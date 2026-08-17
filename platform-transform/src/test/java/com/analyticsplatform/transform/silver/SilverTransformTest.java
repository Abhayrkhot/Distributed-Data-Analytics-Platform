package com.analyticsplatform.transform.silver;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

import com.analyticsplatform.common.testing.Fixtures;
import com.analyticsplatform.common.testing.SparkTestSupport;
import com.analyticsplatform.ingest.source.SourceNormalizer;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * Silver against the golden dataset.
 *
 * <p>The expectations in {@code tests/golden/expected_silver.csv} were hand-computed before this
 * transformation existed, so these tests check that the code satisfies the specification rather
 * than that the specification describes the code. Assertions are on exact values — a transformation
 * producing 14 rows of nulls would pass any count-only test.
 */
class SilverTransformTest {

    private static final double TOLERANCE = 0.0001;

    private static Dataset<Row> bronze;
    private static Dataset<Row> silver;
    private static Dataset<Row> golden;

    @BeforeAll
    static void transformOnce() {
        SilverTransform.registerUdfs(SparkTestSupport.spark());

        bronze = SourceNormalizer.normalizeYellow(Fixtures.yellow2024())
                .union(SourceNormalizer.normalizeYellow(Fixtures.yellow2025()))
                .union(SourceNormalizer.normalizeGreen(Fixtures.green2024()));

        silver = SilverTransform.transform(bronze, Fixtures.taxiZones()).cache();
        golden = Fixtures.golden("expected_silver.csv").cache();
    }

    /** Locates a silver row by its pickup timestamp, which is unique across the fixture. */
    private static Row at(String pickupTs) {
        List<Row> matches = silver.filter(col("pickup_ts").equalTo(pickupTs)).collectAsList();
        assertThat(matches).as("exactly one row at %s", pickupTs).hasSize(1);
        return matches.get(0);
    }

    private static double dbl(Row row, String column) {
        return row.getDouble(row.fieldIndex(column));
    }

    @Nested
    @DisplayName("row counts reconcile with the contract")
    class Counts {

        @Test
        @DisplayName("19 bronze minus 4 rejected minus 1 duplicate equals 14 silver")
        void reconciliation() {
            long rejected = SilverTransform.reject(bronze).count();

            assertThat(bronze.count()).isEqualTo(19);
            assertThat(rejected).isEqualTo(4);
            assertThat(silver.count()).isEqualTo(14);
            assertThat(bronze.count() - rejected - 1).isEqualTo(silver.count());
        }

        @Test
        @DisplayName("silver matches the golden row count")
        void matchesGoldenCount() {
            assertThat(silver.count()).isEqualTo(golden.count());
        }

        @Test
        @DisplayName("trip keys are unique after deduplication")
        void tripKeysAreUnique() {
            assertThat(silver.select("trip_key").distinct().count()).isEqualTo(silver.count());
        }
    }

    @Nested
    @DisplayName("rejection")
    class Rejection {

        /** Each rejected row must be attributed to the rule that actually caught it. */
        @ParameterizedTest(name = "{0} is rejected as {1}")
        @CsvSource({
            "2024-01-16 12:00:00, negative_fare",
            "2024-01-16 13:00:00, dropoff_not_after_pickup",
            "2024-01-17 10:00:00, null_timestamp",
            "2024-01-16 17:00:00, distance_out_of_range",
        })
        void rejectionReasonsAreAttributed(String pickupTs, String reason) {
            List<Row> rows = SilverTransform.reject(bronze)
                    .filter(col("pickup_ts").equalTo(pickupTs)).collectAsList();

            assertThat(rows).hasSize(1);
            assertThat(rows.get(0).getString(rows.get(0).fieldIndex("reject_reason")))
                    .isEqualTo(reason);
        }

        @Test
        @DisplayName("rejected rows match the golden rejection list")
        void matchesGoldenRejections() {
            Dataset<Row> expected = Fixtures.golden("expected_rejected.csv");

            assertThat(SilverTransform.reject(bronze).count()).isEqualTo(expected.count());
            assertThat(SilverTransform.reject(bronze)
                    .select("reject_reason").distinct().count()).isEqualTo(4);
        }

        @Test
        @DisplayName("no rejected row reaches silver")
        void rejectedRowsAreAbsentFromSilver() {
            assertThat(silver.filter(col("fare_amount").lt(0)).count()).isZero();
            assertThat(silver.filter(col("trip_distance_mi").gt(300)).count()).isZero();
            assertThat(silver.filter(col("dropoff_ts").leq(col("pickup_ts"))).count()).isZero();
            assertThat(silver.filter(col("pickup_ts").isNull()
                    .or(col("dropoff_ts").isNull())).count()).isZero();
        }
    }

    @Nested
    @DisplayName("derived values match the hand-computed expectations")
    class Derived {

        @ParameterizedTest(name = "{0}: {1} min, {2} mph")
        @CsvSource({
            "2024-01-15 08:30:00,  22.0,  9.5455",
            "2024-01-15 09:00:00,  12.0,  6.0",
            "2024-01-15 23:45:00,  30.0, 16.0",     // crosses midnight
            "2024-01-16 10:00:00,  20.0, 12.0",
            "2024-01-16 11:00:00,   5.0,  0.0",     // zero distance
            "2024-01-17 07:00:00,  30.0, 24.0",
            "2024-01-17 09:00:00, 120.0, 30.0",
            "2025-01-05 08:00:00,  25.0, 10.8",
            "2024-01-15 14:00:00,  20.0,  9.0",     // green
        })
        void durationAndSpeed(String pickupTs, double duration, double speed) {
            Row row = at(pickupTs);

            assertThat(dbl(row, "trip_duration_min")).isCloseTo(duration, within(TOLERANCE));
            assertThat(dbl(row, "avg_speed_mph")).isCloseTo(speed, within(TOLERANCE));
        }

        /** A trip crossing midnight must measure 30 minutes, not negative or 23 hours. */
        @Test
        @DisplayName("a cross-midnight trip computes a correct duration")
        void crossMidnightDuration() {
            assertThat(dbl(at("2024-01-15 23:45:00"), "trip_duration_min"))
                    .isCloseTo(30.0, within(TOLERANCE));
        }

        @Test
        @DisplayName("tip percentage is fare-relative")
        void tipPercentage() {
            assertThat(dbl(at("2024-01-15 08:30:00"), "tip_pct"))
                    .isCloseTo(0.2, within(TOLERANCE));
            assertThat(dbl(at("2024-01-15 09:00:00"), "tip_pct"))
                    .isCloseTo(0.0, within(TOLERANCE));
        }

        /**
         * The zero-fare trip. Null, not zero and not infinity — encoding it as zero would drag
         * down every average that includes it.
         */
        @Test
        @DisplayName("a zero-fare trip has a null tip percentage")
        void zeroFareTipPercentIsNull() {
            Row row = at("2024-01-17 08:00:00");

            assertThat(row.isNullAt(row.fieldIndex("tip_pct"))).isTrue();
            assertThat(dbl(row, "trip_duration_min")).isCloseTo(5.0, within(TOLERANCE));
        }

        @Test
        @DisplayName("a zero-distance trip has speed zero, not null or infinity")
        void zeroDistanceSpeedIsZero() {
            Row row = at("2024-01-16 11:00:00");

            assertThat(row.isNullAt(row.fieldIndex("avg_speed_mph"))).isFalse();
            assertThat(dbl(row, "avg_speed_mph")).isCloseTo(0.0, within(TOLERANCE));
        }
    }

    @Nested
    @DisplayName("enrichment")
    class Enrichment {

        @ParameterizedTest(name = "{0}: {1}/{2} -> {3}/{4}")
        @CsvSource({
            "2024-01-15 08:30:00, Manhattan, Lincoln Square East, Manhattan, Upper East Side North",
            "2024-01-15 14:00:00, Queens,    Astoria,             Brooklyn,  Brooklyn Heights",
            "2024-01-15 23:45:00, Manhattan, Upper East Side North, Queens,  Astoria",
        })
        void zonesAreJoined(String pickupTs, String puBorough, String puZone,
                            String doBorough, String doZone) {
            Row row = at(pickupTs);

            assertThat(row.getString(row.fieldIndex("pickup_borough"))).isEqualTo(puBorough);
            assertThat(row.getString(row.fieldIndex("pickup_zone"))).isEqualTo(puZone);
            assertThat(row.getString(row.fieldIndex("dropoff_borough"))).isEqualTo(doBorough);
            assertThat(row.getString(row.fieldIndex("dropoff_zone"))).isEqualTo(doZone);
        }

        /**
         * A trip with an unrecognized zone is kept and labelled, not dropped. Losing revenue from
         * the warehouse to avoid admitting a gap in a reference table is the wrong trade.
         */
        @Test
        @DisplayName("an unknown zone is labelled Unknown rather than dropped")
        void unknownZoneIsRetained() {
            Row row = at("2024-01-17 07:00:00");

            assertThat(row.getString(row.fieldIndex("pickup_borough"))).isEqualTo("Unknown");
            assertThat(row.getString(row.fieldIndex("pickup_zone"))).isEqualTo("NV");
            assertThat(row.getString(row.fieldIndex("dropoff_borough"))).isEqualTo("Manhattan");
        }

        @Test
        @DisplayName("payment codes map to labels")
        void paymentLabels() {
            assertThat(at("2024-01-15 08:30:00").getString(
                    at("2024-01-15 08:30:00").fieldIndex("payment_type"))).isEqualTo("credit_card");
            assertThat(at("2024-01-15 09:00:00").getString(
                    at("2024-01-15 09:00:00").fieldIndex("payment_type"))).isEqualTo("cash");
        }
    }

    @Nested
    @DisplayName("deduplication")
    class Deduplication {

        @Test
        @DisplayName("the exact duplicate collapses to one row")
        void exactDuplicateCollapses() {
            assertThat(silver.filter(col("pickup_ts").equalTo("2024-01-15 08:30:00")).count())
                    .isEqualTo(1);
        }

        /**
         * A PARTIAL duplicate: two rows sharing a trip_key but differing elsewhere. The fixture's
         * duplicate is an exact copy, so it cannot detect an arbitrary survivor — either choice
         * looks identical. Only a partial duplicate distinguishes an ordered window from
         * dropDuplicates, which is what makes this the test that matters.
         */
        @Test
        @DisplayName("a partial duplicate resolves to a fixed survivor")
        void partialDuplicateHasDeterministicSurvivor() {
            Dataset<Row> pair = bronze
                    .filter(col("pickup_ts").equalTo("2024-01-15 09:00:00"))
                    .limit(1);
            // Same trip_key inputs, different passenger_count: the tiebreaker must decide.
            Dataset<Row> variant = pair.withColumn("passenger_count", lit(9L));
            Dataset<Row> withPartialDuplicate = pair.union(variant);

            Dataset<Row> result = SilverTransform.transform(
                    withPartialDuplicate, Fixtures.taxiZones());

            assertThat(result.count()).as("collapses to one row").isEqualTo(1);
            Row survivor = result.first();
            assertThat(survivor.<Long>getAs("passenger_count"))
                    .as("the tiebreaker prefers the lower passenger_count, always")
                    .isEqualTo(2L);
        }

        /**
         * dropDuplicates picks an arbitrary survivor, so identical input could yield different
         * output between runs. The ordered window makes the choice fixed.
         */
        @Test
        @DisplayName("deduplication is deterministic across repartitioning")
        void deduplicationIsDeterministic() {
            Dataset<Row> viaOnePartition = SilverTransform.transform(
                    bronze.repartition(1), Fixtures.taxiZones());
            Dataset<Row> viaManyPartitions = SilverTransform.transform(
                    bronze.repartition(7), Fixtures.taxiZones());

            List<String> one = viaOnePartition.select("trip_key").as(
                    org.apache.spark.sql.Encoders.STRING()).collectAsList()
                    .stream().sorted().toList();
            List<String> many = viaManyPartitions.select("trip_key").as(
                    org.apache.spark.sql.Encoders.STRING()).collectAsList()
                    .stream().sorted().toList();

            assertThat(many).isEqualTo(one);
        }
    }

    @Nested
    @DisplayName("golden comparison")
    class GoldenComparison {

        /**
         * The whole silver output against the hand-computed expectation, column by column. This is
         * the test that would catch a subtle arithmetic change anywhere in the transformation.
         */
        @Test
        @DisplayName("every silver row matches the golden expectation exactly")
        void fullOutputMatchesGolden() {
            List<Row> expected = golden.orderBy("source", "pickup_ts").collectAsList();
            List<Row> actual = silver.orderBy("source", "pickup_ts").collectAsList();

            assertThat(actual).hasSameSizeAs(expected);

            for (int i = 0; i < expected.size(); i++) {
                Row want = expected.get(i);
                Row got = actual.get(i);
                String where = "row " + i + " (" + want.getAs("pickup_ts") + ")";

                assertThat(got.<String>getAs("source")).as(where + " source")
                        .isEqualTo(want.getAs("source"));
                assertThat(got.<String>getAs("pickup_borough")).as(where + " pickup_borough")
                        .isEqualTo(want.getAs("pickup_borough"));
                assertThat(got.<String>getAs("dropoff_zone")).as(where + " dropoff_zone")
                        .isEqualTo(want.getAs("dropoff_zone"));
                assertThat(got.<String>getAs("payment_type")).as(where + " payment_type")
                        .isEqualTo(want.getAs("payment_type"));
                assertThat(dbl(got, "trip_duration_min")).as(where + " duration")
                        .isCloseTo(((Number) want.getAs("trip_duration_min")).doubleValue(),
                                within(TOLERANCE));
                assertThat(dbl(got, "total_amount")).as(where + " total")
                        .isCloseTo(((Number) want.getAs("total_amount")).doubleValue(),
                                within(TOLERANCE));

                int tipIndex = want.fieldIndex("tip_pct");
                if (want.isNullAt(tipIndex)) {
                    assertThat(got.isNullAt(got.fieldIndex("tip_pct")))
                            .as(where + " tip_pct should be null").isTrue();
                } else {
                    assertThat(dbl(got, "tip_pct")).as(where + " tip_pct")
                            .isCloseTo(((Number) want.getAs("tip_pct")).doubleValue(),
                                    within(TOLERANCE));
                }
            }
        }

        @Test
        @DisplayName("total revenue matches the documented 525.10")
        void revenueMatches() {
            Row row = silver.agg(org.apache.spark.sql.functions.sum("total_amount")).first();

            assertThat(row.getDouble(0)).isCloseTo(525.10, within(0.005));
        }
    }
}
