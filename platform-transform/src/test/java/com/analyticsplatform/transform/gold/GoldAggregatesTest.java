package com.analyticsplatform.transform.gold;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.sum;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

import com.analyticsplatform.common.testing.Fixtures;
import com.analyticsplatform.common.testing.SparkTestSupport;
import com.analyticsplatform.ingest.source.SourceNormalizer;
import com.analyticsplatform.transform.silver.SilverTransform;
import java.util.List;
import java.util.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * The gold aggregates against the hand-computed golden files.
 *
 * <p>Every aggregate must independently account for all 14 trips and all 525.10 of silver's revenue.
 * That cross-check is what catches a dropped group: an aggregate missing one is internally
 * consistent and looks entirely reasonable in isolation.
 */
class GoldAggregatesTest {

    private static final double TOLERANCE = 0.0001;

    private static Dataset<Row> silver;

    @BeforeAll
    static void buildSilver() {
        SilverTransform.registerUdfs(SparkTestSupport.spark());
        Dataset<Row> bronze = SourceNormalizer.normalizeYellow(Fixtures.yellow2024())
                .union(SourceNormalizer.normalizeYellow(Fixtures.yellow2025()))
                .union(SourceNormalizer.normalizeGreen(Fixtures.green2024()));
        silver = SilverTransform.transform(bronze, Fixtures.taxiZones()).cache();
    }

    private static double sumOf(Dataset<Row> data, String column) {
        Row row = data.agg(sum(col(column))).first();
        return row.isNullAt(0) ? 0.0 : ((Number) row.get(0)).doubleValue();
    }

    @Nested
    @DisplayName("every aggregate reconciles to silver")
    class Reconciliation {

        static java.util.stream.Stream<org.junit.jupiter.params.provider.Arguments> aggregates() {
            return java.util.stream.Stream.of(
                    org.junit.jupiter.params.provider.Arguments.of(
                            "zone_hourly", (Function<Dataset<Row>, Dataset<Row>>) GoldAggregates::zoneHourly),
                    org.junit.jupiter.params.provider.Arguments.of(
                            "borough_od", (Function<Dataset<Row>, Dataset<Row>>) GoldAggregates::boroughOd),
                    org.junit.jupiter.params.provider.Arguments.of(
                            "payment_daily", (Function<Dataset<Row>, Dataset<Row>>) GoldAggregates::paymentDaily),
                    org.junit.jupiter.params.provider.Arguments.of(
                            "daily_kpi", (Function<Dataset<Row>, Dataset<Row>>) GoldAggregates::dailyKpi));
        }

        @ParameterizedTest(name = "{0} accounts for every trip and every dollar")
        @org.junit.jupiter.params.provider.MethodSource("aggregates")
        void aggregateReconciles(String name, Function<Dataset<Row>, Dataset<Row>> aggregate) {
            Dataset<Row> result = aggregate.apply(silver);

            assertThat(sumOf(result, "trip_count")).as("%s trip count", name).isEqualTo(14.0);
            assertThat(sumOf(result, "total_revenue")).as("%s revenue", name)
                    .isCloseTo(525.10, within(0.005));
        }
    }

    @Nested
    @DisplayName("group counts match the golden files")
    class GroupCounts {

        @ParameterizedTest(name = "{0} produces {1} groups")
        @CsvSource({
            "zone_hourly,   14",
            "borough_od,    12",
            "payment_daily, 11",
            "daily_kpi,     11",
        })
        void groupCounts(String name, long expected) {
            Dataset<Row> result = switch (name) {
                case "zone_hourly" -> GoldAggregates.zoneHourly(silver);
                case "borough_od" -> GoldAggregates.boroughOd(silver);
                case "payment_daily" -> GoldAggregates.paymentDaily(silver);
                default -> GoldAggregates.dailyKpi(silver);
            };

            assertThat(result.count()).isEqualTo(expected);
            assertThat(Fixtures.golden("expected_agg_" + name + ".csv").count()).isEqualTo(expected);
        }
    }

    @Nested
    @DisplayName("borough OD matches expected values")
    class BoroughOd {

        /** The multi-row group: two Manhattan-to-Manhattan trips on 2024-01-15. */
        @Test
        @DisplayName("a two-trip group averages correctly")
        void twoTripGroup() {
            Row row = GoldAggregates.boroughOd(silver)
                    .filter(col("pickup_date").equalTo("2024-01-15")
                            .and(col("pickup_borough").equalTo("Manhattan"))
                            .and(col("dropoff_borough").equalTo("Manhattan")))
                    .first();

            assertThat(row.getLong(row.fieldIndex("trip_count"))).isEqualTo(2);
            assertThat(((Number) row.getAs("total_revenue")).doubleValue())
                    .isCloseTo(40.30, within(0.005));
            assertThat(((Number) row.getAs("avg_distance_mi")).doubleValue())
                    .as("(3.50 + 1.20) / 2").isCloseTo(2.35, within(TOLERANCE));
            assertThat(((Number) row.getAs("avg_duration_min")).doubleValue())
                    .as("(22 + 12) / 2").isCloseTo(17.0, within(TOLERANCE));
        }

        /** Unknown boroughs appear on both sides rather than being dropped. */
        @Test
        @DisplayName("unknown boroughs survive into the matrix")
        void unknownBoroughsAppear() {
            Dataset<Row> od = GoldAggregates.boroughOd(silver);

            assertThat(od.filter(col("pickup_borough").equalTo("Unknown")).count()).isEqualTo(1);
            assertThat(od.filter(col("dropoff_borough").equalTo("Unknown")).count()).isEqualTo(1);
        }
    }

    @Nested
    @DisplayName("null averages behave as silver intended")
    class NullAverages {

        /**
         * The payoff from making {@code tip_pct} null rather than zero in silver. This group holds
         * three credit-card trips, one of which has a null tip_pct; the average is of the other
         * two, not diluted by a fabricated zero.
         */
        @Test
        @DisplayName("a mixed group averages only its non-null values")
        void mixedGroupIgnoresNull() {
            Row row = GoldAggregates.paymentDaily(silver)
                    .filter(col("pickup_date").equalTo("2024-01-17")).first();

            assertThat(row.getLong(row.fieldIndex("trip_count"))).isEqualTo(3);
            assertThat(((Number) row.getAs("avg_tip_pct")).doubleValue())
                    .as("average of 0.2 and 0.2, not 0.1333").isCloseTo(0.2, within(TOLERANCE));
        }

        /** A group whose only row has a null tip_pct averages to null, not to zero. */
        @Test
        @DisplayName("an all-null group averages to null")
        void allNullGroupIsNull() {
            Row row = GoldAggregates.dailyKpi(silver)
                    .filter(col("pickup_date").equalTo("2024-01-17")
                            .and(col("source").equalTo("yellow"))
                            .and(col("vendor_id").equalTo(2)))
                    .first();

            assertThat(row.isNullAt(row.fieldIndex("avg_tip_pct")))
                    .as("null, not a fabricated 0.0").isTrue();
        }
    }

    @Nested
    @DisplayName("revenue share")
    class RevenueShare {

        /** Shares must sum to exactly 1.0 within each (date, source) partition. */
        @Test
        @DisplayName("shares sum to one per day and source")
        void sharesSumToOne() {
            List<Row> partitions = GoldAggregates.paymentDaily(silver)
                    .groupBy("pickup_date", "source")
                    .agg(sum("revenue_share").alias("total"))
                    .collectAsList();

            assertThat(partitions).isNotEmpty();
            for (Row row : partitions) {
                assertThat(((Number) row.getAs("total")).doubleValue())
                        .as("shares for %s / %s", row.get(0), row.get(1))
                        .isCloseTo(1.0, within(0.000002));
            }
        }

        @Test
        @DisplayName("a single-payment day has a share of exactly one")
        void singlePaymentDayIsWhole() {
            Row row = GoldAggregates.paymentDaily(silver)
                    .filter(col("pickup_date").equalTo("2024-01-17")).first();

            assertThat(((Number) row.getAs("revenue_share")).doubleValue())
                    .isCloseTo(1.0, within(0.000002));
        }

        @ParameterizedTest(name = "{0} {1} share is {2}")
        @CsvSource({
            "2024-01-15, credit_card, 0.854897",
            "2024-01-15, cash,        0.145103",
            "2025-01-05, credit_card, 0.663561",
        })
        void yellowSharesMatchGolden(String date, String payment, double expected) {
            Row row = GoldAggregates.paymentDaily(silver)
                    .filter(col("pickup_date").equalTo(date)
                            .and(col("source").equalTo("yellow"))
                            .and(col("payment_type").equalTo(payment)))
                    .first();

            assertThat(((Number) row.getAs("revenue_share")).doubleValue())
                    .isCloseTo(expected, within(0.000002));
        }
    }

    @Nested
    @DisplayName("fact table")
    class FactTable {

        @Test
        @DisplayName("carries every silver row and stamps the producing run")
        void factCarriesEveryRow() {
            Dataset<Row> fact = GoldAggregates.factTrip(silver, 4242L);

            assertThat(fact.count()).isEqualTo(silver.count());
            assertThat(fact.filter(col("etl_run_id").equalTo(4242L)).count())
                    .as("every row traceable to its run").isEqualTo(14);
            assertThat(List.of(fact.columns())).contains("pickup_date", "etl_run_id");
        }

        @Test
        @DisplayName("revenue is preserved exactly")
        void factRevenueMatches() {
            assertThat(sumOf(GoldAggregates.factTrip(silver, 1L), "total_amount"))
                    .isCloseTo(525.10, within(0.005));
        }
    }

    @Nested
    @DisplayName("aggregation is order-independent")
    class Metamorphic {

        /**
         * §18: identical logical output regardless of input ordering or partitioning. An aggregate
         * that depended on either would produce different numbers on a differently-sized cluster.
         */
        @ParameterizedTest(name = "{0} partitions yields identical output")
        @CsvSource({"1", "3", "7"})
        void partitionCountDoesNotChangeOutput(int partitions) {
            Dataset<Row> repartitioned = silver.repartition(partitions);

            assertThat(sumOf(GoldAggregates.boroughOd(repartitioned), "total_revenue"))
                    .isCloseTo(sumOf(GoldAggregates.boroughOd(silver), "total_revenue"),
                            within(0.005));
            assertThat(GoldAggregates.boroughOd(repartitioned).count())
                    .isEqualTo(GoldAggregates.boroughOd(silver).count());
        }
    }
}
