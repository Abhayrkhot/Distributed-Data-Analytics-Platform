package com.analyticsplatform.common.testing;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.sum;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

import com.analyticsplatform.common.schema.CanonicalSchema;
import com.analyticsplatform.common.schema.SchemaCompatibility;
import com.analyticsplatform.common.schema.SchemaCompatibility.ChangeType;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * Guards the hand-computed golden dataset.
 *
 * <p>The expectations in {@code tests/golden/} were written before the transformations existed, so
 * nothing yet produces them. What can be checked today is that they are internally consistent and
 * that the fixtures still contain the awkward cases the contract claims. Without this, a fixture
 * edit could quietly invalidate every expectation downstream and nobody would find out until a
 * silver test failed for reasons that had nothing to do with silver.
 */
class GoldenDatasetTest {

    private static final double TOLERANCE = 0.005;

    /**
     * Sums a column regardless of its inferred type. Spark's {@code sum} returns a {@code Long}
     * over integer columns and a {@code Double} over floating ones, so reading it as either
     * specific type breaks on the other.
     */
    private static double sumOf(Dataset<Row> data, String column) {
        Row row = data.agg(sum(col(column))).first();
        return row.isNullAt(0) ? 0.0 : ((Number) row.get(0)).doubleValue();
    }

    private static double numberAt(Row row, String column) {
        return ((Number) row.get(row.fieldIndex(column))).doubleValue();
    }

    @Nested
    @DisplayName("fixtures")
    class FixtureContents {

        @ParameterizedTest(name = "{0} has {1} rows")
        @CsvSource({"yellow2024, 12", "yellow2025, 3", "green2024, 4", "zones, 9"})
        void fixtureRowCounts(String which, long expected) {
            Dataset<Row> data = switch (which) {
                case "yellow2024" -> Fixtures.yellow2024();
                case "yellow2025" -> Fixtures.yellow2025();
                case "green2024" -> Fixtures.green2024();
                default -> Fixtures.taxiZones();
            };
            assertThat(data.count()).isEqualTo(expected);
        }

        /**
         * The precondition for the schema-evolution claim: 2025 differs from 2024 by exactly one
         * added nullable column. If a fixture edit broke this, the evolution test would still pass
         * for the wrong reason.
         */
        @Test
        @DisplayName("2025 differs from 2024 by exactly cbd_congestion_fee, additively")
        void yellow2025IsAdditiveOver2024() {
            SchemaCompatibility.SchemaDiff diff = SchemaCompatibility.classify(
                    Fixtures.YELLOW_2024_SCHEMA, Fixtures.YELLOW_2025_SCHEMA);

            assertThat(diff.changeType()).isEqualTo(ChangeType.ADDITIVE);
            assertThat(diff.addedColumns()).containsExactly("cbd_congestion_fee");
            assertThat(diff.removedColumns()).isEmpty();
        }

        /** Green is structurally different from yellow — that is the point of including it. */
        @Test
        @DisplayName("green and yellow are genuinely different schemas")
        void greenDiffersFromYellow() {
            assertThat(CanonicalSchema.hash(Fixtures.GREEN_2024_SCHEMA))
                    .isNotEqualTo(CanonicalSchema.hash(Fixtures.YELLOW_2024_SCHEMA));

            List<String> green = CanonicalSchema.fieldNames(Fixtures.GREEN_2024_SCHEMA);
            assertThat(green).contains("lpep_pickup_datetime", "ehail_fee", "trip_type")
                    .doesNotContain("tpep_pickup_datetime", "airport_fee");
        }

        @Test
        @DisplayName("the documented awkward rows are present")
        void awkwardRowsExist() {
            Dataset<Row> yellow = Fixtures.yellow2024();

            assertThat(yellow.filter(col("passenger_count").isNull()).count())
                    .as("null passenger_count").isEqualTo(1);
            assertThat(yellow.filter(col("tpep_dropoff_datetime").isNull()).count())
                    .as("null dropoff timestamp").isEqualTo(1);
            assertThat(yellow.filter(col("fare_amount").lt(0)).count())
                    .as("negative fare").isEqualTo(1);
            assertThat(yellow.filter(col("fare_amount").equalTo(0)).count())
                    .as("zero fare, for the null tip_pct case").isEqualTo(1);
            assertThat(yellow.filter(col("trip_distance").equalTo(0)).count())
                    .as("zero distance").isEqualTo(1);
            assertThat(yellow.filter(col("PULocationID").equalTo(264)).count())
                    .as("unknown pickup zone").isEqualTo(1);
            assertThat(yellow.filter(
                    col("tpep_dropoff_datetime").lt(col("tpep_pickup_datetime"))).count())
                    .as("inverted timestamps").isEqualTo(1);
            assertThat(Fixtures.green2024().filter(col("trip_distance").gt(300)).count())
                    .as("out-of-range distance").isEqualTo(1);
        }

        /** Row 3 must be an exact copy, or the golden output depends on the dedupe tiebreaker. */
        @Test
        @DisplayName("the duplicate row is an exact copy")
        void duplicateIsExact() {
            Dataset<Row> yellow = Fixtures.yellow2024();

            assertThat(yellow.count() - yellow.distinct().count())
                    .as("exactly one wholly-duplicated row").isEqualTo(1);
        }

        @Test
        @DisplayName("cross-midnight trip is present")
        void crossMidnightTripExists() {
            assertThat(Fixtures.yellow2024()
                    .filter("date(tpep_dropoff_datetime) > date(tpep_pickup_datetime)")
                    .count()).isEqualTo(1);
        }
    }

    @Nested
    @DisplayName("golden expectations reconcile")
    class Reconciliation {

        /** 19 bronze − 4 rejected − 1 duplicate = 14 silver. */
        @Test
        @DisplayName("bronze minus rejects minus duplicates equals silver")
        void rowCountsReconcile() {
            long bronze = Fixtures.yellow2024().count()
                    + Fixtures.yellow2025().count()
                    + Fixtures.green2024().count();
            long rejected = Fixtures.golden("expected_rejected.csv").count();
            long duplicates = 1;
            long silver = Fixtures.golden("expected_silver.csv").count();

            assertThat(bronze).isEqualTo(19);
            assertThat(rejected).isEqualTo(4);
            assertThat(silver).isEqualTo(14);
            assertThat(bronze - rejected - duplicates).isEqualTo(silver);
        }

        /**
         * Every aggregate must account for all 14 trips and all of silver's revenue. An aggregate
         * that quietly dropped a group would still look plausible on its own.
         */
        @ParameterizedTest(name = "{0} accounts for every trip and every dollar")
        @CsvSource({
            "expected_agg_borough_od.csv",
            "expected_agg_daily_kpi.csv",
            "expected_agg_payment_daily.csv",
            "expected_agg_zone_hourly.csv",
        })
        void aggregatesReconcileToSilver(String file) {
            Dataset<Row> silver = Fixtures.golden("expected_silver.csv");
            Dataset<Row> aggregate = Fixtures.golden(file);

            assertThat(sumOf(aggregate, "trip_count"))
                    .as("trip count in %s", file).isEqualTo(14.0);
            assertThat(sumOf(aggregate, "total_revenue"))
                    .as("revenue in %s", file)
                    .isCloseTo(sumOf(silver, "total_amount"), within(TOLERANCE));
        }

        @Test
        @DisplayName("silver revenue is the documented 525.10")
        void silverRevenueMatchesTheDocumentedTotal() {
            assertThat(sumOf(Fixtures.golden("expected_silver.csv"), "total_amount"))
                    .isCloseTo(525.10, within(TOLERANCE));
        }

        /** revenue_share must sum to 1.0 within each (date, source) partition. */
        @Test
        @DisplayName("payment revenue shares sum to one per day and source")
        void revenueSharesSumToOne() {
            Dataset<Row> shares = Fixtures.golden("expected_agg_payment_daily.csv")
                    .groupBy("pickup_date", "source")
                    .agg(sum("revenue_share").alias("total_share"));

            for (Row row : shares.collectAsList()) {
                assertThat(row.getDouble(row.fieldIndex("total_share")))
                        .as("shares for %s / %s", row.get(0), row.get(1))
                        .isCloseTo(1.0, within(0.000002));
            }
        }
    }

    @Nested
    @DisplayName("null semantics survive into the expectations")
    class NullSemantics {

        /**
         * The zero-fare trip has a null tip_pct, not zero. If it were zero it would drag every
         * average that includes it, which is precisely the silent corruption the null policy
         * exists to prevent.
         */
        @Test
        @DisplayName("the zero-fare trip carries a null tip_pct in silver")
        void zeroFareTripHasNullTipPct() {
            Dataset<Row> silver = Fixtures.golden("expected_silver.csv");

            assertThat(silver.filter(col("fare_amount").equalTo(0)).count()).isEqualTo(1);
            assertThat(silver.filter(col("fare_amount").equalTo(0)
                    .and(col("tip_pct").isNull())).count()).isEqualTo(1);
        }

        /** A group containing only that row averages to null, not to zero. */
        @Test
        @DisplayName("an all-null group averages to null in daily KPIs")
        void allNullGroupAveragesToNull() {
            Dataset<Row> kpi = Fixtures.golden("expected_agg_daily_kpi.csv")
                    .filter(col("pickup_date").equalTo("2024-01-17")
                            .and(col("source").equalTo("yellow"))
                            .and(col("vendor_id").equalTo(2)));

            assertThat(kpi.count()).isEqualTo(1);
            assertThat(kpi.filter(col("avg_tip_pct").isNull()).count()).isEqualTo(1);
        }

        /**
         * Where a group mixes null and non-null, the average ignores the null: 0.2, not 0.133.
         * Getting this wrong is easy and the difference is small enough to go unnoticed.
         */
        @Test
        @DisplayName("a mixed group averages only the non-null values")
        void mixedGroupIgnoresNulls() {
            Dataset<Row> payment = Fixtures.golden("expected_agg_payment_daily.csv")
                    .filter(col("pickup_date").equalTo("2024-01-17"));

            Row row = payment.first();
            assertThat(numberAt(row, "trip_count")).isEqualTo(3.0);
            assertThat(numberAt(row, "avg_tip_pct")).isCloseTo(0.2, within(TOLERANCE));
        }

        @Test
        @DisplayName("unknown zones are retained as Unknown, not dropped")
        void unknownZonesAreRetained() {
            Dataset<Row> silver = Fixtures.golden("expected_silver.csv");

            assertThat(silver.filter(col("pickup_borough").equalTo("Unknown")).count())
                    .isEqualTo(1);
            assertThat(silver.filter(col("dropoff_borough").equalTo("Unknown")).count())
                    .isEqualTo(1);
        }
    }
}
