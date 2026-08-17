package com.analyticsplatform.stream.job;

import static org.apache.spark.sql.functions.col;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

import com.analyticsplatform.common.testing.SparkTestSupport;
import com.analyticsplatform.stream.event.EventValidator;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * Window semantics and malformed-event handling, against static data.
 *
 * <p>Deliberately not a streaming test. Boundary behaviour — which window an event on the edge lands
 * in, whether a late event is counted — is a property of the aggregation, and testing it through a
 * running query would be slow, flaky, and no more convincing.
 */
class WindowAggregatorTest {

    private static final StructType EVENT_SCHEMA = new StructType(new StructField[] {
        new StructField("event_id", DataTypes.StringType, true, Metadata.empty()),
        new StructField("trip_key", DataTypes.StringType, true, Metadata.empty()),
        new StructField("event_time", DataTypes.TimestampType, true, Metadata.empty()),
        new StructField("producer_timestamp", DataTypes.TimestampType, true, Metadata.empty()),
        new StructField("schema_version", DataTypes.IntegerType, true, Metadata.empty()),
        new StructField("source", DataTypes.StringType, true, Metadata.empty()),
        new StructField("pickup_borough", DataTypes.StringType, true, Metadata.empty()),
        new StructField("fare_amount", DataTypes.DoubleType, true, Metadata.empty()),
        new StructField("total_amount", DataTypes.DoubleType, true, Metadata.empty()),
        new StructField("trip_distance_mi", DataTypes.DoubleType, true, Metadata.empty()),
    });

    /** A well-formed event at the given instant. */
    private static Row event(String id, String instant, String borough, double total) {
        return RowFactory.create(id, "trip-" + id,
                Timestamp.from(Instant.parse(instant)),
                Timestamp.from(Instant.parse(instant)),
                1, "yellow", borough, total / 2.0, total, 3.0);
    }

    private static Dataset<Row> events(Row... rows) {
        return SparkTestSupport.spark().createDataFrame(List.of(rows), EVENT_SCHEMA);
    }

    private static Dataset<Row> aggregate(Dataset<Row> events) {
        return WindowAggregator.aggregateBatch(EventValidator.valid(
                EventValidator.classify(events)));
    }

    @Nested
    @DisplayName("window boundaries")
    class Boundaries {

        /**
         * A five-minute tumbling window is half-open: [00:00, 00:05). An event exactly on the
         * boundary belongs to the later window. Getting this wrong double-counts or loses one event
         * per boundary, which is invisible at small scale and material at large.
         */
        @ParameterizedTest(name = "an event at {0} lands in the window starting {1}")
        @CsvSource({
            "2024-01-15T08:00:00Z, 2024-01-15T08:00:00Z",
            "2024-01-15T08:04:59Z, 2024-01-15T08:00:00Z",
            "2024-01-15T08:05:00Z, 2024-01-15T08:05:00Z",   // exactly on the boundary
            "2024-01-15T08:09:59Z, 2024-01-15T08:05:00Z",
            "2024-01-15T08:10:00Z, 2024-01-15T08:10:00Z",
        })
        void windowAssignment(String eventTime, String expectedStart) {
            List<Row> rows = aggregate(events(event("e1", eventTime, "Manhattan", 10.0)))
                    .collectAsList();

            assertThat(rows).hasSize(1);
            assertThat(rows.get(0).<Timestamp>getAs("window_start").toInstant())
                    .isEqualTo(Instant.parse(expectedStart));
        }

        @Test
        @DisplayName("window_end is exactly five minutes after window_start")
        void windowWidthIsFiveMinutes() {
            Row row = aggregate(events(
                    event("e1", "2024-01-15T08:02:00Z", "Manhattan", 10.0))).first();

            Instant start = row.<Timestamp>getAs("window_start").toInstant();
            Instant end = row.<Timestamp>getAs("window_end").toInstant();

            assertThat(java.time.Duration.between(start, end).toMinutes()).isEqualTo(5);
        }

        @Test
        @DisplayName("events spanning several windows produce one row each")
        void eventsSpanMultipleWindows() {
            Dataset<Row> result = aggregate(events(
                    event("e1", "2024-01-15T08:01:00Z", "Manhattan", 10.0),
                    event("e2", "2024-01-15T08:06:00Z", "Manhattan", 20.0),
                    event("e3", "2024-01-15T08:11:00Z", "Manhattan", 30.0)));

            assertThat(result.count()).isEqualTo(3);
        }

        @Test
        @DisplayName("boroughs are aggregated separately within a window")
        void boroughsAreSeparate() {
            Dataset<Row> result = aggregate(events(
                    event("e1", "2024-01-15T08:01:00Z", "Manhattan", 10.0),
                    event("e2", "2024-01-15T08:02:00Z", "Queens", 20.0),
                    event("e3", "2024-01-15T08:03:00Z", "Manhattan", 30.0)));

            assertThat(result.count()).isEqualTo(2);
            Row manhattan = result.filter(col("pickup_borough").equalTo("Manhattan")).first();
            assertThat(manhattan.getLong(manhattan.fieldIndex("trip_count"))).isEqualTo(2);
            assertThat(((Number) manhattan.getAs("total_revenue")).doubleValue())
                    .isCloseTo(40.0, within(0.005));
        }
    }

    @Nested
    @DisplayName("aggregate values")
    class Values {

        @Test
        @DisplayName("counts, sums and averages within a window")
        void aggregatesCorrectly() {
            Row row = aggregate(events(
                    event("e1", "2024-01-15T08:01:00Z", "Manhattan", 10.0),
                    event("e2", "2024-01-15T08:02:00Z", "Manhattan", 20.0),
                    event("e3", "2024-01-15T08:03:00Z", "Manhattan", 30.0))).first();

            assertThat(row.getLong(row.fieldIndex("trip_count"))).isEqualTo(3);
            assertThat(((Number) row.getAs("total_revenue")).doubleValue())
                    .isCloseTo(60.0, within(0.005));
            // fare is total/2 in the fixture, so (5 + 10 + 15) / 3
            assertThat(((Number) row.getAs("avg_fare")).doubleValue())
                    .isCloseTo(10.0, within(0.0001));
        }

        /** Ordering must not affect the result — §18 input-order invariance. */
        @Test
        @DisplayName("input order does not change the output")
        void orderIndependent() {
            Row[] forward = {
                event("e1", "2024-01-15T08:01:00Z", "Manhattan", 10.0),
                event("e2", "2024-01-15T08:02:00Z", "Manhattan", 20.0),
                event("e3", "2024-01-15T08:03:00Z", "Manhattan", 30.0)};
            Row[] reversed = {forward[2], forward[1], forward[0]};

            Row a = aggregate(events(forward)).first();
            Row b = aggregate(events(reversed)).first();

            assertThat(((Number) b.getAs("total_revenue")).doubleValue())
                    .isEqualTo(((Number) a.getAs("total_revenue")).doubleValue());
            assertThat(b.getLong(b.fieldIndex("trip_count")))
                    .isEqualTo(a.getLong(a.fieldIndex("trip_count")));
        }

        /** §18 partition-count invariance: identical output on any cluster shape. */
        @ParameterizedTest(name = "{0} partitions yields identical output")
        @CsvSource({"1", "3", "8"})
        void partitionCountIndependent(int partitions) {
            Dataset<Row> source = events(
                    event("e1", "2024-01-15T08:01:00Z", "Manhattan", 10.0),
                    event("e2", "2024-01-15T08:02:00Z", "Queens", 20.0),
                    event("e3", "2024-01-15T08:07:00Z", "Manhattan", 30.0));

            Dataset<Row> once = aggregate(source.repartition(1));
            Dataset<Row> many = aggregate(source.repartition(partitions));

            assertThat(many.count()).isEqualTo(once.count());
            assertThat(sumRevenue(many)).isCloseTo(sumRevenue(once), within(0.005));
        }

        private static double sumRevenue(Dataset<Row> data) {
            Row row = data.agg(org.apache.spark.sql.functions.sum("total_revenue")).first();
            return row.isNullAt(0) ? 0.0 : ((Number) row.get(0)).doubleValue();
        }
    }

    @Nested
    @DisplayName("determinism")
    class Determinism {

        /**
         * The property replay correctness rests on: identical input yields byte-identical output.
         * A processing-time value or a random anywhere in the aggregation would break this, and
         * the ReplacingMergeTree version would then be choosing between two different answers
         * rather than deduplicating one.
         */
        @Test
        @DisplayName("repeated aggregation of identical input is byte-identical")
        void repeatedAggregationIsIdentical() {
            Dataset<Row> source = events(
                    event("e1", "2024-01-15T08:01:00Z", "Manhattan", 10.0),
                    event("e2", "2024-01-15T08:02:00Z", "Queens", 20.0),
                    event("e3", "2024-01-15T08:07:00Z", "Manhattan", 30.0));

            List<String> first = render(aggregate(source));
            for (int attempt = 0; attempt < 5; attempt++) {
                assertThat(render(aggregate(source)))
                        .as("attempt %d must match the first", attempt)
                        .isEqualTo(first);
            }
        }

        /** Canonical rendering so comparison covers every column, not just counts. */
        private static List<String> render(Dataset<Row> data) {
            List<String> out = new ArrayList<>();
            for (Row row : data.orderBy("window_start", "pickup_borough").collectAsList()) {
                out.add(row.mkString("|"));
            }
            return out;
        }
    }

    @Nested
    @DisplayName("malformed events are classified, never fatal")
    class Malformed {

        @Test
        @DisplayName("an unparseable message is rejected as unparseable")
        void unparseableIsClassified() {
            Row allNull = RowFactory.create(
                    null, null, null, null, null, null, null, null, null, null);
            Dataset<Row> classified = EventValidator.classify(events(allNull));

            assertThat(EventValidator.rejected(classified).count()).isEqualTo(1);
            assertThat(EventValidator.rejected(classified).first()
                    .<String>getAs("reject_reason")).isEqualTo("unparseable");
        }

        @ParameterizedTest(name = "{0} is rejected")
        @CsvSource({
            "missing_event_id",
            "missing_trip_key",
            "missing_event_time",
            "unknown_schema_version",
            "unknown_source",
            "missing_amount",
        })
        void everyMalformedClassIsRejected(String expectedReason) {
            Row row = switch (expectedReason) {
                case "missing_event_id" -> RowFactory.create(null, "t1",
                        Timestamp.from(Instant.parse("2024-01-15T08:00:00Z")), null, 1,
                        "yellow", "Manhattan", 1.0, 1.0, 1.0);
                case "missing_trip_key" -> RowFactory.create("e1", null,
                        Timestamp.from(Instant.parse("2024-01-15T08:00:00Z")), null, 1,
                        "yellow", "Manhattan", 1.0, 1.0, 1.0);
                case "missing_event_time" -> RowFactory.create("e1", "t1", null, null, 1,
                        "yellow", "Manhattan", 1.0, 1.0, 1.0);
                case "unknown_schema_version" -> RowFactory.create("e1", "t1",
                        Timestamp.from(Instant.parse("2024-01-15T08:00:00Z")), null, 99,
                        "yellow", "Manhattan", 1.0, 1.0, 1.0);
                case "unknown_source" -> RowFactory.create("e1", "t1",
                        Timestamp.from(Instant.parse("2024-01-15T08:00:00Z")), null, 1,
                        "blue", "Manhattan", 1.0, 1.0, 1.0);
                default -> RowFactory.create("e1", "t1",
                        Timestamp.from(Instant.parse("2024-01-15T08:00:00Z")), null, 1,
                        "yellow", "Manhattan", 1.0, null, 1.0);
            };

            Dataset<Row> classified = EventValidator.classify(events(row));

            assertThat(EventValidator.rejected(classified).count()).isEqualTo(1);
            assertThat(EventValidator.rejected(classified).first()
                    .<String>getAs("reject_reason")).isEqualTo(expectedReason);
            assertThat(EventValidator.valid(classified).count()).isZero();
        }

        /**
         * A rejected event must not reach the aggregate. Contaminating a window with an event whose
         * amount is null would silently understate revenue.
         */
        @Test
        @DisplayName("rejected events never reach an aggregate")
        void rejectedEventsAreExcluded() {
            Row good = event("e1", "2024-01-15T08:01:00Z", "Manhattan", 10.0);
            Row bad = RowFactory.create("e2", "t2",
                    Timestamp.from(Instant.parse("2024-01-15T08:02:00Z")), null, 1,
                    "yellow", "Manhattan", 1.0, null, 1.0);

            Dataset<Row> result = aggregate(events(good, bad));

            Row row = result.first();
            assertThat(row.getLong(row.fieldIndex("trip_count")))
                    .as("only the well-formed event is counted").isEqualTo(1);
            assertThat(((Number) row.getAs("total_revenue")).doubleValue())
                    .isCloseTo(10.0, within(0.005));
        }

        /** One bad message among many must not cost the good ones. */
        @Test
        @DisplayName("a single bad message does not lose the batch")
        void oneBadMessageDoesNotLoseTheBatch() {
            List<Row> rows = new ArrayList<>();
            for (int i = 0; i < 9; i++) {
                rows.add(event("e" + i, "2024-01-15T08:0" + (i % 5) + ":00Z", "Manhattan", 10.0));
            }
            rows.add(RowFactory.create(null, null, null, null, null, null, null, null, null, null));

            Dataset<Row> classified = EventValidator.classify(
                    SparkTestSupport.spark().createDataFrame(rows, EVENT_SCHEMA));

            assertThat(EventValidator.valid(classified).count()).isEqualTo(9);
            assertThat(EventValidator.rejected(classified).count()).isEqualTo(1);
        }
    }
}
