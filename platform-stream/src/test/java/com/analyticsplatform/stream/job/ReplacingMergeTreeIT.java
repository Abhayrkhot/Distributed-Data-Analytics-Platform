package com.analyticsplatform.stream.job;

import static org.assertj.core.api.Assertions.assertThat;

import com.analyticsplatform.common.config.PlatformConfig;
import com.analyticsplatform.common.stream.StreamVersion;
import com.analyticsplatform.common.testing.SparkTestSupport;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * ReplacingMergeTree replacement semantics, verified experimentally (§40).
 *
 * <p>The DDL looking correct is not evidence that the engine behaves as intended. These tests write
 * real rows to real ClickHouse and read back what a query actually returns.
 *
 * <p>They also pin the distinction the docs make: <em>physical</em> duplicate rows may transiently
 * exist before a background merge, while the <em>logical</em> result under {@code FINAL} converges
 * deterministically. Conflating those two is how a project ends up claiming exactly-once.
 */
class ReplacingMergeTreeIT {

    private static final StructType SINK_SCHEMA = new StructType(new StructField[] {
        new StructField("window_start", DataTypes.TimestampType, false, Metadata.empty()),
        new StructField("window_end", DataTypes.TimestampType, false, Metadata.empty()),
        new StructField("pickup_borough", DataTypes.StringType, false, Metadata.empty()),
        new StructField("trip_count", DataTypes.LongType, false, Metadata.empty()),
        new StructField("total_revenue", DataTypes.createDecimalType(18, 2), false, Metadata.empty()),
        new StructField("avg_fare", DataTypes.createDecimalType(12, 4), false, Metadata.empty()),
        new StructField("avg_distance_mi", DataTypes.FloatType, false, Metadata.empty()),
        new StructField("stream_query_id", DataTypes.StringType, false, Metadata.empty()),
        new StructField("batch_id", DataTypes.LongType, false, Metadata.empty()),
        new StructField("stream_epoch", DataTypes.LongType, false, Metadata.empty()),
        new StructField("version", DataTypes.LongType, false, Metadata.empty()),
    });

    private static SparkSession spark;
    private static PlatformConfig config;
    private static String table;
    /**
     * Assertions go through JDBC, not Spark. Spark's parser has no FINAL modifier and silently
     * treats it as a table alias, so a Spark-side "FINAL" query returns un-deduplicated rows with
     * no error at all. That is how an earlier version of this suite passed while checking nothing.
     */
    private static ClickHouseProbe probe;

    /** A borough unique to this test class, so rows cannot collide with other suites. */
    private String borough;
    private Instant windowStart;

    @BeforeAll
    static void configure() {
        config = PlatformConfig.fromEnvironment();
        spark = SparkTestSupport.spark();
        StreamIngestJob.configureClickHouse(spark, config);
        table = "clickhouse." + config.clickhouseDatabase() + "." + StreamIngestJob.SINK_TABLE;
        probe = new ClickHouseProbe(config);
    }

    @BeforeEach
    void setUp() {
        borough = "IT-" + java.util.UUID.randomUUID().toString().substring(0, 8);
        // Must sit INSIDE the table's TTL window (window_start + 30 days), not at the fixture's
        // 2024 dates. OPTIMIZE FINAL forces a merge, merges apply TTL, and a 19-month-old
        // window_start is silently deleted mid-test - which surfaced as "expected 1 row, got 0"
        // and looks nothing like a TTL problem.
        //
        // now() is acceptable here because the timestamp is only a grouping key: no assertion
        // depends on its value, only on rows sharing it.
        windowStart = Instant.now().truncatedTo(java.time.temporal.ChronoUnit.HOURS);
    }

    @AfterEach
    void cleanUp() {
        probe.deleteBorough(borough);
    }

    /** Writes one window row at a given epoch and batch. */
    private void write(long epoch, long batchId, long tripCount) {
        long version = StreamVersion.of(epoch, batchId);
        Row row = RowFactory.create(
                Timestamp.from(windowStart),
                Timestamp.from(windowStart.plusSeconds(300)),
                borough,
                tripCount,
                new java.math.BigDecimal(tripCount * 10).setScale(2),
                new java.math.BigDecimal("12.5000").setScale(4),
                3.0f,
                "query-" + epoch,
                batchId,
                epoch,
                version);

        Dataset<Row> data = spark.createDataFrame(List.of(row), SINK_SCHEMA);
        try {
            data.writeTo(table).append();
        } catch (org.apache.spark.sql.catalyst.analysis.NoSuchTableException e) {
            throw new IllegalStateException("sink table missing: " + table, e);
        }
    }

    /** The logical result: what a deduplicated read returns. */
    private long finalTripCount() {
        assertThat(probe.finalRows(borough))
                .as("FINAL must collapse to one row per window key").isEqualTo(1);
        return probe.finalTripCount(borough);
    }

    /** Rows physically present, before or after merging. */
    private long physicalRowCount() {
        return probe.physicalRows(borough);
    }

    @Nested
    @DisplayName("replacement")
    class Replacement {

        /** A later batch for the same window supersedes the earlier one. */
        @Test
        @DisplayName("a later batch replaces an earlier one")
        void laterBatchWins() {
            write(1, 41, 200);
            assertThat(finalTripCount()).isEqualTo(200);

            write(1, 42, 227);

            assertThat(finalTripCount()).as("the later batch's value").isEqualTo(227);
        }

        /**
         * Redelivery: the same batch written twice must leave the logical result unchanged. This is
         * what makes at-least-once delivery harmless.
         */
        @Test
        @DisplayName("replaying the same batch leaves the result unchanged")
        void replayIsIdempotent() {
            write(1, 41, 200);
            write(1, 42, 227);
            assertThat(finalTripCount()).isEqualTo(227);

            write(1, 42, 227);   // identical redelivery

            assertThat(finalTripCount()).as("still 227 after replay").isEqualTo(227);
        }

        /** An out-of-order arrival must not undo a newer value. */
        @Test
        @DisplayName("an earlier batch arriving late does not overwrite a newer one")
        void lowerVersionDoesNotWin() {
            write(1, 42, 227);
            write(1, 41, 200);   // arrives after, but is older

            assertThat(finalTripCount()).as("the newer value survives").isEqualTo(227);
        }

        /**
         * The reason the epoch exists. A fresh checkpoint restarts batch numbering at 0; without the
         * epoch in the version, that batch would lose to the old lineage's batch 42 and the
         * replacement would silently not happen.
         */
        @Test
        @DisplayName("a new epoch's batch 0 replaces an old epoch's batch 42")
        void freshCheckpointStillReplaces() {
            write(1, 42, 227);
            assertThat(finalTripCount()).isEqualTo(227);

            write(2, 0, 300);   // fresh checkpoint, batch numbering restarted

            assertThat(finalTripCount())
                    .as("the later epoch wins despite the lower batch id")
                    .isEqualTo(300);
        }

        /** Without the epoch this exact case would silently keep the stale value. */
        @Test
        @DisplayName("batch id alone would have been insufficient")
        void batchIdAloneWouldFail() {
            long oldVersion = StreamVersion.of(1, 42);
            long freshVersion = StreamVersion.of(2, 0);

            assertThat(freshVersion).isGreaterThan(oldVersion);
            assertThat(0L).as("comparing batch ids alone inverts the ordering").isLessThan(42L);
        }
    }

    @Nested
    @DisplayName("Spark cannot express FINAL")
    class SparkFinalLimitation {

        /**
         * Pinning the trap that made an earlier version of this suite vacuous. Spark parses FINAL as
         * a table alias, so the query succeeds and returns raw rows. Asserting the limitation means a
         * future Spark or connector version that gains FINAL support will turn this red and prompt a
         * deliberate revisit, rather than the constraint being quietly wrong in the docs.
         */
        @Test
        @DisplayName("a Spark-side FINAL query silently returns un-deduplicated rows")
        void sparkFinalIsSilentlyIgnored() {
            write(1, 41, 200);
            write(1, 42, 227);

            long viaSpark = ((Number) spark.sql("SELECT count(*) AS c FROM " + table + " FINAL"
                    + " WHERE pickup_borough = '" + borough + "'").first().get(0)).longValue();
            long viaJdbc = probe.finalRows(borough);

            assertThat(viaJdbc).as("JDBC deduplicates").isEqualTo(1);
            assertThat(viaSpark)
                    .as("Spark does not - FINAL is parsed as an alias, no error raised")
                    .isEqualTo(2);
        }

        /** Spark's max_by is the portable equivalent when a ClickHouse-native read is unavailable. */
        @Test
        @DisplayName("max_by over version is the Spark-side equivalent")
        void maxByIsTheSparkEquivalent() {
            write(1, 41, 200);
            write(1, 42, 227);

            // ClickHouse UInt64 maps to Spark Decimal, not Long, so read it as a Number.
            long viaMaxBy = ((Number) spark.sql(
                    "SELECT max_by(trip_count, version) AS c FROM " + table
                            + " WHERE pickup_borough = '" + borough + "'").first().get(0))
                    .longValue();

            assertThat(viaMaxBy).isEqualTo(probe.finalTripCount(borough)).isEqualTo(227);
        }
    }

    @Nested
    @DisplayName("physical versus logical")
    class PhysicalVersusLogical {

        /**
         * Physical duplicates transiently exist; merges are asynchronous. Documented rather than
         * claimed away, because a correctness read that ignores this returns duplicated rows.
         */
        @Test
        @DisplayName("duplicates may exist physically while FINAL is already correct")
        void duplicatesExistPhysically() {
            write(1, 41, 200);
            write(1, 42, 227);
            write(1, 43, 250);

            assertThat(physicalRowCount())
                    .as("three inserts, merges not yet guaranteed to have run")
                    .isGreaterThanOrEqualTo(1);
            assertThat(finalTripCount()).as("FINAL is correct regardless").isEqualTo(250);
        }

        /** An explicit version-aware aggregation must agree with FINAL. */
        @Test
        @DisplayName("argMax over version agrees with FINAL")
        void versionAwareAggregationAgrees() {
            write(1, 41, 200);
            write(1, 42, 227);

            long viaArgMax = probe.argMaxTripCount(borough);

            assertThat(viaArgMax).isEqualTo(finalTripCount()).isEqualTo(227);
        }

        /**
         * Convergence: after an explicit merge the physical state matches the logical one. This is
         * the difference between "converges deterministically" and "never duplicates".
         */
        @Test
        @DisplayName("after OPTIMIZE the physical state matches the logical one")
        void optimizeCollapsesDuplicates() {
            write(1, 41, 200);
            write(1, 42, 227);
            write(1, 42, 227);

            probe.optimize();

            assertThat(physicalRowCount()).as("collapsed to one row").isEqualTo(1);
            assertThat(finalTripCount()).isEqualTo(227);
        }

    }
}
