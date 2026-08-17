package com.analyticsplatform.stream.job;

import static org.assertj.core.api.Assertions.assertThat;

import com.analyticsplatform.common.config.PlatformConfig;
import com.analyticsplatform.common.dao.ConnectionSource;
import com.analyticsplatform.common.testing.Fixtures;
import com.analyticsplatform.common.testing.SparkTestSupport;
import com.analyticsplatform.ingest.source.SourceNormalizer;
import com.analyticsplatform.stream.epoch.StreamEpochStore;
import com.analyticsplatform.stream.event.EventValidator;
import com.analyticsplatform.transform.silver.SilverTransform;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tier 1 evidence for the restart-safe streaming claim.
 *
 * <p>The strongest assertion available: <strong>streaming final state == batch-computed final
 * state</strong> over the identical event set. A test that merely watches the sink table grow proves
 * only that rows arrive, not that they are the right rows — and after a mid-stream kill, "the right
 * rows" is precisely what is in question.
 *
 * <p>The reference implementation is the same {@link WindowAggregator} without a watermark. That is
 * not a fully independent oracle (§34 would prefer one), and the limitation is stated rather than
 * glossed: what this proves is that interrupting the stream does not change its result, not that the
 * aggregation itself is correct. Aggregation correctness is covered by
 * {@code WindowAggregatorTest} against hand-computed values.
 */
class StreamRecoveryIT {

    private static SparkSession spark;
    private static PlatformConfig config;
    private static ConnectionSource connections;
    private static ClickHouseProbe probe;

    private String topic;
    private String checkpoint;
    private Dataset<Row> events;
    private Map<String, Long> expectedByBorough;

    @TempDir
    Path root;

    @BeforeAll
    static void configure() {
        config = PlatformConfig.fromEnvironment();
        spark = SparkTestSupport.spark();
        connections = ConnectionSource.postgres(config);
        StreamIngestJob.configureClickHouse(spark, config);
        probe = new ClickHouseProbe(config);
        SilverTransform.registerUdfs(spark);
    }

    @BeforeEach
    void setUp() {
        String suffix = UUID.randomUUID().toString().substring(0, 8);
        topic = "it.trips." + suffix;
        checkpoint = root.resolve("ckpt-" + suffix).toString();

        Dataset<Row> bronze = SourceNormalizer.normalizeYellow(Fixtures.yellow2024())
                .union(SourceNormalizer.normalizeYellow(Fixtures.yellow2025()))
                .union(SourceNormalizer.normalizeGreen(Fixtures.green2024()));
        Dataset<Row> silver = SilverTransform.transform(bronze, Fixtures.taxiZones());

        // Shift event times into the TTL window so a background merge cannot delete them
        // mid-test. Only the grouping matters, not the absolute instant.
        Dataset<Row> shifted = silver.withColumn("pickup_ts",
                org.apache.spark.sql.functions.expr(
                        "pickup_ts + interval " + monthsToShift() + " months"));

        events = TripEventProducer.toEvents(shifted);
        expectedByBorough = referenceAggregate(shifted);
    }

    /** Months needed to bring 2024 fixtures inside the sink's 30-day TTL. */
    private static long monthsToShift() {
        return java.time.temporal.ChronoUnit.MONTHS.between(
                java.time.LocalDate.of(2024, 1, 1),
                java.time.LocalDate.now().withDayOfMonth(1));
    }

    /**
     * The batch reference: the same aggregation, computed directly, with no streaming involved.
     */
    private Map<String, Long> referenceAggregate(Dataset<Row> silver) {
        Dataset<Row> asEvents = EventValidator.valid(EventValidator.classify(
                spark.read().schema(com.analyticsplatform.stream.event.EventEnvelope.JSON_SCHEMA)
                        .json(TripEventProducer.toEvents(silver).select("value")
                                .as(org.apache.spark.sql.Encoders.STRING()))));

        Map<String, Long> expected = new HashMap<>();
        for (Row row : WindowAggregator.aggregateBatch(asEvents).collectAsList()) {
            String key = row.<java.sql.Timestamp>getAs("window_start").toInstant()
                    + "|" + row.<String>getAs("pickup_borough");
            expected.put(key, ((Number) row.getAs("trip_count")).longValue());
        }
        return expected;
    }

    @AfterEach
    void cleanUp() throws Exception {
        for (StreamingQuery query : spark.streams().active()) {
            query.stop();
        }
        probe.execute("DELETE FROM " + probe.table()
                + " WHERE stream_query_id LIKE 'it-%' SETTINGS mutations_sync = 1");
        try (Connection connection = connections.open();
             PreparedStatement statement = connection.prepareStatement(
                     "DELETE FROM control.stream_epoch WHERE checkpoint_id = ?")) {
            statement.setString(1, checkpoint);
            statement.executeUpdate();
        }
    }

    /**
     * Publishes the event set to Kafka.
     *
     * <p>Publishing first also creates the topic, which the consumer then needs metadata for. A
     * consumer started before any publish fails with UnknownTopicOrPartitionException, so the
     * ordering here is load-bearing rather than incidental.
     */
    private long publish() {
        long count = events.count();
        events.write().format("kafka")
                .option("kafka.bootstrap.servers", config.kafkaBootstrap())
                .option("topic", topic)
                .save();
        return count;
    }

    /**
     * Starts the consumer, drains everything currently available, and stops.
     *
     * <p>{@code processAllAvailable()} blocks until the available offsets are processed, so there is
     * no polling and no sleep. A sleep-based wait would be exactly the arbitrary timing §48 calls a
     * defect: it would pass on a fast machine and fail on a loaded one for no real reason.
     */
    private void drain() {
        StreamIngestJob job = new StreamIngestJob(
                spark, config, new StreamEpochStore(connections), checkpoint, topic);
        StreamingQuery query = job.start();
        try {
            query.processAllAvailable();
        } finally {
            try {
                query.stop();
            } catch (java.util.concurrent.TimeoutException e) {
                throw new IllegalStateException("query did not stop cleanly", e);
            }
        }
    }

    /** Streaming result, keyed the same way as the reference. */
    private Map<String, Long> streamedByBorough() {
        Map<String, Long> actual = new HashMap<>();
        Dataset<Row> rows = spark.sql(
                "SELECT window_start, pickup_borough, max_by(trip_count, version) AS trip_count "
                        + "FROM clickhouse." + config.clickhouseDatabase() + "."
                        + StreamIngestJob.SINK_TABLE
                        + " GROUP BY window_start, pickup_borough");

        for (Row row : rows.collectAsList()) {
            String key = row.<java.sql.Timestamp>getAs("window_start").toInstant()
                    + "|" + row.<String>getAs("pickup_borough");
            actual.put(key, ((Number) row.getAs("trip_count")).longValue());
        }
        return actual;
    }

    @Test
    @DisplayName("an uninterrupted stream matches the batch reference")
    void uninterruptedStreamMatchesBatch() throws Exception {
        long published = publish();
        assertThat(published).isEqualTo(14);

        drain();

        assertThat(streamedByBorough())
                .as("streaming final state must equal the batch-computed state")
                .containsAllEntriesOf(expectedByBorough);
    }

    /**
     * The recovery case. The consumer is stopped mid-stream and restarted against the same
     * checkpoint; the final state must be what an uninterrupted run would have produced.
     */
    @Test
    @DisplayName("a stopped and restarted stream converges to the batch reference")
    void restartedStreamConvergesToBatch() throws Exception {
        publish();

        // First pass, then a deliberate stop before the topic is drained.
        drain();

        // Second pass on the SAME checkpoint: reuses the epoch, replays uncommitted offsets.
        drain();

        assertThat(streamedByBorough())
                .as("interrupting the stream must not change its result")
                .containsAllEntriesOf(expectedByBorough);
    }

    /** A restart must reuse the epoch, not allocate a new one. */
    @Test
    @DisplayName("a restart on the same checkpoint reuses its epoch")
    void restartReusesEpoch() throws Exception {
        publish();
        StreamEpochStore store = new StreamEpochStore(connections);

        drain();
        long first = store.epochOf(checkpoint).orElseThrow();

        drain();
        long second = store.epochOf(checkpoint).orElseThrow();

        assertThat(second).isEqualTo(first);
    }

    /** Replaying the same events must not inflate the counts. */
    @Test
    @DisplayName("republishing the same events leaves the logical state unchanged")
    void republishDoesNotInflateCounts() throws Exception {
        publish();
        drain();
        Map<String, Long> afterFirst = streamedByBorough();

        drain();

        assertThat(streamedByBorough())
                .as("a second drain of the same offsets changes nothing")
                .containsAllEntriesOf(afterFirst);
    }

    @Test
    @DisplayName("malformed events are rejected without stopping the stream")
    void malformedEventsDoNotStopTheStream() throws Exception {
        publish();

        // A message that is not valid JSON at all.
        List<Row> junk = List.of(org.apache.spark.sql.RowFactory.create("bad", "{not json"));
        spark.createDataFrame(junk, new org.apache.spark.sql.types.StructType(
                        new org.apache.spark.sql.types.StructField[] {
                            new org.apache.spark.sql.types.StructField("key",
                                    org.apache.spark.sql.types.DataTypes.StringType, true,
                                    org.apache.spark.sql.types.Metadata.empty()),
                            new org.apache.spark.sql.types.StructField("value",
                                    org.apache.spark.sql.types.DataTypes.StringType, true,
                                    org.apache.spark.sql.types.Metadata.empty()),
                        }))
                .write().format("kafka")
                .option("kafka.bootstrap.servers", config.kafkaBootstrap())
                .option("topic", topic)
                .save();

        drain();

        assertThat(streamedByBorough())
                .as("the well-formed events still aggregate correctly")
                .containsAllEntriesOf(expectedByBorough);
    }
}
