package com.analyticsplatform.stream.job;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.from_json;
import static org.apache.spark.sql.functions.lit;

import com.analyticsplatform.common.config.PlatformConfig;
import com.analyticsplatform.stream.epoch.StreamEpochStore;
import com.analyticsplatform.stream.epoch.StreamEpochStore.Allocation;
import com.analyticsplatform.stream.event.EventEnvelope;
import com.analyticsplatform.stream.event.EventValidator;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Kafka to ClickHouse: windowed trip aggregates.
 *
 * <h2>Delivery semantics, stated accurately</h2>
 *
 * <p>This is <strong>at-least-once delivery into an idempotent sink that converges
 * deterministically</strong> — not exactly-once. A crash between ClickHouse accepting a batch and
 * Spark committing checkpoint progress redelivers that batch. Claiming exactly-once would be a
 * claim about a transaction boundary that does not exist across these two systems.
 *
 * <p>What makes redelivery harmless is the pair of properties below, and both are tested:
 *
 * <ol>
 *   <li>the {@code foreachBatch} body is a pure function of its input, so a replayed batch produces
 *       byte-identical rows;
 *   <li>the sink is a {@code ReplacingMergeTree} keyed on the window with a monotonic version, so
 *       identical rows collapse rather than accumulate.
 * </ol>
 *
 * <p>Physical duplicates <em>may</em> transiently exist before ClickHouse merges. That is not
 * claimed away.
 *
 * <h2>Reading this table correctly</h2>
 *
 * <p><strong>A deduplicated read requires a ClickHouse-native client.</strong> Spark cannot express
 * {@code FINAL} — its parser has no such modifier, so {@code SELECT ... FROM t FINAL} parses
 * {@code FINAL} as a table <em>alias</em>, runs against the raw table, and silently returns
 * duplicates with no error at all. A consumer querying this table through Spark must instead use
 * {@code max_by(value, version)} grouped on the window key. Either is correct; reading it plainly
 * through Spark is not.
 */
public final class StreamIngestJob {

    private static final Logger log = LoggerFactory.getLogger(StreamIngestJob.class);

    public static final String DEFAULT_TOPIC = "taxi.trips.raw";
    public static final String SINK_TABLE = "stream_trip_window";

    private final SparkSession spark;
    private final PlatformConfig config;
    private final StreamEpochStore epochStore;
    private final String checkpointLocation;

    /**
     * Which topic to consume.
     *
     * <p>Injectable so an integration test can use its own topic instead of the shared production
     * one. Hardcoding it meant the recovery suite subscribed to a topic it had never published to
     * and failed with UnknownTopicOrPartitionException — a confusing symptom for a wiring mistake.
     */
    private final String topic;

    /** Observed batch count, so tests can assert progress without scraping logs. */
    private final AtomicLong batchesProcessed = new AtomicLong();

    public StreamIngestJob(
            SparkSession spark,
            PlatformConfig config,
            StreamEpochStore epochStore,
            String checkpointLocation,
            String topic) {
        this.topic = topic;
        this.spark = spark;
        this.config = config;
        this.epochStore = epochStore;
        this.checkpointLocation = checkpointLocation;
    }

    /** Parses and validates the Kafka value column into typed events. */
    public static Dataset<Row> parse(Dataset<Row> kafka) {
        Dataset<Row> parsed = kafka
                .select(from_json(col("value").cast("string"), EventEnvelope.JSON_SCHEMA)
                        .alias("event"))
                .select("event.*");

        return EventValidator.classify(parsed);
    }

    /**
     * Starts the query.
     *
     * <p>The epoch is allocated once, before the query starts, and captured by the sink closure. It
     * must not be re-read per batch: a re-read during a checkpoint reset would change the version
     * mid-flight and break the ordering the sink depends on.
     */
    public StreamingQuery start() {
        Allocation allocation = epochStore.allocate(checkpointLocation, "pending");
        log.info("stream epoch {} for checkpoint {} (fresh={})",
                allocation.epoch(), checkpointLocation, allocation.fresh());

        Dataset<Row> kafka = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", config.kafkaBootstrap())
                .option("subscribe", topic)
                .option("startingOffsets", "earliest")
                // One bad message must not stop the stream; the validator classifies it instead.
                .option("failOnDataLoss", "false")
                .load();

        Dataset<Row> valid = EventValidator.valid(parse(kafka));
        Dataset<Row> windowed = WindowAggregator.aggregate(valid, true);

        try {
            return windowed.writeStream()
                    .outputMode(OutputMode.Update())
                    .option("checkpointLocation", checkpointLocation)
                    .trigger(Trigger.ProcessingTime(2000))
                    .foreachBatch((batch, batchId) -> {
                        writeBatch(batch, batchId, allocation);
                        batchesProcessed.incrementAndGet();
                    })
                    .start();
        } catch (java.util.concurrent.TimeoutException e) {
            // start() times out when a query is already running against this checkpoint. That is a
            // deployment mistake — two consumers on one checkpoint — not a transient fault, so it
            // must not look like something a retry would fix.
            throw new IllegalStateException(
                    "a streaming query is already active for checkpoint " + checkpointLocation, e);
        }
    }

    /**
     * Writes one microbatch.
     *
     * <p>Deliberately a pure function of {@code (batch, batchId, allocation)}. No processing-time
     * value appears anywhere: {@code version} comes from the epoch and batch id, both of which are
     * replayed identically from the checkpoint. That is what makes a redelivered batch produce
     * byte-identical rows instead of merely similar ones.
     */
    void writeBatch(Dataset<Row> batch, long batchId, Allocation allocation) {
        Dataset<Row> stamped = batch
                .withColumn("stream_query_id", lit(allocation.streamQueryId()))
                .withColumn("batch_id", lit(batchId))
                .withColumn("stream_epoch", lit(allocation.epoch()))
                .withColumn("version", lit(allocation.versionFor(batchId)));

        String qualified = "clickhouse." + config.clickhouseDatabase() + "." + SINK_TABLE;
        try {
            stamped.select(
                    col("window_start"), col("window_end"), col("pickup_borough"),
                    col("trip_count"), col("total_revenue"), col("avg_fare"),
                    col("avg_distance_mi"), col("stream_query_id"), col("batch_id"),
                    col("stream_epoch"), col("version"))
                    .writeTo(qualified).append();
        } catch (org.apache.spark.sql.catalyst.analysis.NoSuchTableException e) {
            throw new IllegalStateException(
                    "ClickHouse table " + qualified + " does not exist; apply "
                            + "docker/clickhouse/init/01_marts.sql", e);
        }
    }

    public long batchesProcessed() {
        return batchesProcessed.get();
    }

    /** Configures the ClickHouse catalog this job writes through. */
    public static void configureClickHouse(SparkSession spark, PlatformConfig config) {
        String prefix = "spark.sql.catalog.clickhouse";
        spark.conf().set(prefix, "com.clickhouse.spark.ClickHouseCatalog");
        spark.conf().set(prefix + ".host", config.clickhouseHost());
        spark.conf().set(prefix + ".protocol", "http");
        spark.conf().set(prefix + ".http_port", String.valueOf(config.clickhouseHttpPort()));
        spark.conf().set(prefix + ".user", config.clickhouseUser());
        spark.conf().set(prefix + ".password", config.clickhousePassword());
        spark.conf().set(prefix + ".database", config.clickhouseDatabase());
    }
}
