package com.analyticsplatform.stream.job;

import static org.assertj.core.api.Assertions.assertThat;

import com.analyticsplatform.common.config.PlatformConfig;
import com.analyticsplatform.common.dao.ConnectionSource;
import com.analyticsplatform.common.testing.Fixtures;
import com.analyticsplatform.common.testing.SparkTestSupport;
import com.analyticsplatform.ingest.source.SourceNormalizer;
import com.analyticsplatform.stream.epoch.StreamEpochStore;
import com.analyticsplatform.transform.silver.SilverTransform;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

/**
 * §46 · A genuinely long-running streaming soak.
 *
 * <p>Separate from {@code StreamRecoveryIT} because that suite runs a few batches and exits — as a
 * soak it produced a <em>vacuous pass</em>: the harness reported "no unbounded growth observed"
 * after the consumer had been alive for fifteen seconds. A stream that is correct for thirty
 * seconds and leaks for six hours passes every other suite in this project.
 *
 * <p>This one keeps a query alive for the configured duration, publishing continuously, so the
 * measurements describe a running system rather than a startup.
 *
 * <p>Gated on {@code SOAK_MINUTES} so it never runs as part of an ordinary suite.
 */
@EnabledIfEnvironmentVariable(named = "SOAK_MINUTES", matches = "\\d+")
class SoakIT {

    private static SparkSession spark;
    private static PlatformConfig config;
    private static ConnectionSource connections;
    private static ClickHouseProbe probe;

    private static String topic;
    private static String checkpoint;
    private static Path work;

    @BeforeAll
    static void setUp() throws Exception {
        config = PlatformConfig.fromEnvironment();
        spark = SparkTestSupport.spark();
        connections = ConnectionSource.postgres(config);
        StreamIngestJob.configureClickHouse(spark, config);
        probe = new ClickHouseProbe(config);
        SilverTransform.registerUdfs(spark);

        topic = System.getenv().getOrDefault("SOAK_TOPIC", "soak.trips." + UUID.randomUUID());
        work = Files.createTempDirectory("soak-");
        checkpoint = work.resolve("ckpt").toString();
    }

    @AfterAll
    static void cleanUp() throws Exception {
        for (StreamingQuery query : spark.streams().active()) {
            query.stop();
        }
        probe.execute("DELETE FROM " + probe.table()
                + " WHERE pickup_borough LIKE 'SOAK-%' SETTINGS mutations_sync = 1");
        try (Connection connection = connections.open();
             PreparedStatement s = connection.prepareStatement(
                     "DELETE FROM control.stream_epoch WHERE checkpoint_id = ?")) {
            s.setString(1, checkpoint);
            s.executeUpdate();
        }
    }

    /** Events with a borough tag unique to the soak, so cleanup cannot touch other suites. */
    private static Dataset<Row> events(int wave) {
        Dataset<Row> bronze = SourceNormalizer.normalizeYellow(Fixtures.yellow2024())
                .union(SourceNormalizer.normalizeGreen(Fixtures.green2024()));
        Dataset<Row> silver = SilverTransform.transform(bronze, Fixtures.taxiZones());

        // Each wave lands in its own window and its own borough, so state genuinely accumulates
        // rather than every event replacing the same row - which is what makes unbounded growth
        // observable at all.
        return TripEventProducer.toEvents(silver
                .withColumn("pickup_ts", org.apache.spark.sql.functions.expr(
                        "current_timestamp() + interval " + (wave * 5) + " minutes"))
                .withColumn("pickup_borough", org.apache.spark.sql.functions.lit(
                        "SOAK-" + (wave % 7))));
    }

    @Test
    @DisplayName("the stream stays healthy under sustained load")
    void soak() throws Exception {
        int minutes = Integer.parseInt(System.getenv("SOAK_MINUTES"));
        Instant deadline = Instant.now().plus(Duration.ofMinutes(minutes));

        // Seed the topic before the consumer subscribes, or it fails on unknown metadata.
        events(0).write().format("kafka")
                .option("kafka.bootstrap.servers", config.kafkaBootstrap())
                .option("topic", topic).save();

        StreamIngestJob job = new StreamIngestJob(
                spark, config, new StreamEpochStore(connections), checkpoint, topic);
        StreamingQuery query = job.start();

        long firstHeap = usedHeapMb();
        int wave = 1;
        long batches = 0;

        try {
            while (Instant.now().isBefore(deadline)) {
                events(wave++).write().format("kafka")
                        .option("kafka.bootstrap.servers", config.kafkaBootstrap())
                        .option("topic", topic).save();

                query.processAllAvailable();
                batches++;

                assertThat(query.isActive())
                        .as("the query must survive the whole soak, not just its first batches")
                        .isTrue();
                assertThat(query.exception().isEmpty())
                        .as("no streaming exception during the soak").isTrue();
            }
        } finally {
            query.stop();
        }

        long lastHeap = usedHeapMb();
        long distinctWindows = probe.queryLong(
                "SELECT count() FROM (SELECT DISTINCT window_start, pickup_borough FROM "
                        + probe.table() + " WHERE pickup_borough LIKE 'SOAK-%')");
        long physical = probe.queryLong("SELECT count() FROM " + probe.table()
                + " WHERE pickup_borough LIKE 'SOAK-%'");

        System.out.printf("%n  soak: %d min, %d waves, heap %dMB -> %dMB, "
                        + "%d physical rows over %d distinct windows%n",
                minutes, batches, firstHeap, lastHeap, physical, distinctWindows);

        assertThat(batches).as("the soak must have actually driven batches").isGreaterThan(1);
        assertThat(distinctWindows).as("windows were produced").isPositive();

        // Physical rows may exceed distinct windows before a merge; what must not happen is
        // unbounded divergence, which would mean the sink is not converging at all.
        assertThat(physical).as("physical rows must not run away from logical windows")
                .isLessThan(distinctWindows * 20 + 100);
    }

    private static long usedHeapMb() {
        Runtime runtime = Runtime.getRuntime();
        return (runtime.totalMemory() - runtime.freeMemory()) / (1024 * 1024);
    }
}
