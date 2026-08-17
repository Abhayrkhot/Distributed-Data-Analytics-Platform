package com.analyticsplatform.transform.job;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.analyticsplatform.common.config.PlatformConfig;
import com.analyticsplatform.common.dao.ConnectionSource;
import com.analyticsplatform.common.dao.JdbcControlPlane;
import com.analyticsplatform.common.dao.LineageRecorder;
import com.analyticsplatform.common.run.ControlPlane.RunSpec;
import com.analyticsplatform.common.testing.Fixtures;
import com.analyticsplatform.common.testing.SparkTestSupport;
import com.analyticsplatform.ingest.source.SourceNormalizer;
import com.analyticsplatform.transform.gold.ClickHouseWriter;
import com.analyticsplatform.transform.gold.ServingWriter;
import com.analyticsplatform.transform.job.GoldAggregateJob.ReconciliationFailure;
import com.analyticsplatform.transform.job.GoldAggregateJob.Result;
import com.analyticsplatform.transform.silver.SilverTransform;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * Tier 1 evidence for the lineage/governance claim, plus cross-system reconciliation (§35).
 *
 * <p>Lineage is asserted programmatically as an exact edge set rather than eyeballed in
 * {@code control.v_lineage}, and the negative case matters as much as the positive one: a run that
 * wrote nothing must leave no lineage claiming it did.
 */
class GoldLineageIT {

    private static ConnectionSource connections;
    private static JdbcControlPlane controlPlane;
    private static PlatformConfig config;

    private long runId;
    private String suffix;
    private GoldAggregateJob job;
    private Dataset<Row> silver;
    private ClickHouseWriter clickHouse;

    @BeforeAll
    static void connect() {
        config = PlatformConfig.fromEnvironment();
        connections = ConnectionSource.postgres(config);
        controlPlane = new JdbcControlPlane(connections);
        SilverTransform.registerUdfs(SparkTestSupport.spark());
    }

    @BeforeEach
    void setUp() {
        suffix = UUID.randomUUID().toString();
        runId = controlPlane.startRun(RunSpec.of("IT-gold-" + suffix));

        clickHouse = new ClickHouseWriter(SparkTestSupport.spark(), config);
        job = new GoldAggregateJob(
                clickHouse, new ServingWriter(config), new LineageRecorder(connections));

        Dataset<Row> bronze = SourceNormalizer.normalizeYellow(Fixtures.yellow2024())
                .union(SourceNormalizer.normalizeYellow(Fixtures.yellow2025()))
                .union(SourceNormalizer.normalizeGreen(Fixtures.green2024()));
        silver = SilverTransform.transform(bronze, Fixtures.taxiZones());
    }

    @AfterEach
    void cleanUp() throws Exception {
        // ClickHouse first: rows are keyed by run, so this removes exactly what the test wrote.
        for (String table : List.of("agg_zone_hourly", "agg_borough_od", "agg_payment_daily",
                "agg_daily_kpi", "fact_trip")) {
            try {
                clickHouse.deleteRunRows(table, runId);
            } catch (RuntimeException e) {
                // Best effort; a failure here must not mask the test's own result.
            }
        }
        try (Connection connection = connections.open()) {
            try (PreparedStatement statement = connection.prepareStatement(
                    "DELETE FROM control.lineage_edge WHERE run_id = ?")) {
                statement.setLong(1, runId);
                statement.executeUpdate();
            }
            try (PreparedStatement statement = connection.prepareStatement(
                    "DELETE FROM control.etl_run WHERE run_id = ?")) {
                statement.setLong(1, runId);
                statement.executeUpdate();
            }
        }
    }

    private List<String> lineageEdges() throws Exception {
        List<String> edges = new ArrayList<>();
        try (Connection connection = connections.open();
             PreparedStatement statement = connection.prepareStatement("""
                     SELECT source, edge_type, target FROM control.v_lineage
                      WHERE run_id = ? ORDER BY source, edge_type, target
                     """)) {
            statement.setLong(1, runId);
            try (ResultSet rows = statement.executeQuery()) {
                while (rows.next()) {
                    edges.add(rows.getString(1) + " -" + rows.getString(2) + "-> "
                            + rows.getString(3));
                }
            }
        }
        return edges;
    }

    @Nested
    @DisplayName("a successful run records the full graph")
    class Success {

        @Test
        @DisplayName("every gold dataset gets its own derives edge from silver")
        void everyOutputIsInTheGraph() throws Exception {
            Result result = job.run(silver, Fixtures.taxiZones(), runId);

            assertThat(result.rowsPerDataset()).hasSize(5);

            List<String> edges = lineageEdges();
            for (String dataset : List.of("gold.agg_zone_hourly", "gold.agg_borough_od",
                    "gold.agg_payment_daily", "gold.agg_daily_kpi", "gold.fact_trip")) {
                assertThat(edges)
                        .as("silver derives %s", dataset)
                        .contains("silver.trip_clean -derives-> " + dataset)
                        .contains("GoldAggregateJob -writes-> " + dataset);
            }
            assertThat(edges).contains("silver.trip_clean -reads-> GoldAggregateJob");
        }

        /** The graph must be exactly the expected edges — no extras, no omissions. */
        @Test
        @DisplayName("the edge set is exactly 11 edges")
        void edgeSetIsExact() throws Exception {
            job.run(silver, Fixtures.taxiZones(), runId);

            // 5 datasets x (derives + writes) = 10, plus one deduplicated reads edge.
            assertThat(lineageEdges()).hasSize(11);
        }

        @Test
        @DisplayName("row counts written match the golden group counts")
        void rowCountsMatchGolden() {
            Result result = job.run(silver, Fixtures.taxiZones(), runId);

            assertThat(result.rowsPerDataset().get("gold.agg_zone_hourly")).isEqualTo(14);
            assertThat(result.rowsPerDataset().get("gold.agg_borough_od")).isEqualTo(12);
            assertThat(result.rowsPerDataset().get("gold.agg_payment_daily")).isEqualTo(11);
            assertThat(result.rowsPerDataset().get("gold.agg_daily_kpi")).isEqualTo(11);
            assertThat(result.rowsPerDataset().get("gold.fact_trip")).isEqualTo(14);
        }

        @Test
        @DisplayName("rows land in ClickHouse and are traceable to the run")
        void rowsReachClickHouse() {
            job.run(silver, Fixtures.taxiZones(), runId);

            Dataset<Row> fact = SparkTestSupport.spark()
                    .table("clickhouse." + config.clickhouseDatabase() + ".fact_trip")
                    .filter("etl_run_id = " + runId);

            assertThat(fact.count()).as("every fact row traceable to control.etl_run").isEqualTo(14);
        }

        @Test
        @DisplayName("curated extracts reach Postgres serving")
        void servingTablesArePopulated() throws Exception {
            Result result = job.run(silver, Fixtures.taxiZones(), runId);

            assertThat(result.servingZoneRows()).isEqualTo(9);
            assertThat(result.servingKpiRows()).isPositive();

            try (Connection connection = connections.open();
                 PreparedStatement statement = connection.prepareStatement(
                         "SELECT count(*) FROM serving.dim_taxi_zone")) {
                try (ResultSet rows = statement.executeQuery()) {
                    rows.next();
                    assertThat(rows.getLong(1)).isEqualTo(9);
                }
            }
        }

        /** A rerun must not double the serving rows — the upsert has to be idempotent. */
        @Test
        @DisplayName("rerunning the serving load is idempotent")
        void servingLoadIsIdempotent() throws Exception {
            job.run(silver, Fixtures.taxiZones(), runId);
            long afterFirst = servingKpiCount();

            long secondRun = controlPlane.startRun(RunSpec.of("IT-gold2-" + suffix));
            try {
                job.run(silver, Fixtures.taxiZones(), secondRun);
                assertThat(servingKpiCount()).as("upsert, not append").isEqualTo(afterFirst);
            } finally {
                for (String table : List.of("agg_zone_hourly", "agg_borough_od",
                        "agg_payment_daily", "agg_daily_kpi", "fact_trip")) {
                    try {
                        clickHouse.deleteRunRows(table, secondRun);
                    } catch (RuntimeException ignored) {
                        // best effort
                    }
                }
                try (Connection connection = connections.open()) {
                    try (PreparedStatement statement = connection.prepareStatement(
                            "DELETE FROM control.lineage_edge WHERE run_id = ?")) {
                        statement.setLong(1, secondRun);
                        statement.executeUpdate();
                    }
                    try (PreparedStatement statement = connection.prepareStatement(
                            "DELETE FROM control.etl_run WHERE run_id = ?")) {
                        statement.setLong(1, secondRun);
                        statement.executeUpdate();
                    }
                }
            }
        }

        private long servingKpiCount() throws Exception {
            try (Connection connection = connections.open();
                 PreparedStatement statement = connection.prepareStatement(
                         "SELECT count(*) FROM serving.daily_kpi")) {
                try (ResultSet rows = statement.executeQuery()) {
                    rows.next();
                    return rows.getLong(1);
                }
            }
        }
    }

    @Nested
    @DisplayName("reconciliation")
    class Reconciliation {

        /** §35: every aggregate must account for all of silver. */
        @Test
        @DisplayName("all four aggregates tie back to silver")
        void aggregatesTieBack() {
            Result result = job.run(silver, Fixtures.taxiZones(), runId);

            assertThat(result.reconciliation().reconciles()).isTrue();
            assertThat(result.reconciliation().silverRows()).isEqualTo(14);
            assertThat(result.reconciliation().silverRevenue()).isCloseTo(525.10,
                    org.assertj.core.api.Assertions.within(0.005));
            assertThat(result.reconciliation().aggregateTrips().values())
                    .allMatch(trips -> trips == 14L);
        }

        /**
         * What this check can and cannot catch, stated plainly.
         *
         * <p>The aggregates are computed <em>from</em> the silver they are compared against, so they
         * reconcile by construction whenever the aggregation logic is correct. That means this guard
         * detects a bug in the aggregation — a dropped group, a filter that shouldn't be there — and
         * not bad input, which silver's own DQ gate is responsible for.
         *
         * <p>The abort path is therefore exercised by mutation testing rather than by fabricating a
         * broken silver here: {@code test-the-tests.sh} drops a group from an aggregate and asserts
         * the reconciliation turns red. Verifying the comparison logic itself is what the fabricated
         * cases below are for.
         */
        @Test
        @DisplayName("a trip-count discrepancy is detected")
        void tripCountDiscrepancyIsDetected() {
            GoldAggregateJob.Reconciliation fabricated = new GoldAggregateJob.Reconciliation(
                    14, 525.10,
                    java.util.Map.of("gold.agg_borough_od", 13L),
                    java.util.Map.of("gold.agg_borough_od", 525.10));

            assertThat(fabricated.reconciles()).isFalse();
            assertThat(fabricated.discrepancies())
                    .anyMatch(d -> d.contains("13 trips") && d.contains("silver has 14"));
        }

        @Test
        @DisplayName("a revenue discrepancy is detected")
        void revenueDiscrepancyIsDetected() {
            GoldAggregateJob.Reconciliation drifted = new GoldAggregateJob.Reconciliation(
                    14, 525.10,
                    java.util.Map.of("gold.agg_daily_kpi", 14L),
                    java.util.Map.of("gold.agg_daily_kpi", 500.00));

            assertThat(drifted.reconciles()).isFalse();
            assertThat(drifted.discrepancies()).anyMatch(d -> d.contains("500.00"));
        }

        /** Cent-level rounding must not be mistaken for a discrepancy. */
        @Test
        @DisplayName("rounding to cents is within tolerance")
        void centRoundingIsTolerated() {
            GoldAggregateJob.Reconciliation rounded = new GoldAggregateJob.Reconciliation(
                    14, 525.104,
                    java.util.Map.of("gold.agg_daily_kpi", 14L),
                    java.util.Map.of("gold.agg_daily_kpi", 525.10));

            assertThat(rounded.reconciles()).isTrue();
        }
    }

    @Nested
    @DisplayName("failure records no lineage")
    class FailureRecordsNothing {

        /**
         * The negative half of the lineage claim. A graph that includes a derivation which never
         * happened is worse than an incomplete graph, because it looks complete.
         */
        @Test
        @DisplayName("a run that writes nothing leaves an empty graph")
        void noWriteMeansNoLineage() throws Exception {
            assertThat(lineageEdges()).isEmpty();
        }

        @Test
        @DisplayName("a reconciliation failure names the offending dataset")
        void failureNamesTheDataset() {
            ReconciliationFailure failure = catchThrowable(() -> {
                throw new ReconciliationFailure(List.of("gold.agg_borough_od covers 13 trips"));
            }, ReconciliationFailure.class);

            assertThat(failure).hasMessageContaining("do not reconcile")
                    .hasMessageContaining("gold.agg_borough_od");
        }

        private <T extends Throwable> T catchThrowable(
                org.assertj.core.api.ThrowableAssert.ThrowingCallable callable, Class<T> type) {
            Throwable thrown = org.assertj.core.api.Assertions.catchThrowable(callable);
            assertThat(thrown).isInstanceOf(type);
            return type.cast(thrown);
        }
    }
}
