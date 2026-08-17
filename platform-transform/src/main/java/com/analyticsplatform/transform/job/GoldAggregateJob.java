package com.analyticsplatform.transform.job;

import com.analyticsplatform.common.dao.LineageRecorder;
import com.analyticsplatform.transform.gold.ClickHouseWriter;
import com.analyticsplatform.transform.gold.GoldAggregates;
import com.analyticsplatform.transform.gold.ServingWriter;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Silver to gold: four aggregates plus the trip fact into ClickHouse, curated extracts into Postgres.
 *
 * <h2>Reconciliation is part of the job, not a separate report</h2>
 *
 * <p>Every aggregate is checked against silver before anything is written. An aggregate that dropped
 * a group is internally consistent and looks entirely plausible on its own — the only thing that
 * catches it is comparing totals to the source. Doing that check after publishing would mean the
 * warehouse already served the wrong number.
 *
 * <h2>Lineage after the write, per output</h2>
 *
 * <p>Each gold dataset gets its own {@code silver → gold} edge, recorded only once its write
 * succeeded. Recording up front would leave a graph asserting derivations that never happened, which
 * is worse than an incomplete graph because it looks complete.
 */
public final class GoldAggregateJob {

    private static final Logger log = LoggerFactory.getLogger(GoldAggregateJob.class);

    public static final String JOB_NAME = "GoldAggregateJob";
    public static final String SOURCE_DATASET = "silver.trip_clean";

    /** Gold dataset name to ClickHouse table. */
    private static final Map<String, String> TABLES = Map.of(
            "gold.agg_zone_hourly", "agg_zone_hourly",
            "gold.agg_borough_od", "agg_borough_od",
            "gold.agg_payment_daily", "agg_payment_daily",
            "gold.agg_daily_kpi", "agg_daily_kpi",
            "gold.fact_trip", "fact_trip");

    /** What the job wrote. */
    public record Result(
            Map<String, Long> rowsPerDataset,
            long servingKpiRows,
            long servingZoneRows,
            Reconciliation reconciliation) {

        public long totalGoldRows() {
            return rowsPerDataset.values().stream().mapToLong(Long::longValue).sum();
        }
    }

    /** Totals compared between silver and each aggregate. */
    public record Reconciliation(
            long silverRows,
            double silverRevenue,
            Map<String, Long> aggregateTrips,
            Map<String, Double> aggregateRevenue) {

        /** Tolerance in dollars; aggregates round to cents, so exact equality is the wrong test. */
        private static final double REVENUE_TOLERANCE = 0.01;

        public List<String> discrepancies() {
            List<String> problems = new java.util.ArrayList<>();
            aggregateTrips.forEach((name, trips) -> {
                if (trips != silverRows) {
                    problems.add(name + " covers " + trips + " trips, silver has " + silverRows);
                }
            });
            aggregateRevenue.forEach((name, revenue) -> {
                if (Math.abs(revenue - silverRevenue) > REVENUE_TOLERANCE) {
                    problems.add(String.format(
                            "%s revenue %.2f, silver %.2f", name, revenue, silverRevenue));
                }
            });
            return problems;
        }

        public boolean reconciles() {
            return discrepancies().isEmpty();
        }
    }

    /** Raised when an aggregate does not account for all of silver. */
    public static final class ReconciliationFailure extends RuntimeException {
        ReconciliationFailure(List<String> discrepancies) {
            super("gold aggregates do not reconcile with silver:\n  "
                    + String.join("\n  ", discrepancies));
        }
    }

    private final ClickHouseWriter clickHouse;
    private final ServingWriter serving;
    private final LineageRecorder lineage;

    public GoldAggregateJob(
            ClickHouseWriter clickHouse, ServingWriter serving, LineageRecorder lineage) {
        this.clickHouse = clickHouse;
        this.serving = serving;
        this.lineage = lineage;
    }

    public Result run(Dataset<Row> silver, Dataset<Row> zones, long runId) {
        Dataset<Row> cached = silver.cache();
        try {
            Map<String, Dataset<Row>> outputs = new LinkedHashMap<>();
            outputs.put("gold.agg_zone_hourly", GoldAggregates.zoneHourly(cached));
            outputs.put("gold.agg_borough_od", GoldAggregates.boroughOd(cached));
            outputs.put("gold.agg_payment_daily", GoldAggregates.paymentDaily(cached));
            outputs.put("gold.agg_daily_kpi", GoldAggregates.dailyKpi(cached));

            Reconciliation reconciliation = reconcile(cached, outputs);
            if (!reconciliation.reconciles()) {
                // Before any write. Publishing first and reporting after would mean the warehouse
                // already returned the wrong number to someone.
                throw new ReconciliationFailure(reconciliation.discrepancies());
            }
            log.info("gold reconciles with silver: {} trips, {} revenue",
                    reconciliation.silverRows(), reconciliation.silverRevenue());

            outputs.put("gold.fact_trip", GoldAggregates.factTrip(cached, runId));

            Map<String, Long> written = new LinkedHashMap<>();
            outputs.forEach((dataset, data) -> {
                Dataset<Row> stamped = data.columns().length > 0
                        && !List.of(data.columns()).contains("etl_run_id")
                        ? data.withColumn("etl_run_id",
                                org.apache.spark.sql.functions.lit(runId))
                        : data;

                long rows = clickHouse.append(stamped, TABLES.get(dataset));
                written.put(dataset, rows);
                // Per output, and only after its write succeeded.
                lineage.recordTransformation(runId, JOB_NAME, SOURCE_DATASET, dataset);
            });

            long kpiRows = serving.writeDailyKpi(outputs.get("gold.agg_daily_kpi"));
            long zoneRows = serving.writeZoneDimension(zones);

            return new Result(written, kpiRows, zoneRows, reconciliation);
        } finally {
            cached.unpersist();
        }
    }

    /** Compares every aggregate's totals against silver. */
    private Reconciliation reconcile(Dataset<Row> silver, Map<String, Dataset<Row>> aggregates) {
        long silverRows = silver.count();
        double silverRevenue = sumOf(silver, "total_amount");

        Map<String, Long> trips = new LinkedHashMap<>();
        Map<String, Double> revenue = new LinkedHashMap<>();
        aggregates.forEach((name, data) -> {
            trips.put(name, (long) sumOf(data, "trip_count"));
            revenue.put(name, sumOf(data, "total_revenue"));
        });

        return new Reconciliation(silverRows, silverRevenue, trips, revenue);
    }

    private static double sumOf(Dataset<Row> data, String column) {
        Row row = data.agg(org.apache.spark.sql.functions.sum(
                org.apache.spark.sql.functions.col(column))).first();
        return row.isNullAt(0) ? 0.0 : ((Number) row.get(0)).doubleValue();
    }
}
