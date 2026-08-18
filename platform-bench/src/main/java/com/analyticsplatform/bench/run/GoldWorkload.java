package com.analyticsplatform.bench.run;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.broadcast;
import static org.apache.spark.sql.functions.coalesce;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.month;
import static org.apache.spark.sql.functions.sum;
import static org.apache.spark.sql.functions.year;

import com.analyticsplatform.bench.config.BenchmarkConfig;
import java.util.List;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * The workload under measurement: silver to a borough-level aggregate.
 *
 * <p>Chosen because it exercises everything the tuning targets — a shuffle, a dimension join, and a
 * scan whose width and partition count the configuration can change. A workload that only scanned
 * would make partition tuning look irrelevant; one that only shuffled would make pruning look so.
 *
 * <h2>The configuration changes the plan, not the answer</h2>
 *
 * <p>Every variant must produce identical output. Pruning reads fewer columns and fewer partitions
 * but computes the same aggregate; the broadcast hint changes the join strategy, not its result. If
 * a variant ever produced something different, the correctness gate rejects the whole comparison —
 * which is the only reason it is safe to let the configuration influence the plan at all.
 */
public final class GoldWorkload {

    /** Columns the aggregate actually needs. Everything else is dead weight. */
    private static final List<String> REQUIRED_COLUMNS = List.of(
            "pickup_ts", "pickup_location_id", "pickup_borough",
            "fare_amount", "total_amount", "trip_distance_mi", "trip_duration_min");

    private GoldWorkload() {
    }

    /**
     * Builds the aggregate under a given configuration.
     *
     * @param silver the trip fact
     * @param zones  the zone dimension, small enough to broadcast
     */
    public static Dataset<Row> aggregate(
            Dataset<Row> silver, Dataset<Row> zones, BenchmarkConfig config) {

        Dataset<Row> source = silver;

        // The scope filter is part of the WORKLOAD, applied to every configuration. It is not the
        // optimization.
        //
        // An earlier revision applied it only when partitionPruning was on, and the correctness
        // gate rejected the whole comparison on real data: the January 2024 file contains rows
        // with pickup years 2002, 2009 and 2023 (15 in 2.9M, corrupt meter timestamps), so the
        // "optimized" config was faster partly because it silently processed less data and
        // produced a DIFFERENT answer. That is a filter mislabelled as an optimization - genuine
        // partition pruning skips files that cannot match and never changes the result.
        //
        // What the flag now controls is WHERE the filter runs, which changes the plan and not the
        // answer. Both branches below produce identical output; only the cost differs.
        Column inScope = year(col("pickup_ts")).isin(2024, 2025);

        if (config.partitionPruning()) {
            // Early: filter before the join, so the predicate reaches the scan and the join sees
            // fewer rows.
            source = source.filter(inScope);
        }

        // Column pruning: project early. Without it the scan carries every column through the
        // shuffle, which is the cost the tuning is meant to remove.
        if (config.columnPruning()) {
            source = source.select(REQUIRED_COLUMNS.stream()
                    .map(org.apache.spark.sql.functions::col)
                    .toArray(org.apache.spark.sql.Column[]::new));
        }

        Dataset<Row> dimension = zones.select(
                col("LocationID").alias("__zone_id"),
                col("service_zone").alias("__service_zone"));

        // Spark auto-broadcasts small tables, so the hint may be a no-op. The ablation and the
        // EXPLAIN capture are what determine whether it actually changed anything - the hint alone
        // is not evidence.
        Dataset<Row> joined = config.broadcastHint()
                ? source.join(broadcast(dimension),
                        col("pickup_location_id").equalTo(col("__zone_id")), "left")
                : source.join(dimension,
                        col("pickup_location_id").equalTo(col("__zone_id")), "left");

        // Late: the unpruned configuration applies the same filter here, after the join has
        // already carried every row through. Identical result, materially more work.
        Dataset<Row> scoped = config.partitionPruning() ? joined : joined.filter(inScope);

        return scoped
                .withColumn("service_zone", coalesce(col("__service_zone"), lit("Unknown")))
                .groupBy(col("pickup_borough"), col("service_zone"),
                        year(col("pickup_ts")).alias("pickup_year"),
                        month(col("pickup_ts")).alias("pickup_month"))
                .agg(
                        count(lit(1)).alias("trip_count"),
                        sum("total_amount").alias("total_revenue"),
                        avg("fare_amount").alias("avg_fare"),
                        avg("trip_distance_mi").alias("avg_distance_mi"),
                        avg("trip_duration_min").alias("avg_duration_min"));
    }

    /** The physical plan, for evidence that a claimed optimization actually engaged. */
    public static String explain(Dataset<Row> plan) {
        return plan.queryExecution().executedPlan().toString();
    }

    /**
     * Whether a plan uses a broadcast join.
     *
     * <p>Read from the plan rather than assumed from the hint. Spark broadcasts small tables on its
     * own, so a hint can be entirely redundant — and attributing an improvement to a no-op is
     * exactly the kind of claim this project is trying not to make.
     */
    public static boolean usesBroadcastJoin(Dataset<Row> plan) {
        return explain(plan).contains("BroadcastHashJoin");
    }
}
