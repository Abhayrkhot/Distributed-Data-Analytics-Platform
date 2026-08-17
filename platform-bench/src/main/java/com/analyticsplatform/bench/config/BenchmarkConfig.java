package com.analyticsplatform.bench.config;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A named set of Spark settings to measure.
 *
 * <h2>Three honest configurations, not a sabotaged baseline</h2>
 *
 * <p>A tempting baseline is "Spark defaults with AQE disabled". That would be dishonest: <strong>AQE
 * is enabled by default in Spark 3.5</strong>, so turning it off is not a default, it is a
 * handicap. A percentage measured against a deliberately crippled configuration says nothing about
 * the tuning work and would not survive being asked about.
 *
 * <p>So there are three:
 *
 * <ul>
 *   <li>{@link #sparkDefault()} — Spark 3.5's actual out-of-the-box behaviour, AQE included
 *   <li>{@link #naiveApp()} — a plausible first-attempt implementation: no partition pruning, no
 *       column pruning, no broadcast hint. This is what someone writes before profiling.
 *   <li>{@link #optimized()} — the tuned configuration
 * </ul>
 *
 * <p>All three appear in the report. The headline claim names its baseline explicitly rather than
 * saying "improved Spark performance by X%", which invites the reader to assume the largest gap.
 *
 * <h2>Compression lives only in Experiment B</h2>
 *
 * <p>None of the execution configurations vary the codec. Mixing a physical-layout change into an
 * execution measurement makes the resulting number indefensible: it is no longer attributable to
 * either.
 */
public record BenchmarkConfig(
        String label,
        Experiment experiment,
        Map<String, String> sparkSettings,
        boolean partitionPruning,
        boolean columnPruning,
        boolean broadcastHint) {

    /** Which experiment a configuration belongs to. */
    public enum Experiment {
        /** Execution tuning. Identical physical input and identical compression throughout. */
        A_EXECUTION("A_execution"),
        /** Physical layout. Measured in bytes and files scanned, not just wall clock. */
        B_STORAGE("B_storage");

        private final String dbValue;

        Experiment(String dbValue) {
            this.dbValue = dbValue;
        }

        public String dbValue() {
            return dbValue;
        }
    }

    /**
     * Settings Spark refuses to change on a live session.
     *
     * <p>{@code spark.conf().set()} throws CANNOT_MODIFY_CONFIG for these, so a benchmark cannot
     * vary them between runs — they are fixed when the session is created. Listing them here means
     * an attempt to measure one fails immediately with an explanation, rather than at the first
     * execution with a Spark error that reads like a bug in the harness.
     */
    private static final java.util.Set<String> STATIC_SETTINGS = java.util.Set.of(
            "spark.serializer",
            "spark.master",
            "spark.driver.memory",
            "spark.executor.memory",
            "spark.executor.cores",
            "spark.kryo.registrator");

    public BenchmarkConfig {
        if (label == null || label.isBlank()) {
            throw new IllegalArgumentException("label is required");
        }
        if (experiment == null) {
            throw new IllegalArgumentException("experiment is required");
        }
        sparkSettings = sparkSettings == null ? Map.of() : Map.copyOf(sparkSettings);

        // Compression belongs to Experiment B alone. Enforced rather than documented, because a
        // codec quietly appearing in an execution config is exactly the mistake that makes a
        // headline percentage unattributable.
        if (experiment == Experiment.A_EXECUTION) {
            for (String key : sparkSettings.keySet()) {
                if (key.contains("compression") || key.contains("codec")) {
                    throw new IllegalArgumentException(
                            "execution config '" + label + "' sets " + key
                                    + "; compression belongs to Experiment B");
                }
            }
        }

        for (String key : sparkSettings.keySet()) {
            if (STATIC_SETTINGS.contains(key)) {
                throw new IllegalArgumentException(
                        "config '" + label + "' sets " + key + ", which Spark fixes at session "
                                + "creation and refuses to change at runtime. It cannot be varied "
                                + "between benchmark runs, so measuring it here would silently "
                                + "compare two runs with identical settings.");
            }
        }
    }

    /**
     * Spark 3.5's genuine defaults.
     *
     * <p>Deliberately does not disable AQE. Spark 3.5 ships with
     * {@code spark.sql.adaptive.enabled=true}, and pretending otherwise would manufacture a gap.
     */
    public static BenchmarkConfig sparkDefault() {
        Map<String, String> settings = new LinkedHashMap<>();
        settings.put("spark.sql.shuffle.partitions", "200");
        settings.put("spark.sql.adaptive.enabled", "true");
        settings.put("spark.sql.adaptive.coalescePartitions.enabled", "true");
        settings.put("spark.sql.autoBroadcastJoinThreshold", "10485760");
        return new BenchmarkConfig("spark_default", Experiment.A_EXECUTION, settings,
                false, false, false);
    }

    /**
     * A plausible first attempt: Spark defaults plus application code that reads more than it needs.
     *
     * <p>This is the honest baseline for the headline claim, because it is what the pipeline
     * actually looked like before tuning — not a configuration invented to lose.
     */
    public static BenchmarkConfig naiveApp() {
        Map<String, String> settings = new LinkedHashMap<>();
        settings.put("spark.sql.shuffle.partitions", "200");
        settings.put("spark.sql.adaptive.enabled", "true");
        settings.put("spark.sql.adaptive.coalescePartitions.enabled", "true");
        settings.put("spark.sql.autoBroadcastJoinThreshold", "10485760");
        return new BenchmarkConfig("naive_app", Experiment.A_EXECUTION, settings,
                false, false, false);
    }

    /** The tuned configuration. */
    public static BenchmarkConfig optimized() {
        Map<String, String> settings = new LinkedHashMap<>();
        // Sized to the cluster (6 cores), not left at the 200 default, which on a dataset this
        // size is mostly scheduler overhead.
        settings.put("spark.sql.shuffle.partitions", "12");
        settings.put("spark.sql.adaptive.enabled", "true");
        settings.put("spark.sql.adaptive.coalescePartitions.enabled", "true");
        settings.put("spark.sql.adaptive.skewJoin.enabled", "true");
        settings.put("spark.sql.files.maxPartitionBytes", "67108864");
        // Kryo is deliberately absent. spark.serializer is fixed at session creation, so this
        // harness cannot vary it, and including it would mean claiming a measurement that never
        // happened. Serializer choice belongs in spark-defaults.conf and is applied to every run
        // equally - which is why it cannot appear in a percentage attributed to tuning.
        return new BenchmarkConfig("optimized", Experiment.A_EXECUTION, settings,
                true, true, true);
    }

    /** Experiment B: the storage layouts to compare. */
    public static BenchmarkConfig storage(String label, String codec, boolean partitioned) {
        Map<String, String> settings = new LinkedHashMap<>();
        settings.put("spark.sql.parquet.compression.codec", codec);
        return new BenchmarkConfig(label, Experiment.B_STORAGE, settings,
                partitioned, false, false);
    }

    /**
     * The cumulative ablation ladder for Experiment A.
     *
     * <p>Cumulative order assigns credit in one arbitrary sequence, so a leave-one-out pass runs
     * alongside it — see {@link #leaveOneOut()}. Neither alone answers "which change mattered";
     * together they bracket it.
     */
    public static List<BenchmarkConfig> ablationLadder() {
        BenchmarkConfig base = naiveApp();
        List<BenchmarkConfig> ladder = new java.util.ArrayList<>();
        ladder.add(step(base, "B0_baseline", base.sparkSettings(), false, false, false));

        Map<String, String> withPartitions = new LinkedHashMap<>(base.sparkSettings());
        withPartitions.put("spark.sql.shuffle.partitions", "12");
        ladder.add(step(base, "B1_shuffle_partitions", withPartitions, false, false, false));

        Map<String, String> withSkew = new LinkedHashMap<>(withPartitions);
        withSkew.put("spark.sql.adaptive.skewJoin.enabled", "true");
        ladder.add(step(base, "B2_aqe_skew", withSkew, false, false, false));

        ladder.add(step(base, "B3_partition_pruning", withSkew, true, false, false));
        ladder.add(step(base, "B4_column_pruning", withSkew, true, true, false));
        ladder.add(step(base, "B5_broadcast", withSkew, true, true, true));

        Map<String, String> full = new LinkedHashMap<>(optimized().sparkSettings());
        ladder.add(step(base, "B6_full", full, true, true, true));
        return List.copyOf(ladder);
    }

    /** Full-optimized with one change removed at a time. */
    public static List<BenchmarkConfig> leaveOneOut() {
        BenchmarkConfig full = optimized();
        List<BenchmarkConfig> variants = new java.util.ArrayList<>();

        Map<String, String> noPartitionTuning = new LinkedHashMap<>(full.sparkSettings());
        noPartitionTuning.put("spark.sql.shuffle.partitions", "200");
        variants.add(step(full, "L1_minus_shuffle_partitions", noPartitionTuning,
                true, true, true));

        variants.add(step(full, "L2_minus_partition_pruning", full.sparkSettings(),
                false, true, true));
        variants.add(step(full, "L3_minus_column_pruning", full.sparkSettings(),
                true, false, true));
        variants.add(step(full, "L4_minus_broadcast", full.sparkSettings(),
                true, true, false));
        return List.copyOf(variants);
    }

    private static BenchmarkConfig step(
            BenchmarkConfig source, String label, Map<String, String> settings,
            boolean partitionPruning, boolean columnPruning, boolean broadcastHint) {
        return new BenchmarkConfig(label, source.experiment(), settings,
                partitionPruning, columnPruning, broadcastHint);
    }

    /** A stable description of the settings, for the report. */
    public String describe() {
        StringBuilder out = new StringBuilder(label).append(": ");
        sparkSettings.forEach((k, v) -> out.append(k.replace("spark.sql.", "")).append('=')
                .append(v).append(' '));
        out.append("pruning=").append(partitionPruning ? "part" : "-")
                .append(columnPruning ? "+col" : "")
                .append(" broadcast=").append(broadcastHint);
        return out.toString();
    }
}
