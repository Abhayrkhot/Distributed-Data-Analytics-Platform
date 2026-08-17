package com.analyticsplatform.transform.job;

import com.analyticsplatform.common.dao.LineageRecorder;
import com.analyticsplatform.common.schema.CanonicalSchema;
import com.analyticsplatform.common.schema.SchemaRegistry;
import com.analyticsplatform.ingest.publish.ProcessingUnitStore.UnitKey;
import com.analyticsplatform.ingest.publish.StagedPublisher;
import com.analyticsplatform.ingest.publish.StagedPublisher.Outcome;
import com.analyticsplatform.ingest.publish.StagedPublisher.WriteResult;
import com.analyticsplatform.transform.dq.DqEngine;
import com.analyticsplatform.transform.dq.DqEngine.Report;
import com.analyticsplatform.transform.dq.DqRule;
import com.analyticsplatform.transform.dq.DqRuleStore;
import com.analyticsplatform.transform.silver.SilverTransform;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Bronze to silver, with data quality gating publication.
 *
 * <h2>Order of operations</h2>
 *
 * <pre>
 *   register schema (aborts on a breaking change, before any work)
 *     → transform
 *     → write to staging
 *     → evaluate DQ ON THE STAGED DATA
 *     → blocking breach? abort; target untouched
 *     → promote → manifest (COMMIT) → mark COMPLETE
 *     → record lineage
 * </pre>
 *
 * <p>DQ runs against staged output rather than the in-memory DataFrame, and before promotion rather
 * than after. Both details matter. Evaluating in memory would test a plan Spark might recompute
 * differently on write; evaluating after promotion would mean invalid data is already published by
 * the time anyone objects, and "we detected it" is no consolation to a dashboard that already
 * served it.
 *
 * <p>Lineage is written last, after the commit. A graph claiming silver derives from bronze for a
 * run that aborted is worse than no graph — it is confidently wrong.
 */
public final class SilverTransformJob {

    private static final Logger log = LoggerFactory.getLogger(SilverTransformJob.class);

    public static final String DEFAULT_DATASET = "silver.trip_clean";
    public static final String SOURCE_DATASET = "bronze.trip_raw";
    public static final String STAGE = "bronze_to_silver";
    public static final String JOB_NAME = "SilverTransformJob";

    /** Everything the job needs, so it can be driven from a test without a CLI. */
    public record Inputs(
            Dataset<Row> bronze,
            Dataset<Row> zones,
            String processingUnit,
            Path target,
            long runId,
            String owner) {
    }

    /** What the job did. */
    public record Result(Outcome outcome, Report dqReport, long rowsRead, long rowsWritten) {
        public boolean published() {
            return outcome.published();
        }
    }

    /** Raised when a blocking DQ rule breaches. Carries the report so the caller can log it. */
    public static final class DqGateFailure extends RuntimeException {
        private final transient Report report;

        DqGateFailure(String message, Report report) {
            super(message);
            this.report = report;
        }

        public Report report() {
            return report;
        }
    }

    /**
     * Which dataset's rules and schema this instance governs.
     *
     * <p>Injectable so an integration test can operate on its own dataset with its own rule set,
     * rather than mutating the shared production rules and leaving them changed for whatever runs
     * next.
     */
    private final String dataset;

    private final SparkSession spark;
    private final SchemaRegistry schemaRegistry;
    private final DqRuleStore ruleStore;
    private final DqEngine dqEngine;
    private final StagedPublisher publisher;
    private final LineageRecorder lineage;

    public SilverTransformJob(
            SparkSession spark,
            String dataset,
            SchemaRegistry schemaRegistry,
            DqRuleStore ruleStore,
            DqEngine dqEngine,
            StagedPublisher publisher,
            LineageRecorder lineage) {
        this.dataset = dataset;
        this.spark = spark;
        this.schemaRegistry = schemaRegistry;
        this.ruleStore = ruleStore;
        this.dqEngine = dqEngine;
        this.publisher = publisher;
        this.lineage = lineage;
        SilverTransform.registerUdfs(spark);
    }

    public Result run(Inputs inputs) {
        UnitKey key = new UnitKey(dataset, STAGE, inputs.processingUnit());

        Dataset<Row> silver = SilverTransform.transform(inputs.bronze(), inputs.zones()).cache();
        try {
            // Registered before anything is written. A breaking change must abort while the target
            // is still untouched, not after a staging directory has been filled.
            schemaRegistry.register(dataset, silver.schema());

            List<DqRule> rules = ruleStore.rulesFor(dataset);
            long rowsRead = inputs.bronze().count();

            Report[] captured = new Report[1];
            Outcome outcome = publisher.publish(key, inputs.runId(), inputs.owner(),
                    inputs.target(), staging -> {
                        silver.write().mode(SaveMode.Overwrite).parquet(staging.toString());

                        // Read back from staging: this evaluates what was actually written, not a
                        // plan Spark might recompute differently.
                        Dataset<Row> staged = spark.read().parquet(staging.toString());
                        Report report = evaluate(staged, rules, inputs.zones());
                        captured[0] = report;
                        ruleStore.recordResults(inputs.runId(), dataset, report.results());

                        if (report.blocked()) {
                            throw new DqGateFailure(
                                    "data quality gate blocked publication of " + key + ": "
                                            + report.summary() + "\n  "
                                            + String.join("\n  ", report.breaches().stream()
                                                    .map(DqEngine.RuleResult::describe).toList()),
                                    report);
                        }
                        log.info("DQ passed for {}: {}", key, report.summary());
                        return new WriteResult(staged.count(),
                                CanonicalSchema.hash(staged.schema()));
                    });

            if (outcome.published()) {
                // After the commit, never before: lineage for an aborted run is confidently wrong.
                lineage.recordTransformation(
                        inputs.runId(), JOB_NAME, SOURCE_DATASET, dataset);
            }

            return new Result(outcome, captured[0], rowsRead, outcome.rowCount());
        } finally {
            silver.unpersist();
        }
    }

    private Report evaluate(Dataset<Row> staged, List<DqRule> rules, Dataset<Row> zones) {
        List<DqRule> inapplicable = DqEngine.inapplicable(staged, rules);
        if (!inapplicable.isEmpty()) {
            // A rule pointing at a column that does not exist is a configuration error, not a
            // data problem. Skipping it silently would report a clean bill of health from a
            // check that never ran.
            throw new IllegalStateException(
                    "DQ rules reference columns absent from silver: "
                            + inapplicable.stream().map(DqRule::ruleName).toList());
        }

        Optional<Long> previous = ruleStore.previousRowCount(dataset, STAGE);
        DqEngine.Context context = new DqEngine.Context(
                Map.of("raw.taxi_zone_lookup", zones), previous);
        return dqEngine.evaluate(staged, rules, context);
    }
}
