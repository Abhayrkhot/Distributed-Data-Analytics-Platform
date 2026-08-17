package com.analyticsplatform.ingest.job;

import com.analyticsplatform.common.dao.LineageRecorder;
import com.analyticsplatform.common.schema.CanonicalSchema;
import com.analyticsplatform.common.schema.SchemaRegistry;
import com.analyticsplatform.ingest.publish.ProcessingUnitStore.UnitKey;
import com.analyticsplatform.ingest.publish.StagedPublisher;
import com.analyticsplatform.ingest.publish.StagedPublisher.Outcome;
import com.analyticsplatform.ingest.publish.StagedPublisher.WriteResult;
import com.analyticsplatform.ingest.source.SourceNormalizer;
import java.nio.file.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Raw TLC files to bronze.
 *
 * <p>Wires {@link SourceNormalizer} to the staged-publish protocol. Step 4 built the protocol and
 * the normalizer separately; this is the job that puts them together, so bronze can be produced the
 * same way silver and gold are rather than only from a test harness.
 *
 * <h2>Bronze rejects nothing</h2>
 *
 * <p>Every row that arrives is published, including ones silver will later reject. Filtering here
 * would let a row vanish between the file and the warehouse with nothing recording why — the
 * separation is what makes silver's rejection counts meaningful.
 *
 * <p>Schema registration happens before any write, so a breaking upstream change aborts while the
 * target is still untouched.
 */
public final class BronzeIngestJob {

    private static final Logger log = LoggerFactory.getLogger(BronzeIngestJob.class);

    public static final String DEFAULT_DATASET = "bronze.trip_raw";
    public static final String STAGE = "raw_to_bronze";
    public static final String JOB_NAME = "BronzeIngestJob";

    /** One unit of raw input. */
    public record Inputs(
            Dataset<Row> raw,
            String source,
            String processingUnit,
            Path target,
            long runId,
            String owner) {

        public Inputs {
            if (!SourceNormalizer.YELLOW.equals(source) && !SourceNormalizer.GREEN.equals(source)) {
                throw new IllegalArgumentException("unknown source: " + source);
            }
        }
    }

    public record Result(Outcome outcome, long rowsRead, long rowsWritten) {
        public boolean published() {
            return outcome.published();
        }
    }

    private final String dataset;
    private final SchemaRegistry schemaRegistry;
    private final StagedPublisher publisher;
    private final LineageRecorder lineage;

    public BronzeIngestJob(
            String dataset,
            SchemaRegistry schemaRegistry,
            StagedPublisher publisher,
            LineageRecorder lineage) {
        this.dataset = dataset;
        this.schemaRegistry = schemaRegistry;
        this.publisher = publisher;
        this.lineage = lineage;
    }

    public Result run(Inputs inputs) {
        UnitKey key = new UnitKey(dataset, STAGE, inputs.processingUnit());
        Dataset<Row> normalized = SourceNormalizer.normalize(inputs.raw(), inputs.source()).cache();

        try {
            // Before any write: a breaking change must abort while the target is untouched.
            schemaRegistry.register(dataset, normalized.schema());

            long rowsRead = normalized.count();
            Outcome outcome = publisher.publish(key, inputs.runId(), inputs.owner(),
                    inputs.target(), staging -> {
                        normalized.write().mode(SaveMode.Overwrite).parquet(staging.toString());
                        Dataset<Row> staged =
                                normalized.sparkSession().read().parquet(staging.toString());
                        return new WriteResult(staged.count(),
                                CanonicalSchema.hash(staged.schema()));
                    });

            if (outcome.published()) {
                // After the commit, never before: lineage for an aborted run is confidently wrong.
                lineage.recordTransformation(inputs.runId(), JOB_NAME,
                        "raw." + inputs.source() + "_tripdata", dataset);
            }

            log.info("bronze {} -> {} ({} rows)", inputs.source(), key, outcome.rowCount());
            return new Result(outcome, rowsRead, outcome.rowCount());
        } finally {
            normalized.unpersist();
        }
    }
}
