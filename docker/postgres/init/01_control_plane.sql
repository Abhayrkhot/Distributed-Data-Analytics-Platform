-- ============================================================================
--  Control plane
--
--  Postgres here is deliberately NOT a second copy of the warehouse. It is the
--  metadata plane: what ran, how long it took, what it read and wrote, whether
--  the data passed its quality rules, how datasets derive from one another, how
--  schemas changed, and how far incremental processing has progressed.
--
--  Analytical data lives in ClickHouse (see clickhouse/init/01_marts.sql).
--
--  Design note: constraints here are load-bearing, not decorative. The
--  verification plan requires that impossible states be rejected by the
--  database even if application-level validation contains a bug, so state
--  machine rules, threshold bounds, and commit uniqueness are all enforced as
--  CHECK / UNIQUE constraints rather than left to Java.
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS control;

-- ---------------------------------------------------------------------------
-- Governance catalog: one row per managed dataset.
-- ---------------------------------------------------------------------------
CREATE TABLE control.dataset_catalog (
    dataset_id      BIGSERIAL   PRIMARY KEY,
    dataset_name    TEXT        NOT NULL UNIQUE,
    layer           TEXT        NOT NULL
        CHECK (layer IN ('bronze', 'silver', 'gold', 'stream', 'reference')),
    storage_system  TEXT        NOT NULL
        CHECK (storage_system IN ('filesystem', 'clickhouse', 'postgres', 'kafka')),
    storage_path    TEXT,
    data_format     TEXT,
    partition_keys  TEXT[]      NOT NULL DEFAULT '{}',
    owner           TEXT        NOT NULL,
    pii_class       TEXT        NOT NULL DEFAULT 'none'
        CHECK (pii_class IN ('none', 'low', 'medium', 'high')),
    retention_days  INTEGER     CHECK (retention_days IS NULL OR retention_days > 0),
    description     TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------------
-- Run registry: one row per job execution.
-- ---------------------------------------------------------------------------
CREATE TABLE control.etl_run (
    run_id        BIGSERIAL   PRIMARY KEY,
    run_uuid      UUID        NOT NULL UNIQUE,
    job_name      TEXT        NOT NULL,
    job_version   TEXT,
    layer         TEXT,
    spark_app_id  TEXT,
    config_json   JSONB       NOT NULL DEFAULT '{}'::jsonb,
    config_label  TEXT,
    git_commit    TEXT,
    status        TEXT        NOT NULL
        CHECK (status IN ('RUNNING', 'SUCCESS', 'FAILED', 'ABORTED')),
    started_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    ended_at      TIMESTAMPTZ,
    duration_ms   BIGINT      CHECK (duration_ms IS NULL OR duration_ms >= 0),
    rows_read     BIGINT      NOT NULL DEFAULT 0,
    rows_written  BIGINT      NOT NULL DEFAULT 0,
    rows_rejected BIGINT      NOT NULL DEFAULT 0,
    -- Populated by RunContext.markFailed(e); the original exception is rethrown,
    -- never swallowed.
    error_class   TEXT,
    error_message TEXT,
    CONSTRAINT etl_run_terminal_has_end
        CHECK (status = 'RUNNING' OR ended_at IS NOT NULL),
    -- A failed run must say why; a successful run must not claim an error.
    CONSTRAINT etl_run_failed_has_error
        CHECK (status <> 'FAILED' OR error_class IS NOT NULL),
    CONSTRAINT etl_run_success_has_no_error
        CHECK (status <> 'SUCCESS' OR (error_class IS NULL AND error_message IS NULL))
);

CREATE INDEX idx_etl_run_job_started ON control.etl_run (job_name, started_at DESC);
CREATE INDEX idx_etl_run_label       ON control.etl_run (config_label) WHERE config_label IS NOT NULL;

-- ---------------------------------------------------------------------------
-- Free-form metrics per run.
--
-- attempt_scope exists because Spark retries tasks: summing every onTaskEnd
-- conflates failed attempts with their successful retries. Both scopes are
-- recorded so performance analysis can distinguish total execution effort from
-- useful work.
-- ---------------------------------------------------------------------------
CREATE TABLE control.etl_run_metric (
    metric_id     BIGSERIAL   PRIMARY KEY,
    run_id        BIGINT      NOT NULL REFERENCES control.etl_run (run_id) ON DELETE CASCADE,
    metric_name   TEXT        NOT NULL,
    metric_value  DOUBLE PRECISION NOT NULL,
    metric_unit   TEXT,
    attempt_scope TEXT        NOT NULL DEFAULT 'all_attempts'
        CHECK (attempt_scope IN ('all_attempts', 'successful_only')),
    recorded_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (run_id, metric_name, attempt_scope)
);

CREATE INDEX idx_run_metric_name ON control.etl_run_metric (metric_name);

-- ---------------------------------------------------------------------------
-- Schema registry. Every ingest run diffs the incoming schema (canonicalized,
-- then SHA-256'd) against the latest version here; the classification decides
-- whether the run proceeds (additive/widening) or aborts (breaking).
-- ---------------------------------------------------------------------------
CREATE TABLE control.schema_version (
    schema_version_id BIGSERIAL PRIMARY KEY,
    dataset_name      TEXT        NOT NULL,
    version           INTEGER     NOT NULL CHECK (version > 0),
    schema_json       JSONB       NOT NULL,
    -- SHA-256 over the canonical `name:type:nullable` form, fields sorted.
    schema_hash       TEXT        NOT NULL,
    change_type       TEXT        NOT NULL
        CHECK (change_type IN ('initial', 'additive', 'widening', 'breaking')),
    added_columns     TEXT[]      NOT NULL DEFAULT '{}',
    removed_columns   TEXT[]      NOT NULL DEFAULT '{}',
    retyped_columns   TEXT[]      NOT NULL DEFAULT '{}',
    change_note       TEXT,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (dataset_name, version),
    UNIQUE (dataset_name, schema_hash)
);

CREATE INDEX idx_schema_version_dataset ON control.schema_version (dataset_name, version DESC);

-- ---------------------------------------------------------------------------
-- Processing units: incremental progress, tracked PER PIPELINE STAGE.
--
-- Bronze ingestion and silver transformation are independent states for the
-- same logical unit. Keying on (dataset, stage, unit) lets
--   yellow/2024-01 raw_to_bronze   = COMPLETE
--   yellow/2024-01 bronze_to_silver = FAILED
-- coexist instead of one overwriting the other.
--
-- State machine (enforced below and generatively tested):
--   PENDING -> RUNNING -> COMPLETE
--                      -> FAILED -> RUNNING
--   expired RUNNING -> FAILED -> RUNNING
-- Rejected: COMPLETE -> RUNNING, COMPLETE -> PENDING, FAILED -> COMPLETE.
-- ---------------------------------------------------------------------------
CREATE TABLE control.processing_unit (
    dataset_name     TEXT        NOT NULL,
    pipeline_stage   TEXT        NOT NULL
        CHECK (pipeline_stage IN ('raw_to_bronze', 'bronze_to_silver',
                                  'silver_to_gold', 'stream_to_gold')),
    processing_unit  TEXT        NOT NULL,
    status           TEXT        NOT NULL DEFAULT 'PENDING'
        CHECK (status IN ('PENDING', 'RUNNING', 'COMPLETE', 'FAILED')),
    attempt_count    INTEGER     NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    -- Lease fields are meaningful only while RUNNING.
    lease_owner      TEXT,
    lease_expires_at TIMESTAMPTZ,
    -- Attempt-specific, so a retry can never inherit a failed attempt's files.
    staging_path     TEXT,
    last_run_id      BIGINT      REFERENCES control.etl_run (run_id),
    rows_processed   BIGINT      NOT NULL DEFAULT 0,
    error_message    TEXT,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (dataset_name, pipeline_stage, processing_unit),
    -- At most one valid lease owner: RUNNING implies a lease, and only RUNNING
    -- may hold one. Prevents a stale lease surviving a transition.
    CONSTRAINT pu_lease_iff_running CHECK (
        (status = 'RUNNING'  AND lease_owner IS NOT NULL AND lease_expires_at IS NOT NULL)
     OR (status <> 'RUNNING' AND lease_owner IS NULL     AND lease_expires_at IS NULL)
    )
);

CREATE INDEX idx_pu_status ON control.processing_unit (pipeline_stage, status);
CREATE INDEX idx_pu_expired_lease ON control.processing_unit (lease_expires_at)
    WHERE status = 'RUNNING';

-- ---------------------------------------------------------------------------
-- Unit manifest: THE LOGICAL COMMIT RECORD.
--
-- Writing files into the target path does NOT commit a processing unit. The
-- commit point is this single-row insert, which Postgres makes atomic -- the
-- filesystem only has to be recoverable, not transactional.
--
-- A unit is committed only when: staged output validated, target published,
-- target verified against the staged fingerprint, and this row persisted.
-- processing_unit.status = COMPLETE is bookkeeping that FOLLOWS the commit.
--
-- Therefore: files in the target with no manifest row are NOT committed and are
-- always discarded, never adopted.
-- ---------------------------------------------------------------------------
CREATE TABLE control.unit_manifest (
    manifest_id        BIGSERIAL   PRIMARY KEY,
    dataset_name       TEXT        NOT NULL,
    pipeline_stage     TEXT        NOT NULL,
    processing_unit    TEXT        NOT NULL,
    run_id             BIGINT      NOT NULL REFERENCES control.etl_run (run_id),
    schema_hash        TEXT        NOT NULL,
    row_count          BIGINT      NOT NULL CHECK (row_count >= 0),
    file_count         INTEGER     NOT NULL CHECK (file_count >= 0),
    total_bytes        BIGINT      NOT NULL CHECK (total_bytes >= 0),
    -- Identity of the input that produced this output, for staleness detection.
    source_fingerprint TEXT        NOT NULL,
    target_path        TEXT        NOT NULL,
    published_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- One commit per unit per stage: makes the commit a single atomic insert.
    UNIQUE (dataset_name, pipeline_stage, processing_unit),
    FOREIGN KEY (dataset_name, pipeline_stage, processing_unit)
        REFERENCES control.processing_unit (dataset_name, pipeline_stage, processing_unit)
);

-- ---------------------------------------------------------------------------
-- Data quality: declarative rules, one result row per rule per run.
--
-- Threshold semantics are explicit rather than implied. A bare `0.05` is
-- ambiguous (5% failures allowed? 95% success required?), so the type is stored
-- alongside the value. null_policy removes Spark's three-valued-logic ambiguity
-- where `NULL > 0` is neither true nor false.
-- ---------------------------------------------------------------------------
CREATE TABLE control.dq_rule (
    rule_id         BIGSERIAL   PRIMARY KEY,
    rule_name       TEXT        NOT NULL UNIQUE,
    dataset_name    TEXT        NOT NULL,
    rule_type       TEXT        NOT NULL
        CHECK (rule_type IN ('not_null', 'range', 'unique', 'referential',
                             'row_count_delta', 'freshness', 'accepted_values',
                             'expression')),
    target_column   TEXT,
    rule_params     JSONB       NOT NULL DEFAULT '{}'::jsonb,
    -- WARN records and continues; FAIL aborts before publication.
    severity        TEXT        NOT NULL DEFAULT 'FAIL'
        CHECK (severity IN ('WARN', 'FAIL')),
    threshold_type  TEXT        NOT NULL DEFAULT 'max_violation_fraction'
        CHECK (threshold_type IN ('max_violation_fraction', 'max_violation_count')),
    threshold_value NUMERIC(18,8) NOT NULL DEFAULT 0 CHECK (threshold_value >= 0),
    -- violation: NULL counts as a failure
    -- pass:      NULL is acceptable
    -- ignore:    NULL rows excluded from the denominator
    null_policy     TEXT        NOT NULL DEFAULT 'violation'
        CHECK (null_policy IN ('violation', 'pass', 'ignore')),
    enabled         BOOLEAN     NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- A fraction above 1.0 is meaningless and almost certainly a typo.
    CONSTRAINT dq_fraction_bounded CHECK (
        threshold_type <> 'max_violation_fraction' OR threshold_value <= 1
    ),
    -- A count threshold must be a whole number of rows.
    CONSTRAINT dq_count_is_integral CHECK (
        threshold_type <> 'max_violation_count' OR threshold_value = trunc(threshold_value)
    )
);

CREATE INDEX idx_dq_rule_dataset ON control.dq_rule (dataset_name) WHERE enabled;

CREATE TABLE control.dq_result (
    result_id      BIGSERIAL   PRIMARY KEY,
    run_id         BIGINT      NOT NULL REFERENCES control.etl_run (run_id) ON DELETE CASCADE,
    rule_id        BIGINT      NOT NULL REFERENCES control.dq_rule (rule_id),
    dataset_name   TEXT        NOT NULL,
    rows_evaluated BIGINT      NOT NULL CHECK (rows_evaluated >= 0),
    rows_violated  BIGINT      NOT NULL CHECK (rows_violated >= 0),
    violation_rate NUMERIC(18,8) NOT NULL CHECK (violation_rate >= 0 AND violation_rate <= 1),
    passed         BOOLEAN     NOT NULL,
    severity       TEXT        NOT NULL CHECK (severity IN ('WARN', 'FAIL')),
    sample_json    JSONB,
    evaluated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (run_id, rule_id),
    CONSTRAINT dq_violations_within_evaluated CHECK (rows_violated <= rows_evaluated)
);

CREATE INDEX idx_dq_result_failed ON control.dq_result (dataset_name, evaluated_at DESC)
    WHERE NOT passed;

-- ---------------------------------------------------------------------------
-- Lineage as a directed graph.
-- ---------------------------------------------------------------------------
CREATE TABLE control.lineage_node (
    node_id    BIGSERIAL   PRIMARY KEY,
    node_name  TEXT        NOT NULL,
    node_type  TEXT        NOT NULL CHECK (node_type IN ('dataset', 'job', 'external')),
    node_layer TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (node_name, node_type)
);

CREATE TABLE control.lineage_edge (
    edge_id        BIGSERIAL   PRIMARY KEY,
    run_id         BIGINT      REFERENCES control.etl_run (run_id) ON DELETE SET NULL,
    source_node_id BIGINT      NOT NULL REFERENCES control.lineage_node (node_id),
    target_node_id BIGINT      NOT NULL REFERENCES control.lineage_node (node_id),
    edge_type      TEXT        NOT NULL CHECK (edge_type IN ('reads', 'writes', 'derives')),
    column_mapping JSONB,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT lineage_no_self_edge CHECK (source_node_id <> target_node_id)
);

CREATE INDEX idx_lineage_edge_source ON control.lineage_edge (source_node_id);
CREATE INDEX idx_lineage_edge_target ON control.lineage_edge (target_node_id);
CREATE INDEX idx_lineage_edge_run    ON control.lineage_edge (run_id);

-- ---------------------------------------------------------------------------
-- Streaming epochs.
--
-- Spark's foreachBatch batch_id is scoped to one checkpoint lineage: a fresh
-- checkpoint restarts numbering, so batch_id alone is not globally unique and,
-- worse, a new query writing batch_id=0 would lose the version comparison
-- against an existing batch_id=42 and silently fail to replace it.
--
-- The epoch is allocated from a sequence, so it is monotonic by construction.
-- Allocation is INSERT ... ON CONFLICT DO NOTHING keyed on checkpoint_id:
-- a fresh checkpoint gets exactly one epoch, a restart reuses it.
-- ---------------------------------------------------------------------------
CREATE SEQUENCE control.stream_epoch_seq AS BIGINT START WITH 1 INCREMENT BY 1;

CREATE TABLE control.stream_epoch (
    checkpoint_id   TEXT        PRIMARY KEY,
    epoch           BIGINT      NOT NULL DEFAULT nextval('control.stream_epoch_seq'),
    stream_query_id TEXT,
    allocated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (epoch),
    CONSTRAINT stream_epoch_positive CHECK (epoch > 0)
);

-- ---------------------------------------------------------------------------
-- Benchmark environment metadata + audit trail.
--
-- A measured result is worthless without the conditions that produced it, and a
-- comparison is invalid unless both sides consumed identical input -- hence
-- input_fingerprint, which the reporter refuses to compare across.
-- ---------------------------------------------------------------------------
CREATE TABLE control.benchmark_run (
    benchmark_run_id    BIGSERIAL   PRIMARY KEY,
    run_id              BIGINT      NOT NULL REFERENCES control.etl_run (run_id) ON DELETE CASCADE,
    experiment          TEXT        NOT NULL
        CHECK (experiment IN ('A_execution', 'B_storage')),
    config_label        TEXT        NOT NULL,
    -- Non-null for ablation-ladder / leave-one-out steps.
    ablation_step       TEXT,
    iteration_index     INTEGER     NOT NULL CHECK (iteration_index >= 0),
    -- Actual position in the alternating execution sequence, so the run order
    -- can be audited after the fact rather than assumed.
    sequence_position   INTEGER     NOT NULL CHECK (sequence_position >= 0),
    is_warmup           BOOLEAN     NOT NULL DEFAULT FALSE,
    cache_policy        TEXT        NOT NULL DEFAULT 'warm'
        CHECK (cache_policy IN ('warm', 'cold')),

    -- Comparison validity
    input_fingerprint   TEXT        NOT NULL,
    input_row_count     BIGINT      NOT NULL CHECK (input_row_count >= 0),
    input_bytes         BIGINT      NOT NULL CHECK (input_bytes >= 0),
    input_file_count    INTEGER     NOT NULL CHECK (input_file_count >= 0),
    output_fingerprint  TEXT,
    correctness_passed  BOOLEAN,

    -- Environment
    spark_version       TEXT        NOT NULL,
    java_version        TEXT        NOT NULL,
    clickhouse_version  TEXT,
    docker_memory_bytes BIGINT,
    worker_count        INTEGER,
    total_cores         INTEGER,
    executor_memory     TEXT,
    executor_overhead   TEXT,
    git_commit          TEXT,

    started_at          TIMESTAMPTZ NOT NULL,
    finished_at         TIMESTAMPTZ,
    -- Zero and negative durations are rejected outright: they indicate a
    -- broken measurement, not a very fast run.
    duration_ms         BIGINT      CHECK (duration_ms IS NULL OR duration_ms > 0),

    UNIQUE (experiment, config_label, ablation_step, iteration_index, is_warmup)
);

CREATE INDEX idx_benchmark_config ON control.benchmark_run (experiment, config_label);

-- ---------------------------------------------------------------------------
-- Curated serving tables.
-- ---------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS serving;

CREATE TABLE serving.dim_taxi_zone (
    location_id  INTEGER PRIMARY KEY,
    borough      TEXT NOT NULL,
    zone_name    TEXT NOT NULL,
    service_zone TEXT
);

CREATE TABLE serving.daily_kpi (
    kpi_date         DATE   NOT NULL,
    vendor_name      TEXT   NOT NULL,
    trip_count       BIGINT NOT NULL,
    total_revenue    NUMERIC(18,2) NOT NULL,
    avg_fare         NUMERIC(12,4) NOT NULL,
    avg_distance_mi  NUMERIC(12,4) NOT NULL,
    avg_duration_min NUMERIC(12,4) NOT NULL,
    avg_tip_pct      NUMERIC(9,6),
    loaded_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (kpi_date, vendor_name)
);

-- ---------------------------------------------------------------------------
-- Reporting views.
-- ---------------------------------------------------------------------------

CREATE VIEW control.v_latest_run AS
SELECT DISTINCT ON (r.job_name)
       r.job_name, r.run_id, r.status, r.started_at, r.duration_ms,
       r.rows_read, r.rows_written, r.rows_rejected,
       (SELECT count(*) FROM control.dq_result d WHERE d.run_id = r.run_id)                  AS dq_rules_run,
       (SELECT count(*) FROM control.dq_result d WHERE d.run_id = r.run_id AND NOT d.passed) AS dq_rules_failed
FROM control.etl_run r
ORDER BY r.job_name, r.started_at DESC;

-- Warm-up runs are excluded; only measured iterations reach the statistics.
CREATE VIEW control.v_benchmark_summary AS
SELECT experiment,
       config_label,
       ablation_step,
       count(*)                                            AS measured_runs,
       round(avg(duration_ms))                             AS mean_duration_ms,
       round(percentile_cont(0.5) WITHIN GROUP (ORDER BY duration_ms)::numeric) AS median_duration_ms,
       min(duration_ms)                                    AS min_duration_ms,
       max(duration_ms)                                    AS max_duration_ms,
       round(stddev_samp(duration_ms), 1)                  AS stddev_duration_ms,
       round(avg(input_row_count / NULLIF(duration_ms, 0) * 1000.0)) AS mean_rows_per_sec,
       count(DISTINCT input_fingerprint)                   AS distinct_input_fingerprints,
       bool_and(COALESCE(correctness_passed, FALSE))       AS all_correctness_passed
FROM control.benchmark_run
WHERE NOT is_warmup AND duration_ms IS NOT NULL
GROUP BY experiment, config_label, ablation_step;

CREATE VIEW control.v_lineage AS
SELECT e.edge_id, s.node_name AS source, s.node_type AS source_type,
       t.node_name AS target, t.node_type AS target_type,
       e.edge_type, e.run_id, e.created_at
FROM control.lineage_edge e
JOIN control.lineage_node s ON s.node_id = e.source_node_id
JOIN control.lineage_node t ON t.node_id = e.target_node_id;

-- Committed units: the manifest is the authority, status is bookkeeping.
-- A row here with status <> 'COMPLETE' is a unit awaiting status repair.
CREATE VIEW control.v_committed_unit AS
SELECT m.dataset_name, m.pipeline_stage, m.processing_unit,
       m.row_count, m.file_count, m.total_bytes, m.schema_hash,
       m.source_fingerprint, m.target_path, m.published_at,
       p.status AS unit_status,
       (p.status <> 'COMPLETE') AS needs_status_repair
FROM control.unit_manifest m
JOIN control.processing_unit p
  ON  p.dataset_name    = m.dataset_name
  AND p.pipeline_stage  = m.pipeline_stage
  AND p.processing_unit = m.processing_unit;
