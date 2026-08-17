#!/usr/bin/env bash
#
# Tier 0 / §43 · SQL constraint verification.
#
# The verification contract requires that impossible states be rejected by the
# database even if application-level validation contains a bug. So this suite is
# mostly NEGATIVE: it asserts that invalid writes are refused, not merely that
# valid ones succeed. Testing only the happy path would pass against a schema
# with no constraints at all.
#
# Every case runs inside its own transaction and rolls back, so the suite is
# idempotent and leaves no state behind.
#
#   ./scripts/test-sql-constraints.sh
#
set -uo pipefail

# Credentials come from .env via env.sh and travel as environment variables,
# never as command-line arguments.
# shellcheck source=scripts/env.sh
source "$(dirname "${BASH_SOURCE[0]}")/env.sh"

psql_run() { pg_stdin -v ON_ERROR_STOP=1 -q; }

PASS=0
FAIL=0

# Fixture rows every test can rely on, created inside the same aborting
# transaction so nothing persists.
FIXTURE="
INSERT INTO control.etl_run (run_uuid, job_name, status, ended_at, duration_ms)
VALUES ('00000000-0000-0000-0000-0000000000aa', 'FixtureJob', 'SUCCESS', now(), 1);
INSERT INTO control.processing_unit (dataset_name, pipeline_stage, processing_unit, status)
VALUES ('bronze.trip_raw', 'raw_to_bronze', 'yellow/2024-01', 'PENDING');
"

# expect_reject <name> <sql> [expected substring in error]
expect_reject() {
    local name="$1" sql="$2" want="${3:-}"
    local err
    err=$(psql_run <<SQL 2>&1
BEGIN;
$FIXTURE
$sql
ROLLBACK;
SQL
)
    if [ $? -eq 0 ]; then
        printf '  \033[31mFAIL\033[0m  %-46s accepted an invalid write\n' "$name"
        FAIL=$((FAIL + 1)); return
    fi
    if [ -n "$want" ] && ! grep -qi -- "$want" <<<"$err"; then
        printf '  \033[31mFAIL\033[0m  %-46s rejected, but not by "%s"\n' "$name" "$want"
        printf '        got: %s\n' "$(head -1 <<<"$err")"
        FAIL=$((FAIL + 1)); return
    fi
    printf '  \033[32mok\033[0m    %-46s rejected\n' "$name"
    PASS=$((PASS + 1))
}

# expect_accept <name> <sql>
expect_accept() {
    local name="$1" sql="$2"
    local err
    err=$(psql_run <<SQL 2>&1
BEGIN;
$FIXTURE
$sql
ROLLBACK;
SQL
)
    if [ $? -ne 0 ]; then
        printf '  \033[31mFAIL\033[0m  %-46s rejected a valid write\n' "$name"
        printf '        got: %s\n' "$(head -2 <<<"$err" | tail -1)"
        FAIL=$((FAIL + 1)); return
    fi
    printf '  \033[32mok\033[0m    %-46s accepted\n' "$name"
    PASS=$((PASS + 1))
}

echo "== processing_unit: state machine and lease =="

expect_reject "invalid status" "
INSERT INTO control.processing_unit (dataset_name, pipeline_stage, processing_unit, status)
VALUES ('bronze.trip_raw', 'raw_to_bronze', 'yellow/2024-02', 'DONE');" \
    "processing_unit_status_check"

expect_reject "invalid pipeline_stage" "
INSERT INTO control.processing_unit (dataset_name, pipeline_stage, processing_unit, status)
VALUES ('bronze.trip_raw', 'raw_to_platinum', 'yellow/2024-02', 'PENDING');" \
    "pipeline_stage_check"

expect_reject "duplicate processing-unit PK" "
INSERT INTO control.processing_unit (dataset_name, pipeline_stage, processing_unit, status)
VALUES ('bronze.trip_raw', 'raw_to_bronze', 'yellow/2024-01', 'PENDING');" \
    "processing_unit_pkey"

# The lease invariant: at most one valid owner, and only while RUNNING.
expect_reject "RUNNING without a lease" "
INSERT INTO control.processing_unit (dataset_name, pipeline_stage, processing_unit, status)
VALUES ('bronze.trip_raw', 'raw_to_bronze', 'yellow/2024-03', 'RUNNING');" \
    "pu_lease_iff_running"

expect_reject "COMPLETE holding a stale lease" "
INSERT INTO control.processing_unit
    (dataset_name, pipeline_stage, processing_unit, status, lease_owner, lease_expires_at)
VALUES ('bronze.trip_raw', 'raw_to_bronze', 'yellow/2024-03', 'COMPLETE', 'run-9', now());" \
    "pu_lease_iff_running"

expect_reject "negative attempt_count" "
INSERT INTO control.processing_unit
    (dataset_name, pipeline_stage, processing_unit, status, attempt_count)
VALUES ('bronze.trip_raw', 'raw_to_bronze', 'yellow/2024-03', 'PENDING', -1);" \
    "attempt_count"

expect_accept "RUNNING with a valid lease" "
INSERT INTO control.processing_unit
    (dataset_name, pipeline_stage, processing_unit, status, lease_owner, lease_expires_at)
VALUES ('bronze.trip_raw', 'raw_to_bronze', 'yellow/2024-03', 'RUNNING',
        'run-9', now() + interval '5 min');"

# Bronze COMPLETE and silver FAILED for the same logical unit must coexist --
# this is the whole reason the table is keyed per stage.
expect_accept "same unit, two stages, divergent status" "
UPDATE control.processing_unit SET status = 'COMPLETE'
 WHERE processing_unit = 'yellow/2024-01';
INSERT INTO control.processing_unit (dataset_name, pipeline_stage, processing_unit, status)
VALUES ('silver.trip_clean', 'bronze_to_silver', 'yellow/2024-01', 'FAILED');"

echo
echo "== unit_manifest: the commit record =="

expect_reject "manifest without a processing unit" "
INSERT INTO control.unit_manifest
    (dataset_name, pipeline_stage, processing_unit, run_id, schema_hash,
     row_count, file_count, total_bytes, source_fingerprint, target_path)
SELECT 'bronze.trip_raw', 'raw_to_bronze', 'ghost/2099-01', run_id, 'h', 1, 1, 1, 'f', '/t'
  FROM control.etl_run WHERE job_name = 'FixtureJob';" \
    "unit_manifest_dataset_name_pipeline_stage_processing_unit_fkey"

expect_reject "negative row_count" "
INSERT INTO control.unit_manifest
    (dataset_name, pipeline_stage, processing_unit, run_id, schema_hash,
     row_count, file_count, total_bytes, source_fingerprint, target_path)
SELECT 'bronze.trip_raw', 'raw_to_bronze', 'yellow/2024-01', run_id, 'h', -1, 1, 1, 'f', '/t'
  FROM control.etl_run WHERE job_name = 'FixtureJob';" \
    "row_count"

# One commit per unit per stage: this uniqueness is what makes the commit a
# single atomic insert rather than a read-then-write race.
expect_reject "double commit for one unit" "
INSERT INTO control.unit_manifest
    (dataset_name, pipeline_stage, processing_unit, run_id, schema_hash,
     row_count, file_count, total_bytes, source_fingerprint, target_path)
SELECT 'bronze.trip_raw', 'raw_to_bronze', 'yellow/2024-01', run_id, 'h', 10, 1, 99, 'f', '/t'
  FROM control.etl_run WHERE job_name = 'FixtureJob';
INSERT INTO control.unit_manifest
    (dataset_name, pipeline_stage, processing_unit, run_id, schema_hash,
     row_count, file_count, total_bytes, source_fingerprint, target_path)
SELECT 'bronze.trip_raw', 'raw_to_bronze', 'yellow/2024-01', run_id, 'h2', 20, 2, 88, 'f2', '/t'
  FROM control.etl_run WHERE job_name = 'FixtureJob';" \
    "unit_manifest_dataset_name_pipeline_stage_processing_unit_key"

echo
echo "== dq_rule: threshold and null-policy semantics =="

expect_reject "invalid threshold_type" "
INSERT INTO control.dq_rule (rule_name, dataset_name, rule_type, threshold_type, threshold_value)
VALUES ('t1', 'silver.trip_clean', 'range', 'percent_ok', 0.5);" \
    "threshold_type_check"

expect_reject "invalid null_policy" "
INSERT INTO control.dq_rule (rule_name, dataset_name, rule_type, null_policy)
VALUES ('t2', 'silver.trip_clean', 'range', 'maybe');" \
    "null_policy_check"

expect_reject "invalid rule_type" "
INSERT INTO control.dq_rule (rule_name, dataset_name, rule_type)
VALUES ('t3', 'silver.trip_clean', 'vibes');" \
    "rule_type_check"

# A violation FRACTION above 1.0 is meaningless -- almost always a typo for a count.
expect_reject "fraction threshold above 1.0" "
INSERT INTO control.dq_rule (rule_name, dataset_name, rule_type, threshold_type, threshold_value)
VALUES ('t4', 'silver.trip_clean', 'range', 'max_violation_fraction', 5);" \
    "dq_fraction_bounded"

# A COUNT threshold must be a whole number of rows.
expect_reject "fractional row-count threshold" "
INSERT INTO control.dq_rule (rule_name, dataset_name, rule_type, threshold_type, threshold_value)
VALUES ('t5', 'silver.trip_clean', 'range', 'max_violation_count', 10.5);" \
    "dq_count_is_integral"

expect_reject "negative threshold" "
INSERT INTO control.dq_rule (rule_name, dataset_name, rule_type, threshold_value)
VALUES ('t6', 'silver.trip_clean', 'range', -0.1);" \
    "threshold_value"

expect_accept "count threshold of 1000 rows" "
INSERT INTO control.dq_rule (rule_name, dataset_name, rule_type, threshold_type, threshold_value)
VALUES ('t7', 'silver.trip_clean', 'range', 'max_violation_count', 1000);"

echo
echo "== dq_result =="

expect_reject "more violations than rows evaluated" "
INSERT INTO control.dq_result
    (run_id, rule_id, dataset_name, rows_evaluated, rows_violated, violation_rate, passed, severity)
SELECT r.run_id, q.rule_id, 'silver.trip_clean', 100, 101, 1.0, FALSE, 'FAIL'
  FROM control.etl_run r, control.dq_rule q
 WHERE r.job_name = 'FixtureJob' AND q.rule_name = 'silver_trip_key_unique';" \
    "dq_violations_within_evaluated"

expect_reject "violation_rate above 1.0" "
INSERT INTO control.dq_result
    (run_id, rule_id, dataset_name, rows_evaluated, rows_violated, violation_rate, passed, severity)
SELECT r.run_id, q.rule_id, 'silver.trip_clean', 100, 10, 1.5, FALSE, 'FAIL'
  FROM control.etl_run r, control.dq_rule q
 WHERE r.job_name = 'FixtureJob' AND q.rule_name = 'silver_trip_key_unique';" \
    "violation_rate"

echo
echo "== etl_run: failure metadata cannot be lost or faked =="

expect_reject "FAILED without an error_class" "
INSERT INTO control.etl_run (run_uuid, job_name, status, ended_at)
VALUES ('00000000-0000-0000-0000-0000000000bb', 'J', 'FAILED', now());" \
    "etl_run_failed_has_error"

expect_reject "SUCCESS carrying an error" "
INSERT INTO control.etl_run (run_uuid, job_name, status, ended_at, error_class)
VALUES ('00000000-0000-0000-0000-0000000000cc', 'J', 'SUCCESS', now(), 'java.io.IOException');" \
    "etl_run_success_has_no_error"

expect_reject "terminal status with no end time" "
INSERT INTO control.etl_run (run_uuid, job_name, status)
VALUES ('00000000-0000-0000-0000-0000000000dd', 'J', 'SUCCESS');" \
    "etl_run_terminal_has_end"

echo
echo "== etl_run_metric: retry scopes are distinct rows =="

expect_reject "invalid attempt_scope" "
INSERT INTO control.etl_run_metric (run_id, metric_name, metric_value, attempt_scope)
SELECT run_id, 'shuffle_read_bytes', 1, 'best_effort' FROM control.etl_run WHERE job_name='FixtureJob';" \
    "attempt_scope_check"

# The same metric under both scopes is legal and expected; a duplicate within
# one scope is not.
expect_accept "same metric under both scopes" "
INSERT INTO control.etl_run_metric (run_id, metric_name, metric_value, attempt_scope)
SELECT run_id, 'shuffle_read_bytes', 100, 'all_attempts'    FROM control.etl_run WHERE job_name='FixtureJob';
INSERT INTO control.etl_run_metric (run_id, metric_name, metric_value, attempt_scope)
SELECT run_id, 'shuffle_read_bytes', 80,  'successful_only' FROM control.etl_run WHERE job_name='FixtureJob';"

expect_reject "duplicate metric within one scope" "
INSERT INTO control.etl_run_metric (run_id, metric_name, metric_value, attempt_scope)
SELECT run_id, 'task_count', 1, 'all_attempts' FROM control.etl_run WHERE job_name='FixtureJob';
INSERT INTO control.etl_run_metric (run_id, metric_name, metric_value, attempt_scope)
SELECT run_id, 'task_count', 2, 'all_attempts' FROM control.etl_run WHERE job_name='FixtureJob';" \
    "etl_run_metric_run_id_metric_name_attempt_scope_key"

echo
echo "== stream_epoch: monotonic and unique =="

expect_reject "duplicate epoch" "
INSERT INTO control.stream_epoch (checkpoint_id, epoch) VALUES ('cp-a', 7);
INSERT INTO control.stream_epoch (checkpoint_id, epoch) VALUES ('cp-b', 7);" \
    "stream_epoch_epoch_key"

expect_reject "non-positive epoch" "
INSERT INTO control.stream_epoch (checkpoint_id, epoch) VALUES ('cp-c', 0);" \
    "stream_epoch_positive"

# Restarting against the same checkpoint must reuse its epoch, not allocate a new
# one -- this is the ON CONFLICT path the streaming job depends on.
expect_accept "same checkpoint reuses its epoch" "
INSERT INTO control.stream_epoch (checkpoint_id) VALUES ('cp-reuse');
INSERT INTO control.stream_epoch (checkpoint_id) VALUES ('cp-reuse') ON CONFLICT (checkpoint_id) DO NOTHING;
DO \$\$ BEGIN
  IF (SELECT count(*) FROM control.stream_epoch WHERE checkpoint_id='cp-reuse') <> 1
  THEN RAISE EXCEPTION 'checkpoint allocated more than one epoch'; END IF;
END \$\$;"

echo
echo "== benchmark_run: a broken measurement is not a fast one =="

expect_reject "zero duration" "
INSERT INTO control.benchmark_run
    (run_id, experiment, config_label, iteration_index, sequence_position,
     input_fingerprint, input_row_count, input_bytes, input_file_count,
     spark_version, java_version, started_at, duration_ms)
SELECT run_id, 'A_execution', 'optimized', 1, 1, 'fp', 10, 10, 1, '3.5.9', '17', now(), 0
  FROM control.etl_run WHERE job_name='FixtureJob';" \
    "duration_ms"

expect_reject "negative duration" "
INSERT INTO control.benchmark_run
    (run_id, experiment, config_label, iteration_index, sequence_position,
     input_fingerprint, input_row_count, input_bytes, input_file_count,
     spark_version, java_version, started_at, duration_ms)
SELECT run_id, 'A_execution', 'optimized', 1, 1, 'fp', 10, 10, 1, '3.5.9', '17', now(), -5
  FROM control.etl_run WHERE job_name='FixtureJob';" \
    "duration_ms"

expect_reject "unknown experiment" "
INSERT INTO control.benchmark_run
    (run_id, experiment, config_label, iteration_index, sequence_position,
     input_fingerprint, input_row_count, input_bytes, input_file_count,
     spark_version, java_version, started_at)
SELECT run_id, 'C_vibes', 'optimized', 1, 1, 'fp', 10, 10, 1, '3.5.9', '17', now()
  FROM control.etl_run WHERE job_name='FixtureJob';" \
    "experiment_check"

echo
echo "== lineage =="

expect_reject "self-referential edge" "
INSERT INTO control.lineage_edge (source_node_id, target_node_id, edge_type)
SELECT node_id, node_id, 'derives' FROM control.lineage_node LIMIT 1;" \
    "lineage_no_self_edge"

echo
printf '\n  %d passed, %d failed\n\n' "$PASS" "$FAIL"
[ "$FAIL" -eq 0 ] || exit 1
