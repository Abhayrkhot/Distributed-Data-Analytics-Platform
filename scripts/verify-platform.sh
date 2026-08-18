#!/usr/bin/env bash
#
# §36 · Global metadata invariants.
#
# Every other suite verifies one component against its own contract. This asks a
# different question: is the control plane internally consistent RIGHT NOW, across
# every table, for every run that has ever happened?
#
# That matters because the dangerous states are the ones no single component owns.
# A unit marked COMPLETE with no manifest is nobody's bug in particular - ingest
# thinks it committed, the manifest table thinks nothing happened, and neither
# component's tests can see the disagreement.
#
# Exits non-zero on any violation. Intended as a release gate and as something you
# can run after an incident to find out what the platform actually believes.
#
#   ./scripts/verify-platform.sh
#
set -uo pipefail

source "$(dirname "${BASH_SOURCE[0]:-$0}")/env.sh"
cd "$PROJECT_ROOT"

PASS=0
FAIL=0

# invariant <description> <sql returning violating rows> <explanation of why it matters>
invariant() {
    local description="$1" sql="$2" why="$3"
    local count
    count=$(pg -tAc "$sql" 2>/dev/null | tr -d '[:space:]')

    if [ -z "$count" ]; then
        printf '  \033[31mFAIL\033[0m  %-52s query failed\n' "$description"
        FAIL=$((FAIL + 1))
        return
    fi
    if [ "$count" = "0" ]; then
        printf '  \033[32mok\033[0m    %s\n' "$description"
        PASS=$((PASS + 1))
    else
        printf '  \033[31mFAIL\033[0m  %-52s %s violation(s)\n' "$description" "$count"
        printf '        %s\n' "$why"
        FAIL=$((FAIL + 1))
    fi
}

echo "Global metadata invariants"
echo

# ---------------------------------------------------------------- commit records

# The manifest IS the commit record. A unit claiming COMPLETE without one is
# claiming a commit that never happened.
invariant "every COMPLETE unit has a manifest" "
    SELECT count(*) FROM control.processing_unit p
     WHERE p.status = 'COMPLETE'
       AND NOT EXISTS (SELECT 1 FROM control.unit_manifest m
                        WHERE m.dataset_name = p.dataset_name
                          AND m.pipeline_stage = p.pipeline_stage
                          AND m.processing_unit = p.processing_unit)
" "a unit believes it is committed but no commit record exists"

# The reverse is NOT an error: a manifest without COMPLETE is a unit whose status
# write was lost, which reconciliation repairs. It is reported separately below.
invariant "no manifest references a missing processing unit" "
    SELECT count(*) FROM control.unit_manifest m
     WHERE NOT EXISTS (SELECT 1 FROM control.processing_unit p
                        WHERE p.dataset_name = m.dataset_name
                          AND p.pipeline_stage = m.pipeline_stage
                          AND p.processing_unit = m.processing_unit)
" "a commit record exists for a unit the control plane has never heard of"

invariant "manifests record a non-negative row count" "
    SELECT count(*) FROM control.unit_manifest WHERE row_count < 0 OR total_bytes < 0
" "a commit record claims impossible content"

# ------------------------------------------------------------------ run registry

invariant "no run is stuck RUNNING without an end time" "
    SELECT count(*) FROM control.etl_run
     WHERE status <> 'RUNNING' AND ended_at IS NULL
" "a terminal run has no end time, so its duration is unknowable"

invariant "every FAILED run records why" "
    SELECT count(*) FROM control.etl_run WHERE status = 'FAILED' AND error_class IS NULL
" "a failure with no error class cannot be diagnosed after the fact"

invariant "no SUCCESS run carries an error" "
    SELECT count(*) FROM control.etl_run
     WHERE status = 'SUCCESS' AND (error_class IS NOT NULL OR error_message IS NOT NULL)
" "a run reports success and failure simultaneously"

# ---------------------------------------------------------------------- leases

# The lease invariant the schema enforces as a CHECK; asserted again here because a
# future migration could drop the constraint without anyone noticing.
invariant "only RUNNING units hold a lease" "
    SELECT count(*) FROM control.processing_unit
     WHERE (status = 'RUNNING'  AND (lease_owner IS NULL OR lease_expires_at IS NULL))
        OR (status <> 'RUNNING' AND (lease_owner IS NOT NULL OR lease_expires_at IS NOT NULL))
" "a stale lease survives on a non-running unit, or a running unit has none"

# ------------------------------------------------------------------ data quality

# The publication gate, checked globally. If a blocking DQ failure ever coexists
# with a commit for the same run, invalid data reached the warehouse.
invariant "no committed output from a run with a blocking DQ failure" "
    SELECT count(*) FROM control.unit_manifest m
     WHERE EXISTS (SELECT 1 FROM control.dq_result d
                    WHERE d.run_id = m.run_id
                      AND NOT d.passed
                      AND d.severity = 'FAIL')
" "data was published despite a blocking data-quality failure"

invariant "DQ violations never exceed rows evaluated" "
    SELECT count(*) FROM control.dq_result WHERE rows_violated > rows_evaluated
" "more rows violated a rule than were checked against it"

invariant "DQ thresholds are internally consistent" "
    SELECT count(*) FROM control.dq_rule
     WHERE (threshold_type = 'max_violation_fraction' AND threshold_value > 1)
        OR threshold_value < 0
" "a fraction threshold above 1.0 can never be breached"

# --------------------------------------------------------------------- lineage

invariant "no lineage edge references a missing node" "
    SELECT count(*) FROM control.lineage_edge e
     WHERE NOT EXISTS (SELECT 1 FROM control.lineage_node n WHERE n.node_id = e.source_node_id)
        OR NOT EXISTS (SELECT 1 FROM control.lineage_node n WHERE n.node_id = e.target_node_id)
" "the lineage graph has dangling edges"

invariant "no self-referential lineage edge" "
    SELECT count(*) FROM control.lineage_edge WHERE source_node_id = target_node_id
" "a dataset claims to derive from itself"

# A run that published nothing must leave no lineage. A graph asserting a derivation
# that never happened is worse than no graph: it is confidently wrong.
invariant "no lineage from a run that committed nothing" "
    SELECT count(*) FROM (
        SELECT DISTINCT e.run_id FROM control.lineage_edge e
         WHERE e.run_id IS NOT NULL
           AND NOT EXISTS (SELECT 1 FROM control.unit_manifest m WHERE m.run_id = e.run_id)
           AND NOT EXISTS (SELECT 1 FROM control.benchmark_run b WHERE b.run_id = e.run_id)
    ) orphaned
" "lineage claims a derivation for a run that published nothing"

# ------------------------------------------------------------- schema registry

invariant "schema versions are contiguous per dataset" "
    SELECT count(*) FROM (
        SELECT dataset_name, max(version) AS highest, count(*) AS total
          FROM control.schema_version GROUP BY dataset_name
         HAVING max(version) <> count(*)
    ) gaps
" "a dataset's schema history has gaps, so a version cannot be reconstructed"

invariant "every published dataset has a registered schema" "
    SELECT count(*) FROM (
        SELECT DISTINCT m.dataset_name FROM control.unit_manifest m
         WHERE NOT EXISTS (SELECT 1 FROM control.schema_version s
                            WHERE s.dataset_name = m.dataset_name)
    ) unregistered
" "data was published under a schema the registry has never seen"

# ---------------------------------------------------------------------- streams

invariant "stream epochs are unique" "
    SELECT count(*) FROM (
        SELECT epoch FROM control.stream_epoch GROUP BY epoch HAVING count(*) > 1
    ) duplicated
" "two checkpoints share an epoch, so their versions can collide"

invariant "stream epochs are positive" "
    SELECT count(*) FROM control.stream_epoch WHERE epoch <= 0
" "a non-positive epoch breaks version ordering"

# ------------------------------------------------------------------ benchmarks

invariant "no benchmark records a non-positive duration" "
    SELECT count(*) FROM control.benchmark_run WHERE duration_ms IS NOT NULL AND duration_ms <= 0
" "a zero-duration measurement would understate any mean containing it"

invariant "every benchmark row records its input fingerprint" "
    SELECT count(*) FROM control.benchmark_run
     WHERE input_fingerprint IS NULL OR input_fingerprint = ''
" "a measurement whose input is unknown cannot be compared against another"

echo

# --------------------------------------------------------- advisory, not failures

# Units awaiting status repair are not violations: a crash between the manifest
# insert and the status write leaves exactly this state, and reconciliation fixes it
# on the next run. Reported so an operator can see it, not counted as a failure.
NEEDS_REPAIR=$(pg -tAc "
    SELECT count(*) FROM control.unit_manifest m
     JOIN control.processing_unit p
       ON p.dataset_name = m.dataset_name
      AND p.pipeline_stage = m.pipeline_stage
      AND p.processing_unit = m.processing_unit
     WHERE p.status <> 'COMPLETE'
" 2>/dev/null | tr -d '[:space:]')

EXPIRED=$(pg -tAc "
    SELECT count(*) FROM control.processing_unit
     WHERE status = 'RUNNING' AND lease_expires_at < now()
" 2>/dev/null | tr -d '[:space:]')

if [ "${NEEDS_REPAIR:-0}" != "0" ] || [ "${EXPIRED:-0}" != "0" ]; then
    echo "Advisory (recoverable, not violations):"
    [ "${NEEDS_REPAIR:-0}" != "0" ] && \
        echo "  ${NEEDS_REPAIR} committed unit(s) awaiting status repair - reconciliation handles this"
    [ "${EXPIRED:-0}" != "0" ] && \
        echo "  ${EXPIRED} unit(s) with an expired lease - reclaimable on the next run"
    echo
fi

printf '  %d invariant(s) held, %d violated\n' "$PASS" "$FAIL"
[ "$FAIL" -eq 0 ] || exit 1
