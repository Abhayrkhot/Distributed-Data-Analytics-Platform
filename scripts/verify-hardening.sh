#!/usr/bin/env bash
#
# §45–47 · Tier 3 adversarial hardening.
#
# Deliberately NOT part of verify-all. These stages are slow and some are
# probabilistic, and a release gate that takes an hour is a release gate people
# route around. Tier 3 deepens confidence; it does not block unrelated work.
#
# Any defect found here becomes a deterministic regression test in Tier 0/1/2 —
# a Tier 3 failure is a lead, not a permanent home for the check.
#
#   ./scripts/verify-hardening.sh              # all stages
#   ./scripts/verify-hardening.sh nondeterminism
#
set -uo pipefail

source "$(dirname "${BASH_SOURCE[0]:-$0}")/env.sh"
cd "$PROJECT_ROOT"

ONLY="${1:-all}"
FAILED=()

stage() {
    local name="$1"; shift
    if [ "$ONLY" != "all" ] && [ "$ONLY" != "$name" ]; then
        return
    fi
    echo
    echo "== $name =="
    if "$@"; then
        printf '  \033[32mPASS\033[0m  %s\n' "$name"
    else
        printf '  \033[31mFAIL\033[0m  %s\n' "$name"
        FAILED+=("$name")
    fi
}

# ---------------------------------------------------------------------------
# §47 Nondeterminism detection
#
# Runs the same suite repeatedly from identical state. Anything that varies
# between runs — unordered collections, races, partition ordering, a stray
# timestamp — shows up as an intermittent failure rather than as a clean bug.
# ---------------------------------------------------------------------------
nondeterminism() {
    local runs="${HARDENING_RUNS:-10}"
    local failures=0

    echo "  running the deterministic suite $runs times from identical state"
    for i in $(seq 1 "$runs"); do
        if mvn -B -q -Djacoco.skip=true \
              -Dtest='CanonicalSchemaTest,TripKeyTest,SilverTransformTest,WindowAggregatorTest,GoldenDatasetTest' \
              -Dsurefire.failIfNoSpecifiedTests=false test >/tmp/nd_$i.log 2>&1; then
            printf '.'
        else
            printf 'X'
            failures=$((failures + 1))
            echo
            echo "  run $i failed - see /tmp/nd_$i.log"
            grep -A4 '<<< \(FAILURE\|ERROR\)!' /tmp/nd_$i.log | head -12
        fi
    done
    echo
    if [ "$failures" -eq 0 ]; then
        echo "  $runs/$runs identical - no nondeterminism observed"
        return 0
    fi
    echo "  $failures of $runs runs disagreed"
    return 1
}

# ---------------------------------------------------------------------------
# §45 Resource pressure
#
# Correctness must not depend on having enough memory. Under constraint the job
# either produces the same answer or fails explicitly — what it must never do is
# silently emit partial output.
# ---------------------------------------------------------------------------
resource_pressure() {
    echo "  re-running component tests under a constrained heap and high partition count"
    MAVEN_OPTS="-Xmx512m" mvn -B -q -Djacoco.skip=true \
        -Dspark.sql.shuffle.partitions=200 \
        -Dtest='SilverTransformTest,WindowAggregatorTest' \
        -Dsurefire.failIfNoSpecifiedTests=false test 2>&1 | tail -20
    local status=${PIPESTATUS[0]}
    if [ "$status" -eq 0 ]; then
        echo "  identical results under memory pressure"
    else
        echo "  FAILED under pressure - if this is an OOM the job failed explicitly,"
        echo "  which is correct; if results DIFFERED that is a real defect"
    fi
    return $status
}

# ---------------------------------------------------------------------------
# §19 Mutation expansion
#
# The full mutation suite, which verify-all already runs. Repeated here so a
# hardening pass is self-contained.
# ---------------------------------------------------------------------------
mutation_expansion() {
    ./scripts/test-the-tests.sh
}

# ---------------------------------------------------------------------------
# §17 High-volume property testing
#
# jqwik defaults to 1000 tries. This raises it so the generators explore corners
# a normal run would not reach.
# ---------------------------------------------------------------------------
property_expansion() {
    echo "  running property tests at 10x the usual try count"
    mvn -B -q -Djacoco.skip=true \
        -Djqwik.tries.default=10000 \
        -Dtest='SchemaProperties,StreamVersionProperties,TripKeyProperties,ProcessingUnitStateProperties' \
        -Dsurefire.failIfNoSpecifiedTests=false test 2>&1 | tail -20
    return ${PIPESTATUS[0]}
}

# ---------------------------------------------------------------------------
# §36 Invariants after stress
#
# The control plane must still be consistent after everything above has churned
# through it. This is where a concurrency bug that left bad state shows up.
# ---------------------------------------------------------------------------
invariants_after_stress() {
    ./scripts/verify-platform.sh
}

echo "Tier 3 adversarial hardening"
echo "  slow and partly probabilistic by design; not part of verify-all"

stage "nondeterminism"          nondeterminism
stage "property-expansion"      property_expansion
stage "resource-pressure"       resource_pressure
stage "mutation-expansion"      mutation_expansion
stage "invariants-after-stress" invariants_after_stress

echo
if [ ${#FAILED[@]} -eq 0 ]; then
    echo "hardening: all stages passed"
    exit 0
fi
echo "hardening: ${#FAILED[@]} stage(s) failed:"
for name in "${FAILED[@]}"; do
    echo "  - $name"
done
echo
echo "A Tier 3 failure is a lead, not a home. Turn it into a deterministic"
echo "regression test in Tier 0/1/2 before fixing it."
exit 1
