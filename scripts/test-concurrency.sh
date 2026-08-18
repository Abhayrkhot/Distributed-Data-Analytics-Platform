#!/usr/bin/env bash
#
# Concurrency and lease verification.
#
# Races are the defects least likely to reproduce, so these suites are worth running
# repeatedly rather than once: pass --repeat N to run them N times. A test that
# passes 1 in 10 runs is failing, not flaky.
#
#   ./scripts/test-concurrency.sh
#   ./scripts/test-concurrency.sh --repeat 10
#
set -uo pipefail
source "$(dirname "${BASH_SOURCE[0]:-$0}")/env.sh"
cd "$PROJECT_ROOT"

REPEAT=1
[ "${1:-}" = "--repeat" ] && REPEAT="${2:-5}"

echo "Concurrency and leases"
echo "  repetitions: $REPEAT"
echo

FAILED=0
for i in $(seq 1 "$REPEAT"); do
    printf '  run %d/%d ... ' "$i" "$REPEAT"
    if PG_JDBC_URL="$PG_JDBC_URL_LOCAL" CH_JDBC_URL="$CH_JDBC_URL_LOCAL" \
       KAFKA_BOOTSTRAP="$KAFKA_BOOTSTRAP_HOST" \
       mvn -B -q -Pintegration -Djacoco.skip=true \
           -Dtest='StreamEpochIT,SchemaRegistryIT' \
           -Dsurefire.failIfNoSpecifiedTests=false test >/tmp/conc_$i.log 2>&1; then
        echo "pass"
    else
        echo "FAIL (see /tmp/conc_$i.log)"
        FAILED=$((FAILED + 1))
    fi
done

echo
if [ "$FAILED" -eq 0 ]; then
    echo "  concurrency: PASS ($REPEAT/$REPEAT)"
    exit 0
fi
echo "  concurrency: FAIL ($FAILED of $REPEAT)"
echo "  A race that fails intermittently is failing, not flaky."
exit 1
