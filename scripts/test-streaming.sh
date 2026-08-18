#!/usr/bin/env bash
#
# Streaming determinism and recovery.
#
# The strongest assertion in the project: streaming final state == batch-computed
# final state over the identical event set, including after a mid-stream stop.
#
#   ./scripts/test-streaming.sh
#
set -uo pipefail
source "$(dirname "${BASH_SOURCE[0]:-$0}")/env.sh"
cd "$PROJECT_ROOT"

echo "Streaming determinism and recovery"
echo
for service in postgres clickhouse kafka; do
    printf '  %-12s ' "$service"
    dc ps --format '{{.Service}} {{.Status}}' 2>/dev/null | grep -q "^$service.*healthy" \
        && echo "healthy" || { echo "NOT healthy - run: dc up -d"; exit 1; }
done
echo

PG_JDBC_URL="$PG_JDBC_URL_LOCAL" CH_JDBC_URL="$CH_JDBC_URL_LOCAL" \
KAFKA_BOOTSTRAP="$KAFKA_BOOTSTRAP_HOST" \
    mvn -B -Pintegration -Djacoco.skip=true \
        -pl platform-stream \
        -Dtest='StreamRecoveryIT,ReplacingMergeTreeIT,StreamEpochIT,WindowAggregatorTest' \
        -Dsurefire.failIfNoSpecifiedTests=false test
status=$?
echo
[ $status -eq 0 ] && echo "  streaming: PASS" || echo "  streaming: FAIL"
exit $status
