#!/usr/bin/env bash
#
# The complete small-fixture pipeline: raw -> bronze -> silver -> gold, plus the
# streaming path.
#
# Every stage has its own suite, so this is not about a stage being correct. It is
# about the SEAMS: that bronze's on-disk output is the shape silver expects, that
# row counts survive each hop, that the lineage graph joins up across jobs rather
# than forming disconnected pairs, and that manifests and processing-unit statuses
# agree everywhere.
#
# Those are the failures a per-stage suite structurally cannot see, because each
# stage is tested against a fixture rather than against its real upstream.
#
#   ./scripts/test-e2e.sh
#
set -uo pipefail

source "$(dirname "${BASH_SOURCE[0]:-$0}")/env.sh"
cd "$PROJECT_ROOT"

echo "End-to-end pipeline"
echo

for service in postgres clickhouse kafka; do
    printf '  %-12s ' "$service"
    if dc ps --format '{{.Service}} {{.Status}}' 2>/dev/null | grep -q "^$service.*healthy"; then
        echo "healthy"
    else
        echo "NOT healthy - run: dc up -d"
        exit 1
    fi
done
echo

PG_JDBC_URL="$PG_JDBC_URL_LOCAL" \
CH_JDBC_URL="$CH_JDBC_URL_LOCAL" \
KAFKA_BOOTSTRAP="$KAFKA_BOOTSTRAP_HOST" \
    mvn -B -Pintegration -Djacoco.skip=true \
        -Dtest='E2EPipelineIT,StreamRecoveryIT' \
        -Dsurefire.failIfNoSpecifiedTests=false \
        test
status=$?

echo
if [ $status -eq 0 ]; then
    echo "  end-to-end: PASS"
else
    echo "  end-to-end: FAIL"
fi
exit $status
