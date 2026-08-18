#!/usr/bin/env bash
#
# Core failure and recovery verification.
#
# The publish-protocol failpoint matrix, manifest reconciliation, repeated retry,
# and the DQ publication gate. These are the suites behind the idempotent and
# restart-safe claims, so this is the entry point to run when changing anything in
# the publish path.
#
#   ./scripts/test-failure.sh
#
set -uo pipefail
source "$(dirname "${BASH_SOURCE[0]:-$0}")/env.sh"
cd "$PROJECT_ROOT"

echo "Failure and recovery"
echo
for service in postgres clickhouse; do
    printf '  %-12s ' "$service"
    dc ps --format '{{.Service}} {{.Status}}' 2>/dev/null | grep -q "^$service.*healthy" \
        && echo "healthy" || { echo "NOT healthy - run: dc up -d"; exit 1; }
done
echo

PG_JDBC_URL="$PG_JDBC_URL_LOCAL" CH_JDBC_URL="$CH_JDBC_URL_LOCAL" \
KAFKA_BOOTSTRAP="$KAFKA_BOOTSTRAP_HOST" \
    mvn -B -Pintegration -Djacoco.skip=true \
        -Dtest='PublishProtocolIT,ManifestReconciliationTest,SilverDqGateIT,BronzeIngestJobIT' \
        -Dsurefire.failIfNoSpecifiedTests=false test
status=$?
echo
[ $status -eq 0 ] && echo "  failure/recovery: PASS" || echo "  failure/recovery: FAIL"
exit $status
