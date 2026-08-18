#!/usr/bin/env bash
#
# §14 · Real-data acceptance.
#
# The final proof that the small fixtures represented the real workload. Every other
# suite runs against 19 hand-written rows; this runs against ~6.5 million real ones,
# where the failures differ in kind: schema drift that actually happened, dirty
# values nobody thought to invent, and volumes that expose anything accidentally
# quadratic.
#
# Requires ./scripts/fetch-data.sh first. The test skips itself rather than failing
# when the data is absent, so an unrelated build is never forced into a 106 MB
# download.
#
#   ./scripts/test-acceptance.sh
#
set -uo pipefail

source "$(dirname "${BASH_SOURCE[0]:-$0}")/env.sh"
cd "$PROJECT_ROOT"

echo "Real-data acceptance"
echo

RAW="$PROJECT_ROOT/data/raw"
MISSING=0
for f in yellow_tripdata_2024-01.parquet yellow_tripdata_2025-01.parquet \
         green_tripdata_2024-01.parquet taxi_zone_lookup.csv; do
    printf '  %-40s ' "$f"
    if [ -f "$RAW/$f" ]; then
        echo "$(du -h "$RAW/$f" | cut -f1)"
    else
        echo "MISSING"
        MISSING=$((MISSING + 1))
    fi
done

if [ "$MISSING" -gt 0 ]; then
    echo
    echo "  $MISSING file(s) missing - run ./scripts/fetch-data.sh first"
    exit 1
fi

for service in postgres clickhouse; do
    printf '  %-40s ' "$service"
    dc ps --format '{{.Service}} {{.Status}}' 2>/dev/null | grep -q "^$service.*healthy" \
        && echo "healthy" || { echo "NOT healthy"; exit 1; }
done
echo

# A larger heap than the default: this session holds millions of rows locally rather
# than distributing them, so the constraint is the test JVM, not the Docker cluster.
PG_JDBC_URL="$PG_JDBC_URL_LOCAL" \
CH_JDBC_URL="$CH_JDBC_URL_LOCAL" \
KAFKA_BOOTSTRAP="$KAFKA_BOOTSTRAP_HOST" \
MAVEN_OPTS="-Xmx4g" \
    mvn -B -Pintegration -Djacoco.skip=true \
        -pl platform-transform \
        -Dtest='RealDataAcceptanceIT' \
        -Dsurefire.failIfNoSpecifiedTests=false \
        test
status=$?

echo
[ $status -eq 0 ] && echo "  acceptance: PASS" || echo "  acceptance: FAIL"
exit $status
