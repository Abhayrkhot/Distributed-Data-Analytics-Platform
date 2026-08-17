#!/usr/bin/env bash
#
# Integration verification: the boundaries unit tests cannot reach.
#
# Runs *IT classes against the live stack. Deliberately separate from
# ./scripts/test.sh so the development loop never requires Docker.
#
# Host-facing URLs are substituted here: Spark jobs resolve `postgres` and
# `clickhouse` inside the compose network, but these tests run on the host and
# have to go through the published ports.
#
#   ./scripts/test-integration.sh
#
set -uo pipefail

source "$(dirname "${BASH_SOURCE[0]:-$0}")/env.sh"
cd "$PROJECT_ROOT"

echo "== preflight: services must be healthy =="
missing=0
for service in postgres clickhouse; do
    container="ap-$service"
    health="$(docker inspect --format='{{.State.Health.Status}}' "$container" 2>/dev/null)"
    if [ "$health" = "healthy" ]; then
        printf '  ok    %-12s healthy\n' "$service"
    else
        printf '  FAIL  %-12s %s\n' "$service" "${health:-not running}"
        missing=1
    fi
done
if [ "$missing" -ne 0 ]; then
    echo
    echo "start the stack first:  docker compose --env-file .env -f docker/docker-compose.yml up -d" >&2
    exit 1
fi

echo
echo "== preflight: control-plane schema present =="
tables=$(pg -tAc "SELECT count(*) FROM information_schema.tables WHERE table_schema='control'" 2>/dev/null | tr -d '[:space:]')
if [ "${tables:-0}" -lt 9 ]; then
    echo "  FAIL  expected at least 9 control tables, found ${tables:-0}" >&2
    echo "        the Postgres volume may need reinitializing" >&2
    exit 1
fi
printf '  ok    %s control tables present\n' "$tables"

echo
echo "== integration tests =="
# Point the config at the published ports for the duration of this run only.
PG_JDBC_URL="$PG_JDBC_URL_LOCAL" \
CH_JDBC_URL="$CH_JDBC_URL_LOCAL" \
KAFKA_BOOTSTRAP="$KAFKA_BOOTSTRAP_HOST" \
    mvn -B -Pintegration test "$@"
status=$?

echo
if [ "$status" -eq 0 ]; then
    echo "== leak check: integration rows cleaned up =="
    # Every IT tags its rows with an IT- prefixed job name and deletes them in
    # @AfterEach. Anything left behind means a cleanup path did not run, which
    # would let one test's residue affect the next (§51).
    leaked=$(pg -tAc "SELECT count(*) FROM control.etl_run WHERE job_name LIKE 'IT-%'" 2>/dev/null | tr -d '[:space:]')
    if [ "${leaked:-0}" -eq 0 ]; then
        echo "  ok    no integration rows left behind"
    else
        echo "  WARN  ${leaked} integration run(s) not cleaned up" >&2
        status=1
    fi
fi

exit "$status"
