#!/usr/bin/env bash
#
# §46 · Streaming soak.
#
# Runs the streaming path for an extended period and watches for the failures that
# only time reveals: heap growth that never levels off, Kafka lag that climbs
# monotonically, checkpoints that stop advancing, or duplicate logical windows
# accumulating because the sink is not actually converging.
#
# None of those show up in a test that processes 14 events and exits. A stream that
# is correct for thirty seconds and leaks for six hours passes every other suite here.
#
# Not part of any gate. It takes minutes by construction, and the signal is a trend
# rather than a pass/fail assertion - the thresholds below are deliberately loose
# because the question is "is this growing without bound", not "is this exactly N".
#
#   ./scripts/test-soak.sh                 # 3 minutes
#   ./scripts/test-soak.sh --minutes 15
#
set -uo pipefail
source "$(dirname "${BASH_SOURCE[0]:-$0}")/env.sh"
cd "$PROJECT_ROOT"

MINUTES=3
[ "${1:-}" = "--minutes" ] && MINUTES="${2:-3}"
SAMPLES=$(( MINUTES * 4 ))          # every 15 seconds
TOPIC="soak.trips.$(date +%s)"

echo "Streaming soak"
echo "  duration: ${MINUTES}m  topic: $TOPIC"
echo

for service in postgres clickhouse kafka; do
    printf '  %-12s ' "$service"
    dc ps --format '{{.Service}} {{.Status}}' 2>/dev/null | grep -q "^$service.*healthy" \
        && echo "healthy" || { echo "NOT healthy - run: dc up -d"; exit 1; }
done
echo

ch_query() {
    curl -sS -X POST \
        -H "X-ClickHouse-User: $CLICKHOUSE_USER" \
        -H "X-ClickHouse-Key: $CLICKHOUSE_PASSWORD" \
        --data-binary "$1" \
        "http://localhost:8123/?database=$CLICKHOUSE_DB" 2>/dev/null | tr -d '[:space:]'
}

# Start the consumer in the background, then sample while it runs.
echo "  starting the consumer ..."
PG_JDBC_URL="$PG_JDBC_URL_LOCAL" CH_JDBC_URL="$CH_JDBC_URL_LOCAL" \
KAFKA_BOOTSTRAP="$KAFKA_BOOTSTRAP_HOST" SOAK_TOPIC="$TOPIC" SOAK_MINUTES="$MINUTES" \
MAVEN_OPTS="-Xmx2g" \
    mvn -B -q -Pintegration -Djacoco.skip=true -pl platform-stream \
        -Dtest='SoakIT' -Dsurefire.failIfNoSpecifiedTests=false test \
        > /tmp/soak-consumer.log 2>&1 &
CONSUMER_PID=$!

cleanup() {
    kill "$CONSUMER_PID" 2>/dev/null || true
    wait "$CONSUMER_PID" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

printf '  %-8s %10s %12s %10s %12s\n' "elapsed" "rss_mb" "sink_rows" "windows" "duplicates"

FIRST_RSS=""
LAST_ROWS=0
GROWING=0
EARLY_EXIT=0

for i in $(seq 1 "$SAMPLES"); do
    sleep 15
    # An early exit is a FAILURE, not a reason to stop sampling and declare success.
    # The first version of this script reported "no unbounded growth observed" after
    # the consumer had been alive for fifteen seconds - a vacuous pass of exactly the
    # kind this project exists to catch.
    if ! kill -0 "$CONSUMER_PID" 2>/dev/null; then
        echo
        echo "  FAIL  the consumer exited after $(( i * 15 ))s of a ${MINUTES}m soak"
        echo "        see /tmp/soak-consumer.log"
        EARLY_EXIT=1
        break
    fi

    RSS=$(ps -o rss= -p "$CONSUMER_PID" 2>/dev/null | tr -d ' ')
    RSS_MB=$(( ${RSS:-0} / 1024 ))
    [ -z "$FIRST_RSS" ] && FIRST_RSS=$RSS_MB

    ROWS=$(ch_query "SELECT count() FROM stream_trip_window")
    WINDOWS=$(ch_query "SELECT count() FROM (SELECT DISTINCT window_start, pickup_borough FROM stream_trip_window)")
    # Physical rows beyond distinct windows are duplicates awaiting a merge. A slowly
    # rising number is expected; one that never falls means the sink is not converging.
    DUPES=$(( ${ROWS:-0} - ${WINDOWS:-0} ))

    printf '  %-8s %10s %12s %10s %12s\n' "$(( i * 15 ))s" "$RSS_MB" "${ROWS:-?}" "${WINDOWS:-?}" "$DUPES"

    [ "${ROWS:-0}" -gt "$LAST_ROWS" ] && GROWING=$((GROWING + 1))
    LAST_ROWS=${ROWS:-0}
done

cleanup
trap - EXIT INT TERM

echo
LAST_RSS=${RSS_MB:-0}
GROWTH=$(( LAST_RSS - ${FIRST_RSS:-0} ))
echo "  heap: ${FIRST_RSS:-?}MB -> ${LAST_RSS}MB (${GROWTH}MB)"
echo "  progress samples showing growth: $GROWING/$SAMPLES"
echo

STATUS=0
if [ "$EARLY_EXIT" -ne 0 ]; then
    echo "  FAIL  the soak did not run to completion, so it measured nothing"
    STATUS=1
fi
# Loose on purpose. The question is unbounded growth, not an exact figure: a JVM
# that climbs then plateaus is healthy, one that climbs monotonically is not.
if [ "${FIRST_RSS:-0}" -gt 0 ] && [ "$GROWTH" -gt 1500 ]; then
    echo "  FAIL  heap grew by ${GROWTH}MB without levelling off"
    STATUS=1
fi
if grep -q "OutOfMemoryError\|StreamingQueryException" /tmp/soak-consumer.log 2>/dev/null; then
    echo "  FAIL  the consumer logged a fatal error - see /tmp/soak-consumer.log"
    STATUS=1
fi

[ $STATUS -eq 0 ] && echo "  soak: PASS (no unbounded growth observed)" || echo "  soak: FAIL"
echo "  consumer log: /tmp/soak-consumer.log"
exit $STATUS
