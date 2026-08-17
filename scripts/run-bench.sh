#!/usr/bin/env bash
#
# Runs the benchmark and generates docs/results/benchmark.md.
#
# The figure this produces is whatever it measures. There is no target. If it comes
# out at 18.7%, the report says 18.7%; if 3.1%, that is worth investigating rather
# than adjusting the benchmark until it says something better.
#
# The reporter refuses to emit a headline percentage when the measurement is
# inadmissible - differing input fingerprints, a failed correctness gate, or only
# warm-up runs - because a number nobody can defend is worse than no number.
#
#   ./scripts/run-bench.sh                 # standard: 1 warm-up, 5 measured
#   ./scripts/run-bench.sh --quick         # 1 warm-up, 2 measured
#
set -uo pipefail

source "$(dirname "${BASH_SOURCE[0]:-$0}")/env.sh"
cd "$PROJECT_ROOT"

PLAN="${1:-standard}"

echo "Benchmark"
echo

for service in postgres clickhouse; do
    printf '  %-12s ' "$service"
    if dc ps --format '{{.Service}} {{.Status}}' 2>/dev/null | grep -q "^$service.*healthy"; then
        echo "healthy"
    else
        echo "NOT healthy - run: dc up -d"
        exit 1
    fi
done

printf '  %-12s %s\n' "git" "$(git rev-parse --short HEAD 2>/dev/null || echo 'not a repo')"
printf '  %-12s %s\n' "plan" "$PLAN"
echo

# Quiesce the other services so their background work is not attributed to Spark.
echo "  note: Postgres, ClickHouse and Kafka share this Docker VM. Memory contention"
echo "        is real and is why spread is reported alongside the median."
echo

PG_JDBC_URL="$PG_JDBC_URL_LOCAL" \
CH_JDBC_URL="$CH_JDBC_URL_LOCAL" \
KAFKA_BOOTSTRAP="$KAFKA_BOOTSTRAP_HOST" \
BENCH_PLAN="$PLAN" \
    mvn -B -Pintegration -Djacoco.skip=true \
        -pl platform-bench \
        -Dtest='BenchmarkIT' \
        -Dsurefire.failIfNoSpecifiedTests=false \
        test
status=$?

echo
if [ $status -eq 0 ]; then
    echo "  benchmark: PASS"
    if [ -f docs/results/benchmark.md ]; then
        echo "  report:    docs/results/benchmark.md"
    fi
else
    echo "  benchmark: FAIL"
fi
exit $status
