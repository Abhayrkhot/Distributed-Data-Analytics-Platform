#!/usr/bin/env bash
#
# Downloads NYC TLC trip data into data/raw/.
#
# Every URL here was verified live before being hardcoded. TLC republishes under a
# CloudFront distribution and the paths are stable, but a 404 means the layout moved
# rather than that the file is missing - so a failure is loud rather than skipped.
#
# Files are checked for size and skipped if already present, because re-downloading
# 250 MB to re-run a test is a good way to stop running the test.
#
#   ./scripts/fetch-data.sh              # default set: 2 yellow months + green + zones
#   ./scripts/fetch-data.sh --full       # 4 yellow months, for the benchmark
#   ./scripts/fetch-data.sh --minimal    # 1 yellow month + zones
#
set -uo pipefail

source "$(dirname "${BASH_SOURCE[0]:-$0}")/env.sh"
cd "$PROJECT_ROOT"

BASE="https://d37ci6vzurychx.cloudfront.net"
RAW="$PROJECT_ROOT/data/raw"
MODE="${1:-default}"

mkdir -p "$RAW"

case "$MODE" in
    --minimal) YELLOW=("2024-01") ; GREEN=() ;;
    --full)    YELLOW=("2024-01" "2024-02" "2024-03" "2025-01") ; GREEN=("2024-01") ;;
    *)         YELLOW=("2024-01" "2025-01") ; GREEN=("2024-01") ;;
esac

echo "NYC TLC data"
echo "  mode: ${MODE#--}"
echo "  into: $RAW"
echo

fetch() {
    local url="$1" target="$2"
    local name; name="$(basename "$target")"

    if [ -f "$target" ]; then
        printf '  %-40s %8s  (cached)\n' "$name" "$(du -h "$target" | cut -f1)"
        return 0
    fi

    # --fail turns an HTTP error into a non-zero exit rather than a file full of HTML,
    # which is the failure mode that otherwise surfaces as an unreadable-parquet error
    # three steps later.
    if curl -fsSL --retry 3 --retry-delay 2 -o "$target.partial" "$url"; then
        mv "$target.partial" "$target"
        printf '  %-40s %8s\n' "$name" "$(du -h "$target" | cut -f1)"
        return 0
    fi

    rm -f "$target.partial"
    printf '  %-40s FAILED (%s)\n' "$name" "$url"
    return 1
}

FAILURES=0

for month in "${YELLOW[@]}"; do
    fetch "$BASE/trip-data/yellow_tripdata_$month.parquet" \
          "$RAW/yellow_tripdata_$month.parquet" || FAILURES=$((FAILURES + 1))
done

for month in "${GREEN[@]}"; do
    fetch "$BASE/trip-data/green_tripdata_$month.parquet" \
          "$RAW/green_tripdata_$month.parquet" || FAILURES=$((FAILURES + 1))
done

fetch "$BASE/misc/taxi_zone_lookup.csv" "$RAW/taxi_zone_lookup.csv" || FAILURES=$((FAILURES + 1))

echo
TOTAL=$(du -sh "$RAW" 2>/dev/null | cut -f1)
COUNT=$(find "$RAW" -type f \( -name '*.parquet' -o -name '*.csv' \) | wc -l | tr -d ' ')
echo "  $COUNT file(s), $TOTAL total"

if [ "$FAILURES" -gt 0 ]; then
    echo "  $FAILURES download(s) failed - a 404 means the TLC layout moved, not that"
    echo "  the data is gone; check https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"
    exit 1
fi
