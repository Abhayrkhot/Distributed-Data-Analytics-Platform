#!/usr/bin/env bash
#
# Removes stale staging directories.
#
# Defaults to DRY RUN. This script deletes recursively, and the failure mode is not
# "cleanup did not happen" - it is "cleanup ran somewhere it should not". Requiring
# an explicit --apply means the destructive path is never the one you get by
# accident.
#
# Retention:
#   successful staging  removed once the unit is committed
#   failed staging      kept for the grace period - it is the only evidence of what
#                       a failed run produced
#   active staging      never touched, whatever its age
#
#   ./scripts/clean-staging.sh              # dry run, shows what would go
#   ./scripts/clean-staging.sh --apply      # actually delete
#   ./scripts/clean-staging.sh --apply --retain-days 7
#
set -uo pipefail

source "$(dirname "${BASH_SOURCE[0]:-$0}")/env.sh"
cd "$PROJECT_ROOT"

APPLY=false
RETAIN_DAYS=3

while [ $# -gt 0 ]; do
    case "$1" in
        --apply) APPLY=true; shift ;;
        --retain-days) RETAIN_DAYS="$2"; shift 2 ;;
        *) echo "unknown argument: $1" >&2; exit 2 ;;
    esac
done

STAGING_ROOT="$PROJECT_ROOT/data/staging"
DATA_ROOT="$PROJECT_ROOT/data/warehouse"

echo "Staging cleanup"
echo "  staging root : $STAGING_ROOT"
echo "  data root    : $DATA_ROOT"
echo "  retain       : ${RETAIN_DAYS}d"
echo "  mode         : $([ "$APPLY" = true ] && echo 'APPLY (deletes)' || echo 'dry run')"
echo

if [ ! -d "$STAGING_ROOT" ]; then
    echo "  nothing to do: staging root does not exist"
    exit 0
fi

# Units still RUNNING under a valid lease are in use; their staging must survive.
ACTIVE=$(pg -tAc "
    SELECT coalesce(staging_path, '')
      FROM control.processing_unit
     WHERE status = 'RUNNING' AND lease_expires_at > now() AND staging_path IS NOT NULL
" 2>/dev/null | grep -v '^$' || true)

if [ -n "$ACTIVE" ]; then
    echo "  active (never removed):"
    echo "$ACTIVE" | sed 's/^/    /'
    echo
fi

CUTOFF=$(( RETAIN_DAYS * 24 * 60 ))
REMOVED=0
RETAINED=0

# Attempt level only: <staging>/<stage>/<dataset>/<unit>/<owner>. Deleting anything
# above that would take other units' in-flight attempts with it.
while IFS= read -r candidate; do
    [ -z "$candidate" ] && continue

    if echo "$ACTIVE" | grep -Fxq "$candidate"; then
        RETAINED=$((RETAINED + 1))
        continue
    fi

    # Refuse anything that does not resolve inside the staging root - the shell-side
    # equivalent of StagingCleaner's containment check.
    resolved=$(cd "$candidate" 2>/dev/null && pwd -P) || continue
    staging_real=$(cd "$STAGING_ROOT" && pwd -P)
    case "$resolved" in
        "$staging_real"/*) ;;
        *) echo "  REFUSED $resolved (outside the staging root)"; continue ;;
    esac

    if [ "$APPLY" = true ]; then
        rm -rf "$candidate"
        echo "  removed  $candidate"
    else
        echo "  would remove  $candidate"
    fi
    REMOVED=$((REMOVED + 1))
done < <(find "$STAGING_ROOT" -mindepth 4 -maxdepth 4 -type d -mmin "+$CUTOFF" 2>/dev/null)

echo
printf '  %d removed, %d retained\n' "$REMOVED" "$RETAINED"
[ "$APPLY" = true ] || echo "  (dry run - nothing was deleted; pass --apply to act)"
