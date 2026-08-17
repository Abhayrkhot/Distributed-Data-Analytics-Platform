#!/usr/bin/env bash
#
# §49 · Test the tests.
#
# A green suite proves nothing on its own — it is equally consistent with tests
# that assert nothing. So for each guarantee the project claims, this script
# deliberately breaks the implementation and asserts the relevant test goes RED.
# If violating a guarantee does not turn its test red, the verification for that
# guarantee is incomplete and the claim is unsupported.
#
# Every mutation is reverted, including on interrupt.
#
#   ./scripts/test-the-tests.sh
#
set -uo pipefail

source "$(dirname "${BASH_SOURCE[0]:-$0}")/env.sh" 2>/dev/null || {
    PROJECT_ROOT="$(git rev-parse --show-toplevel)"
    export JAVA_HOME="/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home"
    export PATH="$JAVA_HOME/bin:$PATH"
}
cd "$PROJECT_ROOT"

BACKUP_DIR="$(mktemp -d)"
PASS=0
FAIL=0

restore_all() {
    if [ -d "$BACKUP_DIR" ]; then
        for saved in "$BACKUP_DIR"/*.bak; do
            [ -e "$saved" ] || continue
            target="$(head -1 "${saved%.bak}.path")"
            cp "$saved" "$PROJECT_ROOT/$target"
        done
        rm -rf "$BACKUP_DIR"
    fi
}
# Restore on any exit path, including Ctrl-C, so an interrupted run never leaves
# a deliberately broken implementation behind.
trap restore_all EXIT INT TERM

# mutate <guarantee> <file> <sed-script> <test-name-that-must-fail>
mutate() {
    local guarantee="$1" file="$2" script="$3" expect="$4"
    local key
    key="$(echo "$file" | tr '/' '_')"

    cp "$PROJECT_ROOT/$file" "$BACKUP_DIR/$key.bak"
    printf '%s\n' "$file" > "$BACKUP_DIR/$key.path"

    sed -i '' "$script" "$PROJECT_ROOT/$file"

    if cmp -s "$PROJECT_ROOT/$file" "$BACKUP_DIR/$key.bak"; then
        printf '  \033[31mFAIL\033[0m  %-46s mutation did not apply (stale sed pattern)\n' "$guarantee"
        FAIL=$((FAIL + 1))
        return
    fi

    local output
    output="$(mvn -B -q -pl platform-common test 2>&1)"
    cp "$BACKUP_DIR/$key.bak" "$PROJECT_ROOT/$file"

    if ! grep -q "Tests run:.*Failures: [1-9]\|Tests run:.*Errors: [1-9]\|BUILD FAILURE" <<<"$output"; then
        printf '  \033[31mFAIL\033[0m  %-46s broke the guarantee, suite stayed GREEN\n' "$guarantee"
        FAIL=$((FAIL + 1))
        return
    fi
    if [ -n "$expect" ] && ! grep -q "$expect" <<<"$output"; then
        printf '  \033[33mWARN\033[0m  %-46s went red, but not via %s\n' "$guarantee" "$expect"
        PASS=$((PASS + 1))
        return
    fi

    printf '  \033[32mok\033[0m    %-46s went red as expected\n' "$guarantee"
    PASS=$((PASS + 1))
}

echo "Breaking each guarantee in turn; every case must turn its test red."
echo

SCHEMA=platform-common/src/main/java/com/analyticsplatform/common/schema/SchemaCompatibility.java
CANON=platform-common/src/main/java/com/analyticsplatform/common/schema/CanonicalSchema.java
VERSION=platform-common/src/main/java/com/analyticsplatform/common/stream/StreamVersion.java
TRIPKEY=platform-common/src/main/java/com/analyticsplatform/common/key/TripKey.java
UNIT=platform-common/src/main/java/com/analyticsplatform/common/unit/ProcessingUnitState.java

# Narrowing a column must be rejected; allowing it corrupts data silently.
mutate "schema: allow long -> int" "$SCHEMA" \
    's|            return toRank > fromRank;|            return true;|' \
    "narrowingIsRejected"

# Canonicalization must sort fields; without it, field order changes the hash and
# every reordered schema looks like a new version.
mutate "schema: stop sorting canonical fields" "$CANON" \
    's|        entries.sort(Comparator.naturalOrder());|        // sort removed|' \
    "reordering"

# The epoch is what stops a fresh checkpoint (batch 0) losing to an old batch 42.
mutate "stream: drop epoch from the version" "$VERSION" \
    's|        return (epoch << EPOCH_SHIFT) \| batchId;|        return batchId + 1;|' \
    "Epoch"

# Fixed scale is what makes 12.5 and 12.50 one trip rather than two.
mutate "trip key: stop normalizing decimal scale" "$TRIPKEY" \
    's|        BigDecimal scaled = value.setScale(scale, RoundingMode.HALF_UP);|        BigDecimal scaled = value;|' \
    "equivalent"

# Length prefixing is what stops ("a","b;c") aliasing ("a;b","c").
mutate "trip key: naive concatenation, no length prefix" "$TRIPKEY" \
    's|            out.append(value.length()).append(.:.).append(value);|            out.append(value);|' \
    "Alias\|alias\|boundar"

# COMPLETE -> CLAIM would let a retry loop silently reprocess committed data.
mutate "unit state: allow claiming a COMPLETE unit" "$UNIT" \
    's|                    Status.FAILED, Status.RUNNING),|                    Status.FAILED, Status.RUNNING, Status.COMPLETE, Status.RUNNING),|' \
    "Complete\|COMPLETE\|committed"

# A refused operation must throw; returning the unchanged status silently lets a
# caller believe it committed a unit that is still FAILED.
mutate "unit state: refusal becomes a silent no-op" "$UNIT" \
    's|        return resolve(from, operation).orElseThrow(() -> new IllegalStateException(|        if (resolve(from, operation).isEmpty()) return from;\n        return resolve(from, operation).orElseThrow(() -> new IllegalStateException(|' \
    "refus\|illegal"

echo
printf '  %d guarantee(s) verified, %d insufficient\n\n' "$PASS" "$FAIL"

echo "restoring and confirming the suite is green again..."
restore_all
trap - EXIT INT TERM
if mvn -B -q -pl platform-common test >/dev/null 2>&1; then
    echo "  suite green after restore"
else
    echo "  ERROR: suite is not green after restore - inspect the working tree" >&2
    exit 1
fi

[ "$FAIL" -eq 0 ] || exit 1
