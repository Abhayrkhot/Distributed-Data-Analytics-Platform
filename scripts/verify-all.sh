#!/usr/bin/env bash
#
# Tier 0 + Tier 1 + Tier 2 verification gate.
#
# Exits 0 only when every mandatory check passes. This is the command that
# decides whether a claim may be published: no passing evidence, no claim.
#
# Performance benchmarking stays separate because of runtime, but benchmark
# *correctness* validation is mandatory and belongs here once it exists.
#
#   ./scripts/verify-all.sh
#
set -uo pipefail

source "$(dirname "${BASH_SOURCE[0]:-$0}")/env.sh"
cd "$PROJECT_ROOT"

STAGES_RUN=0
STAGES_FAILED=0
FAILED_NAMES=()

# This repository lives under ~/Desktop, which macOS syncs to iCloud. iCloud
# duplicates files it observes changing mid-write, leaving "Foo 3.class" beside
# "Foo.class" in target/; JaCoCo then aborts with "Can't add different class with
# same name", which reads like a code failure and is not one.
purge_icloud_duplicates() {
    find "$PROJECT_ROOT" -name '*[0-9].class' 2>/dev/null \
        | grep -E ' [0-9]+\.class$' \
        | while IFS= read -r dupe; do rm -f "$dupe"; done
}

stage() {
    local name="$1"; shift
    STAGES_RUN=$((STAGES_RUN + 1))
    printf '\n\033[1m== %s ==\033[0m\n' "$name"
    if "$@"; then
        printf '  \033[32mPASS\033[0m  %s\n' "$name"
    else
        printf '  \033[31mFAIL\033[0m  %s\n' "$name"
        STAGES_FAILED=$((STAGES_FAILED + 1))
        FAILED_NAMES+=("$name")
    fi
}

# ---------------------------------------------------------------- stages

check_environment() {
    local ok=0
    printf '  java   %s\n' "$(java -version 2>&1 | head -1)"
    printf '  maven  %s\n' "$(mvn -v 2>/dev/null | head -1)"
    case "$(java -version 2>&1 | head -1)" in
        *'"17'*) ;;
        *) echo "  JAVA_HOME must point at JDK 17 (Spark 3.5 rejects newer)"; ok=1 ;;
    esac
    for service in postgres clickhouse; do
        local health
        health="$(docker inspect --format='{{.State.Health.Status}}' "ap-$service" 2>/dev/null)"
        printf '  %-11s %s\n' "$service" "${health:-not running}"
        [ "$health" = "healthy" ] || ok=1
    done
    return $ok
}

compile_all() {
    purge_icloud_duplicates
    mvn -B -q clean package -DskipTests
}

# Unit, component and integration tests in ONE invocation so JaCoCo accumulates
# coverage from all of them. Split across invocations, integration-only classes
# read as untested and the gate measures a fiction.
tests_and_coverage() {
    PG_JDBC_URL="$PG_JDBC_URL_LOCAL" \
    CH_JDBC_URL="$CH_JDBC_URL_LOCAL" \
    KAFKA_BOOTSTRAP="$KAFKA_BOOTSTRAP_HOST" \
        mvn -B verify -Pgates,integration
}

sql_constraints() {
    ./scripts/test-sql-constraints.sh
}

mutation_suite() {
    ./scripts/test-the-tests.sh
}

secrets() {
    ./scripts/secrets-scan.sh
}

integration_leak_check() {
    local leaked
    leaked=$(pg -tAc "SELECT count(*) FROM control.etl_run WHERE job_name LIKE 'IT-%'" 2>/dev/null | tr -d '[:space:]')
    printf '  integration rows left behind: %s\n' "${leaked:-unknown}"
    [ "${leaked:-1}" -eq 0 ]
}

# ---------------------------------------------------------------- run

echo "Tier 0 + 1 + 2 verification"

stage "environment"                check_environment
stage "compile"                    compile_all
stage "tests + coverage gates"     tests_and_coverage
stage "SQL constraint rejection"   sql_constraints
stage "test-the-tests (mutations)" mutation_suite
# The global invariant checker asks a question no component-level suite can: is the
# control plane internally consistent right now, across every table and every run?
stage "global invariants" "./scripts/verify-platform.sh"

stage "secret scan"                secrets
stage "test isolation"             integration_leak_check

echo
if [ "$STAGES_FAILED" -eq 0 ]; then
    printf '\033[32mall %d stages passed\033[0m\n\n' "$STAGES_RUN"
    exit 0
fi
printf '\033[31m%d of %d stages failed:\033[0m\n' "$STAGES_FAILED" "$STAGES_RUN"
printf '  - %s\n' "${FAILED_NAMES[@]}"
echo
exit 1
