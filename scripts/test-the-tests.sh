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

# This repository lives under ~/Desktop, which macOS syncs to iCloud. iCloud
# duplicates files it sees change mid-write, so a build can leave
# "Foo 3.class" beside "Foo.class" in target/. JaCoCo then aborts with
# "Can't add different class with same name", which looks like a code failure
# and is not one. Sweeping them is cheaper than diagnosing it again.
purge_icloud_duplicates() {
    find "$PROJECT_ROOT" -name '*[0-9].class' 2>/dev/null \
        | grep -E ' [0-9]+\.class$' \
        | while IFS= read -r dupe; do rm -f "$dupe"; done
}

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

# mutate_it: same contract, but runs the integration suite because the guarantee
# is only observable against a real database.
mutate_it() {
    MUTATE_PROFILE=integration mutate "$@"
    MUTATE_PROFILE=""
}

# Same contract, but for guarantees that live in platform-ingest.
mutate_ingest() {
    MUTATE_MODULE=platform-ingest mutate "$@"
    MUTATE_MODULE=""
}

mutate_ingest_it() {
    MUTATE_MODULE=platform-ingest MUTATE_PROFILE=integration mutate "$@"
    MUTATE_MODULE=""; MUTATE_PROFILE=""
}

# Guarantees that live in platform-transform.
mutate_transform() {
    MUTATE_MODULE=platform-transform mutate "$@"
    MUTATE_MODULE=""
}

mutate_transform_it() {
    MUTATE_MODULE=platform-transform MUTATE_PROFILE=integration mutate "$@"
    MUTATE_MODULE=""; MUTATE_PROFILE=""
}

# Guarantees that live in platform-stream.
mutate_stream() {
    MUTATE_MODULE=platform-stream mutate "$@"
    MUTATE_MODULE=""
}

# Guarantees that live in platform-bench.
mutate_bench() {
    MUTATE_MODULE=platform-bench mutate "$@"
    MUTATE_MODULE=""
}

# mutate <guarantee> <file> <sed-script> <test-name-that-must-fail>
mutate() {
    local guarantee="$1" file="$2" script="$3" expect="$4"
    local key
    # Unique per MUTATION, not per file. Keying on the path alone means two mutations
    # targeting the same file share a backup slot: the second cp overwrites the first's
    # backup with already-mutated content, and the restore then silently leaves the
    # source broken. That happened once and deleted a schemaRegistry.register() call.
    MUTATION_SEQ=$((${MUTATION_SEQ:-0} + 1))
    key="$(printf '%03d_%s' "$MUTATION_SEQ" "$(echo "$file" | tr '/' '_')")"

    cp "$PROJECT_ROOT/$file" "$BACKUP_DIR/$key.bak"
    printf '%s\n' "$file" > "$BACKUP_DIR/$key.path"

    sed -i '' "$script" "$PROJECT_ROOT/$file"

    if cmp -s "$PROJECT_ROOT/$file" "$BACKUP_DIR/$key.bak"; then
        printf '  \033[31mFAIL\033[0m  %-46s mutation did not apply (stale sed pattern)\n' "$guarantee"
        FAIL=$((FAIL + 1))
        return
    fi

    local output rc
    if [ "${MUTATE_PROFILE:-}" = "integration" ]; then
        output="$(PG_JDBC_URL="$PG_JDBC_URL_LOCAL" CH_JDBC_URL="$CH_JDBC_URL_LOCAL" \
                  KAFKA_BOOTSTRAP="$KAFKA_BOOTSTRAP_HOST" \
                  mvn -B -q -Pintegration -Djacoco.skip=true -pl "${MUTATE_MODULE:-platform-common}" -am -DskipTests=false test 2>&1)"
    else
        # Coverage is irrelevant to a mutation run, and skipping the JaCoCo report
        # step avoids it failing on stale duplicate class files in target/.
        output="$(mvn -B -q -Djacoco.skip=true -pl "${MUTATE_MODULE:-platform-common}" test 2>&1)"
    fi
    rc=$?
    cp "$BACKUP_DIR/$key.bak" "$PROJECT_ROOT/$file"

    # A mutation that does not COMPILE proves nothing about the tests: the suite never
    # ran. mvn -q suppresses "BUILD FAILURE" because it is INFO level, so this has to be
    # detected explicitly - otherwise a broken mutation reads as "the tests are weak",
    # which understates coverage and sends you chasing a test that was fine all along.
    if grep -q "COMPILATION ERROR\|cannot find symbol\|package .* does not exist" <<<"$output"; then
        printf '  \033[31mFAIL\033[0m  %-46s mutation did not compile (fix the mutation)\n' "$guarantee"
        FAIL=$((FAIL + 1))
        return
    fi

    if [ "$rc" -eq 0 ] && ! grep -q "Tests run:.*Failures: [1-9]\|Tests run:.*Errors: [1-9]" <<<"$output"; then
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

CONFIG=platform-common/src/main/java/com/analyticsplatform/common/config/PlatformConfig.java
RUNCTX=platform-common/src/main/java/com/analyticsplatform/common/run/RunContext.java

# Without the nesting check, staging cleanup can delete published output, or the
# whole warehouse. Both are unrecoverable and easy to configure by accident.
mutate "config: allow staging inside the data root" "$CONFIG" \
    's|        if (staging.startsWith(data)) {|        if (false) {|' \
    "staging\|Roots\|published output"

# A startup banner that prints passwords undoes the entire point of keeping them
# out of tracked files.
mutate "config: stop redacting secrets" "$CONFIG" \
    's|        values.forEach((key, value) -> out.put(key, isSecret(key) ? "\*\*\*" : value));|        values.forEach(out::put);|' \
    "Secret\|redact\|toString"

# Success must be claimed, never inferred. Defaulting to SUCCESS would record a
# job that returned early as having done its work.
mutate "run context: assume success when nothing was claimed" "$RUNCTX" \
    's|        RunOutcome outcome = claimed != null ? claimed : RunOutcome.failed(|        RunOutcome outcome = claimed != null ? claimed : RunOutcome.success(0, 0, 0); if (false) outcome = RunOutcome.failed(|' \
    "Incomplete\|markSuccess\|NeitherPath"

# The golden dataset is only evidence if editing it is detected. A fixture change
# that silently invalidated every downstream expectation would otherwise surface
# much later, as a silver test failing for reasons unrelated to silver.
mutate "golden: silently change silver revenue" \
    "tests/golden/expected_silver.csv" \
    's|,28.30,0.2000,|,29.30,0.2000,|' \
    "reconcile\|525.10\|revenue"

mutate "fixtures: remove the duplicate row" \
    "tests/fixtures/yellow_tripdata_2024-01.csv" \
    '4{/2024-01-15 08:30:00/d;}' \
    "duplicate\|reconcile\|rowCounts"

echo
echo "Guarantees that only exist against a real database:"
echo

REGISTRY=platform-common/src/main/java/com/analyticsplatform/common/schema/SchemaRegistry.java
LINEAGE=platform-common/src/main/java/com/analyticsplatform/common/dao/LineageRecorder.java
JDBCCP=platform-common/src/main/java/com/analyticsplatform/common/dao/JdbcControlPlane.java

# The whole point of the registry: a narrowing change must stop the run before
# anything is published.
mutate_it "registry: accept breaking schema changes" "$REGISTRY" \
    's|        if (diff.changeType() == ChangeType.BREAKING) {|        if (false) {|' \
    "narrowing\|breaking\|Refused\|droppedColumn"

# Without the advisory lock, two concurrent registrations both read version N and
# both insert N+1, and one dies on a unique violation.
mutate_it "registry: drop the advisory lock" "$REGISTRY" \
    's|                lockDataset(connection, datasetName);|                // lock removed|' \
    "concurrent\|Serialized"

# Without dedup, a retried job appends duplicate edges every attempt until the
# graph is mostly noise.
mutate_it "lineage: stop deduplicating edges" "$LINEAGE" \
    's|                      AND edge_type = ?)|                      AND edge_type = ? AND false)|' \
    "duplicate\|Idempotent\|idempotent"

# Without the status guard, a second contradictory outcome silently updates zero
# rows and looks recorded.
mutate_it "control plane: allow overwriting a terminal run" "$JDBCCP" \
    's|            if (updated == 0) {|            if (false) {|' \
    "doubleFinish\|RUNNING state"

echo
echo "Bronze publish protocol:"
echo

PUBLISHER=platform-ingest/src/main/java/com/analyticsplatform/ingest/publish/StagedPublisher.java
UNITSTORE=platform-ingest/src/main/java/com/analyticsplatform/ingest/publish/ProcessingUnitStore.java
NORMALIZER=platform-ingest/src/main/java/com/analyticsplatform/ingest/source/SourceNormalizer.java

# The single most dangerous shortcut: treating files in the target as committed.
# That is what turns a crash into silent data loss.
mutate_ingest_it "publish: adopt an uncommitted target" "$PUBLISHER" \
    '/discarding uncommitted target/{n;s|deleteRecursively(target);|;|;}' \
    "Uncommitted\|discard\|crashAfterPromotion"

# Detecting corruption but leaving the unit COMPLETE makes it unclaimable forever
# - strictly worse than not detecting it. This is the bug the suite already caught.
mutate_ingest_it "publish: detect corruption but skip the reset" "$UNITSTORE" \
    's|               AND (status <> .RUNNING. OR lease_expires_at < now())|               AND false|' \
    "Rebuilt\|rebuild\|Inconsistent"

# Without the WHERE guard the claim becomes a plain upsert and every concurrent
# worker believes it owns the unit.
mutate_ingest_it "publish: claim without the status guard" "$UNITSTORE" \
    's|             WHERE control.processing_unit.status IN (.PENDING., .FAILED.)|             WHERE true OR control.processing_unit.status IN (\x27PENDING\x27, \x27FAILED\x27)|' \
    "concurrent\|committedUnitIsNotClaimable\|Serialize"

# Bronze must reject nothing: a row vanishing between file and warehouse with no
# record of why is the failure silver's rules exist to make visible.
mutate_ingest "normalizer: filter invalid rows at bronze" "$NORMALIZER" \
    's|        return raw.select(projection);|        return raw.select(projection).filter("fare_amount >= 0");|' \
    "invalidRows\|NoFiltering\|retained"

echo
echo "Silver and data quality:"
echo

DQRULE=platform-transform/src/main/java/com/analyticsplatform/transform/dq/DqRule.java
DQENGINE=platform-transform/src/main/java/com/analyticsplatform/transform/dq/DqEngine.java
SILVER=platform-transform/src/main/java/com/analyticsplatform/transform/silver/SilverTransform.java
SILVERJOB=platform-transform/src/main/java/com/analyticsplatform/transform/job/SilverTransformJob.java

# > vs >= decides whether a run landing exactly on its threshold aborts.
mutate_transform "dq: threshold > becomes >=" "$DQRULE" \
    's|            case MAX_VIOLATION_COUNT -> rowsViolated > thresholdValue.longValue();|            case MAX_VIOLATION_COUNT -> rowsViolated >= thresholdValue.longValue();|' \
    "exactlyAtLimit\|countBoundary\|boundary"

# Ignoring the null policy silently lets an all-null column pass every range rule.
mutate_transform "dq: ignore the null policy" "$DQENGINE" \
    's|            case VIOLATION -> isNull.or(raw.and(isNull.unary_\$bang()));|            case VIOLATION -> raw.and(isNull.unary_$bang());|' \
    "nullPolicy\|EntirelyNull\|VIOLATION"

# A WARN that blocks turns advisory checks into outages.
mutate_transform "dq: treat WARN as blocking" "$DQENGINE" \
    's|            return !passed && rule.severity() == DqRule.Severity.FAIL;|            return !passed;|' \
    "onlyFailSeverityBlocks\|warnDoesNotBlock\|WARN"

# Zero rather than null for a zero-fare trip drags down every average containing it.
mutate_transform "silver: tip_pct zero instead of null for a zero fare" "$SILVER" \
    '193s|lit(null).cast(DataTypes.DoubleType)|lit(0.0)|' \
    "zeroFare\|tip_pct\|Golden"

# dropDuplicates picks an arbitrary survivor, so identical input can yield different output.
mutate_transform "silver: flip the dedup tiebreaker" "$SILVER" \
    '170s|asc_nulls_last|desc_nulls_last|' \
    "partialDuplicate\|survivor\|Determinis"

# Publishing before the gate means invalid data is already served when the alarm fires.
mutate_transform_it "silver job: skip the inapplicable-rule check" "$SILVERJOB" \
    's|        if (!inapplicable.isEmpty()) {|        if (false) {|' \
    "absentColumn\|Misconfiguration"

# Lineage before the commit claims a derivation that may never have happened.
mutate_transform_it "silver job: record lineage regardless of outcome" "$SILVERJOB" \
    's|            if (outcome.published()) {|            if (true) {|' \
    "NoLineage\|lineage"

echo
echo "Gold aggregates and serving:"
echo

GOLD=platform-transform/src/main/java/com/analyticsplatform/transform/gold/GoldAggregates.java
SERVING=platform-transform/src/main/java/com/analyticsplatform/transform/gold/ServingWriter.java

# A dropped group is internally consistent and looks entirely plausible alone. Only
# comparing totals to silver catches it - this is what reconciliation is for.
mutate_transform "gold: drop a group from borough_od" "$GOLD" \
    '/Borough-to-borough origin/{n;n;s|withDateParts(silver)|withDateParts(silver).filter("pickup_borough <> \x27Queens\x27")|;}' \
    "reconcil\|borough_od\|groupCounts"

# Without the window partition, every share is computed against the grand total and
# they no longer sum to 1.0 within a day.
mutate_transform "gold: revenue share without the partition" "$GOLD" \
    's|                .over(Window.partitionBy(col("pickup_date"), col("source")));|                .over(Window.partitionBy(org.apache.spark.sql.functions.lit(1)));|' \
    "sharesSumToOne\|revenue_share\|Share"

# coalesce(tip_pct, 0) reinstates exactly the bias the silver null decision removed.
mutate_transform "gold: treat a null tip_pct as zero" "$GOLD" \
    's|                        round4(avg("tip_pct")).alias("avg_tip_pct"));|                        round4(avg(org.apache.spark.sql.functions.coalesce(col("tip_pct"), org.apache.spark.sql.functions.lit(0.0)))).alias("avg_tip_pct"));|' \
    "NullAverage\|allNullGroup\|mixedGroup\|tip_pct"

# Append instead of upsert doubles the serving rows on every rerun.
mutate_transform_it "serving: append instead of upsert" "$SERVING" \
    's|                ON CONFLICT (kpi_date, vendor_name) DO UPDATE|                ON CONFLICT DO NOTHING; -- was: DO UPDATE|' \
    "Idempotent\|idempotent\|serving"

echo
echo "Streaming:"
echo

AGG=platform-stream/src/main/java/com/analyticsplatform/stream/job/WindowAggregator.java
VALIDATOR=platform-stream/src/main/java/com/analyticsplatform/stream/event/EventValidator.java

# A processing-time value in the aggregation makes every replay produce different rows,
# so the ReplacingMergeTree version starts choosing between two different answers rather
# than deduplicating one.
# rand() rather than current_timestamp(): count(current_timestamp()) is EQUIVALENT to
# count(*), since current_timestamp() is never null, so that mutant changes nothing and the
# harness correctly reported the suite staying green. Mutation testing cannot tell an
# equivalent mutant from a weak test - picking a genuinely non-equivalent mutant is the
# author's job.
mutate_stream "stream: nondeterminism in the aggregation" "$AGG" \
    's|                        sum("total_amount").alias("total_revenue_raw"),|                        sum(col("total_amount").multiply(org.apache.spark.sql.functions.rand())).alias("total_revenue_raw"),|' \
    "Determinis\|aggregatesCorrectly\|identical"

# Accepting an unknown schema version means interpreting a producer we do not understand.
mutate_stream "stream: accept any schema version" "$VALIDATOR" \
    's|                                        EventEnvelope.CURRENT_SCHEMA_VERSION)),|                                        -999)),|' \
    "unknown_schema_version\|Malformed\|everyMalformed"

# Letting rejected events into the aggregate silently understates revenue.
mutate_stream "stream: aggregate rejected events too" "$VALIDATOR" \
    's|        return classified.filter(col("reject_reason").isNull()).drop("reject_reason");|        return classified.drop("reject_reason");|' \
    "rejectedEvents\|oneBadMessage\|Malformed"

echo
echo "End-to-end seams:"
echo

BRONZEJOB=platform-ingest/src/main/java/com/analyticsplatform/ingest/job/BronzeIngestJob.java

# Registering the schema after the write means a breaking upstream change is only
# noticed once a staging directory has already been filled.
mutate_ingest_it "bronze job: register schema after publishing" "$BRONZEJOB" \
    's|            schemaRegistry.register(dataset, normalized.schema());|            // moved|' \
    "schema\|Schema\|register"

# Lineage recorded regardless of outcome claims a derivation that may never have happened.
mutate_ingest_it "bronze job: record lineage even when nothing published" "$BRONZEJOB" \
    's|            if (outcome.published()) {|            if (true) {|' \
    "ineage\|noPublicationNoLineage"

echo
echo "Benchmark honesty:"
echo

STATS=platform-bench/src/main/java/com/analyticsplatform/bench/report/BenchmarkStatistics.java
REPORT=platform-bench/src/main/java/com/analyticsplatform/bench/report/BenchmarkReport.java
OBSERVATION=platform-bench/src/main/java/com/analyticsplatform/bench/run/BenchmarkObservation.java
BENCHCFG=platform-bench/src/main/java/com/analyticsplatform/bench/config/BenchmarkConfig.java

# Reporting a headline from configs that consumed different data is the single most
# seductive false result: it always looks like a win.
mutate_bench "bench: report a headline despite differing inputs" "$REPORT" \
    's|        if (!isValid()) {|        if (false) {|' \
    "differingInput\|invalid\|Refusal"

# Counting warm-ups measures JIT and class loading, not the workload.
mutate_bench "bench: include warm-up runs in the statistics" "$OBSERVATION" \
    's|        return !warmup \&\& correctnessPassed;|        return true;|' \
    "warmup\|Warmup\|excluded"

# A zero duration is a broken measurement; averaging it in understates every config
# that contains one.
mutate_bench "bench: accept zero-duration measurements" "$STATS" \
    's|            if (duration <= 0) {|            if (false) {|' \
    "nonPositive\|broken measurement"

# Mean rather than median lets one disturbed Docker run move the headline figure.
mutate_bench "bench: compute improvement from means" "$STATS" \
    's|        return (baseline.medianMillis - medianMillis) / baseline.medianMillis;|        return (baseline.meanMillis - meanMillis) / baseline.meanMillis;|' \
    "outlier\|Outlier\|median"

# Compression in an execution config makes the resulting percentage unattributable.
mutate_bench "bench: allow compression in an execution config" "$BENCHCFG" \
    's|                if (key.contains("compression") \|\| key.contains("codec")) {|                if (false) {|' \
    "compression\|Experiment B"

echo
printf '  %d guarantee(s) verified, %d insufficient\n\n' "$PASS" "$FAIL"

echo "restoring and confirming the suite is green again..."
restore_all

# The harness edits tracked source, so a silent restore failure would leave a broken
# tree that looks fine until a later build. Compare against git rather than trusting
# the copy-back.
if ! git -C "$PROJECT_ROOT" diff --quiet -- '*.java' '*.csv' 2>/dev/null; then
    echo "  ERROR: tracked source differs from HEAD after restore:" >&2
    git -C "$PROJECT_ROOT" diff --stat -- '*.java' '*.csv' >&2
    exit 1
fi
echo "  tracked source matches HEAD"

# Restoring sources is not enough: every mutation recompiled, so target/ still holds
# classes built from mutated code. An interrupted run leaves those behind and the next
# build fails with NoClassDefFoundError on a nested class that plainly exists in the
# source - a genuinely baffling symptom. Force a clean rebuild.
echo "  rebuilding to clear mutated class output..."
mvn -B -q clean install -DskipTests -Djacoco.skip=true >/dev/null 2>&1 \
    || { echo "  ERROR: rebuild after restore failed" >&2; exit 1; }
trap - EXIT INT TERM
purge_icloud_duplicates
if mvn -B -q -Djacoco.skip=true -pl platform-common test >/dev/null 2>&1; then
    echo "  suite green after restore"
else
    echo "  ERROR: suite is not green after restore - inspect the working tree" >&2
    exit 1
fi

[ "$FAIL" -eq 0 ] || exit 1
