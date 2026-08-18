# Distributed Data Analytics Platform

A batch and streaming analytics platform over NYC TLC taxi data: Spark pipelines landing in
ClickHouse for analytics and Postgres for governance metadata, with a Kafka streaming path.

The organising principle is stated up front because it drove most of the design decisions:

> **Every architectural claim corresponds to running code, every reliability claim has a failure
> test, and every performance claim corresponds to a reproducible measurement.**

Where a claim could not be backed, it was removed rather than softened. Those cases are listed in
[What is *not* claimed](#what-is-not-claimed).

---

## Quickstart

```bash
./scripts/init-secrets.sh                              # generate .env (gitignored)
docker compose --env-file .env -f docker/docker-compose.yml up -d
./scripts/test.sh                                      # fast: unit + component
./scripts/verify-all.sh                                # the full gate
```

Requires **JDK 17** specifically. Homebrew's `mvn` defaults to a newer JDK that Spark 3.5 rejects;
`scripts/env.sh` pins `JAVA_HOME` for you.

---

## Architecture

```
 TLC parquet ─┐
 (yellow,     ├─► BRONZE ──► SILVER ──► GOLD ──► ClickHouse   (analytical marts)
  green,      │   conform    reject     aggregate  Postgres    (curated serving)
  2024+2025) ─┘   nothing    dedupe
                  rejected   derive
                             enrich
       Kafka ──► STREAM ──► 5-min windows ──► ClickHouse (ReplacingMergeTree)

                     ┌──────────── Postgres control plane ────────────┐
                     │ runs · metrics · DQ results · lineage graph    │
                     │ schema registry · processing units · manifests │
                     └────────────────────────────────────────────────┘
```

Postgres is **not** a second copy of the warehouse. It is the metadata plane, and that separation
is what makes the governance claims checkable rather than decorative.

| Module | Contains |
|---|---|
| `platform-common` | control plane, schema registry, trip key, stream version, run context |
| `platform-ingest` | staged-publish protocol, source normalization, bronze job, staging cleaner |
| `platform-transform` | silver transform, DQ engine, gold aggregates, serving writer |
| `platform-stream` | event envelope, epoch allocation, windowed consumer |
| `platform-bench` | benchmark harness, correctness gate, report writer |

---

## The three decisions that shaped everything

### 1. The commit point is a Postgres insert, not files on disk

Writing files into the target does **not** commit a processing unit — the `unit_manifest` row does.

This moves atomicity off a filesystem that cannot promise an atomic directory swap and onto one
that can. The filesystem only has to be *recoverable*, not transactional.

The consequence: **files in the target are never trusted alone.** An uncommitted target is
indistinguishable from a partial write, so it is always discarded rather than adopted. Adopting it
would mean treating "bytes exist" as "the write finished" — the assumption that turns a crash into
silent data loss.

### 2. Data quality gates publication, and is evaluated on staged output

```
transform → write staging → evaluate DQ on the staged data
          → blocking breach? abort; target untouched
          → promote → manifest (COMMIT) → COMPLETE → lineage
```

Evaluating in memory would test a plan Spark might recompute differently on write. Evaluating after
promotion would mean invalid data is already served when the alarm fires.

### 3. Streaming is at-least-once, not exactly-once

A crash between ClickHouse accepting a batch and Spark committing checkpoint progress redelivers
that batch. Claiming exactly-once would assert a transaction boundary that does not exist across
those two systems.

What makes redelivery harmless is two properties, both tested: the `foreachBatch` body is a pure
function of its input, and the sink is a `ReplacingMergeTree` keyed on the window with a monotonic
version.

---

## Claim → evidence

Nothing here is asserted without a test that fails when the guarantee is broken.

| Claim | Evidence |
|---|---|
| **Schema evolution** | `SchemaCompatibilityTest`, `SchemaProperties` (jqwik), `SchemaRegistryIT` — the real TLC 2024→2025 `cbd_congestion_fee` addition; `SilverSchemaEvolutionIT` proves a breaking change publishes nothing |
| **Data-quality enforcement** | `DqEngineTest` (rule × null-policy × cardinality cross-product), `SilverDqGateIT` — a breached FAIL rule leaves no target, no manifest, no lineage |
| **Idempotent / restart-safe batch** | `PublishProtocolIT` — a 7-site failpoint matrix, manifest reconciliation, repeated retry, concurrent claims |
| **Restart-safe streaming** | `StreamRecoveryIT` — streaming final state == batch-computed final state, including after a mid-stream stop; `ReplacingMergeTreeIT` verifies replacement experimentally |
| **Lineage / governance** | `LineageRecorderIT`, `E2EPipelineIT` graph assertions, `verify-platform.sh` global invariants |
| **Performance** | `BenchmarkStatisticsTest`, `BenchmarkReportTest`, `BenchmarkIT` — see the caveat below |

Run `./scripts/test-the-tests.sh` to see each guarantee deliberately broken and its test go red.

---

## What is *not* claimed

Listed explicitly, because the absence of a claim is easy to miss.

- **No measured performance figure yet.** The harness is built and verified, but the integration
  test runs on a 14-row fixture where Spark's fixed overhead dominates — any percentage from it
  would be noise. A real figure requires `./scripts/run-bench.sh` against the full TLC dataset.
  Until that runs, there is infrastructure but no result.
- **Not exactly-once streaming.** See above.
- **Promotion is not atomic.** What is guaranteed is that the pipeline never incrementally writes
  into a live partition, and that every interruption point has a recovery test.
- **`trip_key` is a derived deduplication key**, not a source primary key. TLC data has no unique
  trip id. Tests measure the *duplicate-key rate*; SHA-256 collisions are not measurable at this
  scale and are not claimed.
- **Kryo serialization is not benchmarked.** `spark.serializer` is fixed at session creation, so
  the harness cannot vary it. Including it would have meant claiming a measurement that never
  happened.
- **`agg_zone_hourly` does not exercise grouping** in the fixture — every row falls in a distinct
  group. Grouping is covered by the other three aggregates.

---

## Reading `stream_trip_window` correctly

**Spark cannot express `FINAL`.** Its parser has no such modifier, so
`SELECT ... FROM t FINAL` parses `FINAL` as a table *alias*: the query is accepted, runs against
the raw table, and silently returns duplicates with no error.

A Spark consumer must use `max_by(value, version)` grouped on the window key. A ClickHouse-native
client can use `FINAL`. Either is correct; reading it plainly through Spark is not.

---

## Verification

```bash
./scripts/test.sh              # fast loop: unit + component
./scripts/test-integration.sh  # against the live stack
./scripts/test-e2e.sh          # raw → bronze → silver → gold + streaming
./scripts/verify-platform.sh   # global control-plane invariants
./scripts/verify-all.sh        # Tier 0 + 1 + 2 — the release gate
./scripts/verify-hardening.sh  # Tier 3: nondeterminism, resource pressure, fuzzing
./scripts/run-bench.sh         # produces docs/results/benchmark.md
```

`verify-all.sh` exits 0 only when every stage passes: environment, compile, tests + coverage gates,
SQL constraint rejection, mutation testing, global invariants, secret scan, and test isolation.

---

## Operational notes

- **Docker VM memory is the binding constraint.** Spark, Postgres, ClickHouse and Kafka share one
  7.65 GB VM; the stack is budgeted to ~6.75 GB. Raising Docker Desktop to 12 GB is the escape
  hatch if executors are OOM-killed.
- **`clickhouse-jdbc` is deliberately absent.** Its SQL lexer is ANTLR-4.13-generated and Spark
  3.5's parsers are 4.9.3-generated; the two cannot share a JVM, and the `-all` jar bundles ANTLR
  un-relocated so no exclusion can fix it. ClickHouse is reached through the native Spark connector
  for writes and its HTTP interface for anything needing `FINAL`.
- **Do not keep this repository in an iCloud-synced directory.** iCloud duplicates files it sees
  change mid-write, producing `Foo 2.java` beside `Foo.java`, which breaks compilation and can be
  committed. `.gitignore` has no effect on it.

See [`docs/architecture.md`](docs/architecture.md) for the publish protocol, failure semantics and
recovery matrix.
