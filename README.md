# Distributed Data Analytics Platform

[![CI](https://github.com/Abhayrkhot/Distributed-Data-Analytics-Platform/actions/workflows/ci.yml/badge.svg)](https://github.com/Abhayrkhot/Distributed-Data-Analytics-Platform/actions/workflows/ci.yml)

A batch and streaming analytics platform over NYC taxi data. Spark pipelines land in
ClickHouse for analytics and Postgres for governance metadata, with a Kafka streaming path
alongside.

It processes **6.5 million real rows**, and the thing it is actually built to demonstrate is
not the pipeline — it is that **every claim it makes is backed by a test that fails when the
claim stops being true.**

---

## Results

| | |
|---|---|
| **Rows ingested** | 6,496,401 real NYC TLC records |
| **Rows published to silver** | 6,262,830 (96.4% — 233,571 rejected with recorded reasons) |
| **Measured speedup** | **14.4%** — median 1706ms → 1460ms |
| **Full-pipeline rerun** | every committed unit skipped in 6.4s |
| **Tests** | 749 |
| **Mutation guarantees** | 44 — each a guarantee deliberately broken to prove its test catches it |
| **Global invariants** | 19, checked against the live control plane |

The performance figure is **14.4%, not a round number**, measured over 6.3M rows with five
iterations per configuration. A larger figure was available by comparing against a
deliberately handicapped baseline, and was not used. Full report:
[`docs/results/benchmark.md`](docs/results/benchmark.md).

---

## Architecture

```
  NYC TLC parquet
  ┌──────────────┐
  │ yellow 2024  │──┐
  │ yellow 2025  │  │   ┌─────────┐   ┌─────────┐   ┌────────┐   ┌──────────────┐
  │ green  2024  │──┼──►│ BRONZE  │──►│ SILVER  │──►│  GOLD  │──►│  ClickHouse  │
  │ zone lookup  │──┘   │         │   │         │   │        │   │   (marts)    │
  └──────────────┘      │ conform │   │ reject  │   │ 4 aggs │   ├──────────────┤
                        │ nothing │   │ dedupe  │   │        │   │   Postgres   │
                        │ dropped │   │ derive  │   │        │   │  (serving)   │
                        └─────────┘   │ enrich  │   └────────┘   └──────────────┘
                                      └────┬────┘
                                           │  ▲
                                    DQ GATE│  │ blocks publication
                                           ▼  │ if a FAIL rule breaches
  ┌──────────┐   ┌──────────────┐   ┌──────────────────┐
  │  Kafka   │──►│  STRUCTURED  │──►│   ClickHouse     │
  │  events  │   │  STREAMING   │   │ ReplacingMerge   │
  └──────────┘   │ 5-min windows│   │ Tree (versioned) │
                 └──────────────┘   └──────────────────┘

  ┌──────────────────── Postgres control plane ─────────────────────┐
  │  etl_run · etl_run_metric · dq_rule · dq_result                 │
  │  processing_unit · unit_manifest · schema_version               │
  │  lineage_node · lineage_edge · stream_epoch · benchmark_run     │
  └─────────────────────────────────────────────────────────────────┘
```

Postgres is **not** a second copy of the warehouse. It is the metadata plane — what ran, what
it read and wrote, whether the data passed its rules, how datasets derive from one another,
and how schemas changed. That separation is what makes the governance claims checkable rather
than decorative.

| Module | Responsibility |
|---|---|
| `platform-common` | control plane, schema registry, trip key, stream version, run context |
| `platform-ingest` | staged-publish protocol, source normalization, bronze job, staging cleaner |
| `platform-transform` | silver transform, DQ engine, gold aggregates, serving writer |
| `platform-stream` | event envelope, epoch allocation, windowed consumer |
| `platform-bench` | benchmark harness, correctness gate, report writer |

---

## How the results were arrived at

This is the part worth reading. The numbers above are only meaningful because of how they
were produced.

### The benchmark refuses to report a number it cannot defend

Before any timing is accepted, both configurations must produce **byte-identical output** over
**identically fingerprinted input**. If they diverge, the reporter emits no headline at all and
says why.

That gate earned its place. On real data it rejected the first run:

```
NO HEADLINE: [correctness gate failed for [optimized]]
```

The cause was a genuine error: the "partition pruning" optimization was a `WHERE year IN
(2024, 2025)` filter, and the January 2024 file contains rows with pickup years **2002, 2009
and 2023** — corrupt meter timestamps. The optimized configuration was faster partly because
it silently processed *less data*, and produced a different answer.

Without the gate, that ships as a performance win. The filter is now part of the workload for
every configuration; the flag controls only *where* it runs.

### The baseline is honest

A tempting baseline is "Spark defaults with adaptive execution disabled". That would be
dishonest — **AQE is on by default in Spark 3.5**, so disabling it is a handicap, not a
default. Three configurations are reported, and the headline names which one it compares
against:

| config | median | mean | sd | n |
|---|---|---|---|---|
| `naive_app` (baseline) | 1706ms | 1720ms | 82.0ms | 5 |
| `spark_default` | 1682ms | 1739ms | 113.1ms | 5 |
| `optimized` | **1460ms** | 1433ms | 48.8ms | 5 |

Improvement is computed from **medians**, because Docker on a laptop produces occasional
outliers and one disturbed run should not move the figure.

### Expected outputs were written before the code

The golden dataset in [`tests/golden/`](tests/golden/) was computed **by hand before the
transformations existed**. Silver satisfied it on the first run. Nothing was back-filled from
actual output — and when later rule changes were made, the golden expectations stayed
unchanged, which is what confirmed those rows were legitimately valid.

### Every guarantee is deliberately broken to prove its test catches it

`./scripts/test-the-tests.sh` breaks 44 guarantees one at a time and asserts the defending test
goes red. A test that stays green against a broken implementation is a defect in the test.

This has caught real problems, including two of my own tests that were passing vacuously.

### Real data found what 19 rows could not

Every suite passed against a hand-written fixture. Running on 6.5M real rows immediately
surfaced four defects the fixture structurally could not contain:

- **Null fares slipping through the filter.** `fare_amount < 0` evaluates to *NULL*, not true,
  when the fare is null — so null-fare rows passed silver's rejection and were caught
  downstream. The exact three-valued-logic trap the data-quality null policy was built to
  expose, appearing in production data.
- **48,231 rows (0.76%)** reporting sub-30-second or multi-day durations — meter faults.
- **One row with an $863,372.12 fare for a 1.6-mile trip.** Left in, it would have dominated
  every revenue aggregate it touched.
- **The data-quality gate passing having evaluated zero rules**, because a dataset name matched
  nothing. On a real deployment, a typo would mean DQ checks nothing and publishes.

---

## The three decisions that shaped the design

### 1. The commit point is a Postgres insert, not files on disk

Writing files into the target does **not** commit a unit — the `unit_manifest` row does. This
moves atomicity off a filesystem that cannot promise an atomic directory swap and onto one that
can.

The consequence: **files in the target are never trusted alone.** An uncommitted target is
indistinguishable from a partial write, so it is discarded rather than adopted. Adopting it
would mean treating "bytes exist" as "the write finished" — the assumption that turns a crash
into silent data loss.

A seven-site failpoint matrix crashes the protocol at each boundary and asserts what the next
run does.

### 2. Data quality gates publication, evaluated on staged output

```
transform → write staging → evaluate DQ on the staged data
          → blocking breach? abort; target untouched
          → promote → manifest (COMMIT) → COMPLETE → lineage
```

Evaluating in memory would test a plan Spark might recompute differently on write. Evaluating
after promotion would mean invalid data is already served when the alarm fires.

### 3. Streaming is at-least-once, not exactly-once

A crash between ClickHouse accepting a batch and Spark committing checkpoint progress
redelivers that batch. Claiming exactly-once would assert a transaction boundary that does not
exist across those two systems.

What makes redelivery harmless is tested: the `foreachBatch` body is a pure function of its
input, and the sink is a `ReplacingMergeTree` with a monotonic version. The strongest test
asserts **streaming final state == batch-computed final state**, including after a mid-stream
kill.

---

## Claim → evidence

| Claim | Backed by |
|---|---|
| **Schema evolution** | `SchemaRegistryIT` — the real 2024→2025 `cbd_congestion_fee` addition; `SilverSchemaEvolutionIT` proves a breaking change publishes nothing |
| **Data-quality enforcement** | `DqEngineTest` (rule × null-policy × cardinality), `SilverDqGateIT` — a breached FAIL rule leaves no target, no manifest, no lineage |
| **Idempotent / restart-safe batch** | `PublishProtocolIT` — 7-site failpoint matrix, manifest reconciliation, concurrent claims |
| **Restart-safe streaming** | `StreamRecoveryIT`, `ReplacingMergeTreeIT`, `StreamEpochIT` |
| **Lineage / governance** | `LineageRecorderIT`, `E2EPipelineIT`, `verify-platform.sh` |
| **Scale** | `RealDataAcceptanceIT` — 6.5M real rows end to end |
| **Performance** | `BenchmarkIT` + correctness gate — measured 14.4% |

---

## Running it

```bash
./scripts/init-secrets.sh                              # generate .env (gitignored)
docker compose --env-file .env -f docker/docker-compose.yml up -d
./scripts/test.sh                                      # fast: unit + component
./scripts/verify-all.sh                                # the full gate
```

Requires **JDK 17** specifically — Spark 3.5 rejects newer JDKs. `scripts/env.sh` pins
`JAVA_HOME`.

To reproduce the results:

```bash
./scripts/fetch-data.sh          # 106 MB of real TLC data
./scripts/test-acceptance.sh     # the full pipeline on 6.5M rows
./scripts/run-bench.sh           # regenerates docs/results/benchmark.md
```

| Script | Scope |
|---|---|
| `test.sh` | unit + component, no services |
| `test-integration.sh` | against the live stack |
| `test-e2e.sh` | raw → bronze → silver → gold + streaming |
| `verify-platform.sh` | 19 global control-plane invariants |
| `verify-all.sh` | Tier 0 + 1 + 2 — the release gate, 8 stages |
| `verify-hardening.sh` | Tier 3: nondeterminism, resource pressure, property expansion |
| `test-the-tests.sh` | breaks 44 guarantees, asserts each test goes red |

**CI** runs the deterministic subset on every push: compile, unit and component tests, coverage
gates, secret scanning, and the SQL schema against a real Postgres. The suites needing a
five-service stack and a 106 MB download stay explicitly invoked — a gate slow enough to route
around protects nothing.

---
