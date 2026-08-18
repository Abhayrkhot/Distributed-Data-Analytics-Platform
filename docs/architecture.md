# Architecture

Design decisions and their failure semantics. The README covers what the platform does; this
covers *why it is shaped this way* and *what happens when it breaks*.

---

## The commit protocol

### The manifest is the commit record

Publishing files into the deterministic target path does **not** commit a processing unit. The
`control.unit_manifest` row does.

A unit is committed only when all four hold:

1. staged output has been validated
2. target publication has completed
3. the published target has been verified against the staged fingerprint
4. the `unit_manifest` row has been persisted

`processing_unit.status = COMPLETE` is bookkeeping that *follows* the commit record.

```
claim processing_unit RUNNING (leased)
  → write to ATTEMPT-SPECIFIC staging:  /data/staging/<stage>/<dataset>/<unit>/<run_id>/
  → validate staged output (row count, schema hash, file count)
  → promote to DETERMINISTIC target
  → verify published target against the staged fingerprint
  → INSERT unit_manifest          ← COMMIT POINT (atomic)
  → mark COMPLETE                 ← bookkeeping
```

**Why this shape.** A directory replace on a bind-mounted filesystem is not one atomic operation.
Rather than claim an atomicity the filesystem cannot provide, the commit point is moved onto a
single-row Postgres insert, which genuinely is atomic. The filesystem then only has to be
*recoverable*, not transactional.

**What is actually guaranteed:** the pipeline never incrementally writes into a live partition —
data is fully materialized and validated in staging first — and every interruption point has a
recovery test.

**What is not guaranteed:** that promotion itself is atomic. It tries `ATOMIC_MOVE` and falls back
to a recursive copy across filesystems, and that fallback is explicitly not atomic. Which is
*why* the manifest, not the target, is the commit record.

### Files in the target are never trusted alone

An uncommitted target is indistinguishable from a partial write. It is always discarded, never
adopted. Adopting it would mean treating "bytes exist" as "the write finished" — the assumption
that turns a crash into silent data loss.

### Recovery matrix

On lease expiry the next run reconciles the target against the manifest. **The manifest's presence,
not the target's, decides whether the unit was committed.**

| Observed | Meaning | Action |
|---|---|---|
| no manifest, no/partial target | not committed | retry from scratch |
| **no manifest, target present** | **not committed** — files exist, commit record does not | discard target, retry |
| manifest present, fingerprint matches | **committed**; only the status write was lost | repair `status = COMPLETE`, skip |
| manifest present, fingerprint mismatch | committed record contradicts target | discard both, rebuild from source |

Each branch has a crash-injection test in `PublishProtocolIT`, including a crash placed *between*
the manifest insert and the status update — which must recover as **repair**, not reprocess.

A subtlety worth stating: discarding a contradictory manifest is not enough on its own. The unit
must also be reset out of `COMPLETE`, or the claim statement refuses it and the corruption is
detected and then **impossible to repair** — strictly worse than not detecting it. That was a real
bug the matrix caught.

### Processing unit state machine

```
PENDING ──claim──► RUNNING ──commit──► COMPLETE
                      │                    │
                      ├──fail────► FAILED  │
                      └──expire──► FAILED  │
                                     │     │
                              claim──┘     │
                                           │
              force_rebuild ───────────────┴──► PENDING
```

`COMPLETE` is terminal under normal operation. Reprocessing a committed unit requires an explicit
`FORCE_REBUILD`, so discarding committed data is always deliberate rather than the result of a
retry loop.

Claiming is a **single atomic statement**. Read-then-decide-then-write leaves a window where two
workers both claim the same unit — with workers starting together, that is not rare, it happens
almost every time.

---

## Data quality

### The gate runs before publication, on staged output

```
transform → write staging → evaluate DQ on the staged data
          → blocking breach? abort; target untouched
          → promote → manifest → COMPLETE
```

Both placements matter. Evaluating the in-memory DataFrame would test a plan Spark might recompute
differently on write. Evaluating after promotion would mean invalid data is already served when
the alarm fires.

### Null policy is declared, not inferred

Spark SQL is three-valued: `NULL > 0` is neither true nor false, so a naive range check silently
passes every null. A column that became **entirely null** would satisfy all of its range rules.

Every rule declares one of:

| Policy | NULL treated as | Denominator |
|---|---|---|
| `violation` | a failure | all rows |
| `pass` | acceptable | all rows |
| `ignore` | not judged | non-null rows only |

All three are defensible, which is exactly why it must be declared. `not_null` deliberately ignores
the policy — a rule *about* nullness must not be configurable to permit nulls.

### Threshold semantics are explicit

A bare `0.05` is ambiguous: five percent of rows allowed to fail, or ninety-five percent required
to pass? The type is stored alongside the value (`max_violation_fraction` or
`max_violation_count`), and the comparison is strictly greater-than — a threshold of "at most 5 bad
rows" passes on exactly 5.

### One scan, not nineteen

Column-wise rules compile into a pair of conditional-count expressions and are aggregated together.
On the real dataset that is the difference between DQ running every time and being switched off
when it gets slow — and a check that gets switched off is not a check. A test asserts the batched
result equals evaluating each rule alone.

---

## Streaming

### Delivery semantics

**At-least-once into an idempotent sink that converges deterministically.** Not exactly-once.

A crash between ClickHouse accepting a batch and Spark committing checkpoint progress redelivers
that batch. Claiming exactly-once would assert a transaction boundary that does not exist across
those two systems.

Redelivery is harmless because of two properties, both tested:

1. the `foreachBatch` body is a **pure function** of its input — no `now()`, `rand()` or UUID
   anywhere in the aggregation path, so a replayed batch produces byte-identical rows
2. the sink is a `ReplacingMergeTree` keyed on the window with a **monotonic version**

### Why the epoch exists

Spark's `batch_id` is scoped to one checkpoint lineage. A fresh checkpoint restarts numbering at 0
— so `batch_id = 0` from a new query would **lose** the version comparison against an existing
`batch_id = 42`, and the replacement would silently not happen. The row would look updated and
would not be.

```
version = stream_epoch × 2³² + batch_id
```

The epoch comes from a Postgres sequence, so it is monotonic by construction. Allocation is a
single `INSERT ... ON CONFLICT DO NOTHING` keyed on the checkpoint location: a genuinely fresh
checkpoint gets exactly one epoch, a restart reuses it.

### Physical versus logical

Physical duplicate rows **may transiently exist** before ClickHouse merges. That is not claimed
away. What converges is the *logical* result.

**Spark cannot express `FINAL`.** Its parser has no such modifier, so `SELECT ... FROM t FINAL`
parses `FINAL` as a table *alias*: the query is accepted, runs against the raw table, and returns
duplicates with no error. A Spark consumer must use `max_by(value, version)`; a ClickHouse-native
client can use `FINAL`.

An earlier revision of the replacement test suite passed while checking nothing for exactly this
reason. There is now a test asserting the limitation, so a future version that gains support turns
it red rather than the docs quietly becoming wrong.

---

## Schema evolution

Canonicalize → hash → diff → classify.

Canonical form is recursive: lowercase field name, canonical type spelling, nullable flag, sorted
by name, joined as `name:type:nullable`, SHA-256. Spark's `simpleString()` embeds nested field
names in declaration order, so hashing *that* would make `struct<a,b>` and `struct<b,a>` differ
despite being the same type.

| Class | Cases |
|---|---|
| `additive` | new nullable column |
| `widening` | `byte→short→int→long`, `float→double`, decimal precision growth, `required→nullable` |
| `breaking` | `long→int`, `double→int`, `string→numeric`, `timestamp→date`, `nullable→required`, column removal |

Fail-closed: anything not proven safe is breaking. `int → double` is breaking, because it is lossy
above 2⁵³.

**Silver's schema is pinned** by its explicit `select()`, so it cannot drift because bronze gained
a column. That is deliberate — silver is a published contract — and it means the evolution the
registry guards against there is a change to the transformation itself, not to its input.

---

## Benchmark methodology

### No target percentage exists in the code

The reporter emits whatever it measures and **refuses to emit anything** when the measurement is
inadmissible: differing input fingerprints, a failed correctness gate, or only warm-up runs.

### Three honest configurations

A tempting baseline is "Spark defaults with AQE disabled". That is dishonest — **AQE is on by
default in Spark 3.5**, so disabling it is a handicap, not a default.

| Config | Meaning |
|---|---|
| `spark_default` | Spark 3.5's actual out-of-the-box behaviour |
| `naive_app` | a plausible first attempt: no pruning, no broadcast hint |
| `optimized` | tuned |

All three appear in the report, and the headline **names its baseline** rather than saying
"improved Spark performance by X%", which invites the reader to assume the largest gap.

### Discipline

- **correctness gate before timing** — a config that reads fewer rows is not faster at the same
  work, and every such result *looks* like a win
- **warm-ups first**, so no config pays JIT cost on a live run
- **alternating order**, so cache and thermal drift is not attributed to whichever ran first
- **medians**, so one disturbed Docker run cannot move the figure
- **compression only in Experiment B** — enforced by a constructor that throws, because mixing a
  layout change into an execution measurement makes the number attributable to neither

---

## Known limitations

Stated because absence of a caveat reads as a claim.

| Limitation | Consequence |
|---|---|
| No measured performance figure yet | The harness is verified; a real number needs `run-bench.sh` on the full dataset |
| `UnitFingerprint` covers path and size, not content bytes | Detects truncation, missing parts, stale extras — not same-size corruption |
| `trip_key` is derived | TLC has no unique trip id; tests measure duplicate-key rate, not hash collisions |
| Batch reference in `StreamRecoveryIT` shares `WindowAggregator` | Proves interruption doesn't change the result, not that the aggregation is correct — that is covered separately against hand-computed values |
| Containment re-check in `StagingCleaner` is redundant | `Files.walk` already does not follow symlinks; kept as defence in depth, explicitly not claimed |
| `agg_zone_hourly` is 1:1 with silver in the fixture | Grouping is exercised by the other three aggregates |

---

## Operational notes

**Docker VM memory is the binding constraint.** Spark, Postgres, ClickHouse and Kafka share one
7.65 GB VM; the core stack is budgeted to ~6.75 GB. Executor memory plus overhead must stay under
the advertised worker memory or executors are never scheduled and the app hangs in `WAITING`.

**`clickhouse-jdbc` is deliberately absent.** Its SQL lexer is ANTLR-4.13-generated and Spark 3.5's
parsers are 4.9.3-generated. 4.13 breaks Spark; 4.9.3 breaks the driver. They cannot share a JVM,
and the `-all` jar bundles 224 ANTLR classes un-relocated so no Maven exclusion can fix it —
resolution reports 4.9.3 while the shaded copies shadow it. ClickHouse is reached through the
native Spark connector for writes and its HTTP interface for anything needing `FINAL`.

**Do not keep this repository in an iCloud-synced directory.** iCloud duplicates files it observes
changing mid-write, producing `Foo 2.java` beside `Foo.java`. That breaks compilation and can be
committed. `.gitignore` has no effect on it.
