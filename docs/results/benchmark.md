# Benchmark results

_Generated 2026-08-18T04:16:32.510920Z._

## Result

> Reduced runtime by 17.8% versus the naive_app configuration (median 2122ms -> 1745ms)

| | median | mean | min | max | sd | n |
|---|---|---|---|---|---|---|
| naive_app | 2122ms | 2145ms | 1833ms | 2686ms | 228.3ms | 15 |
| optimized | 1745ms | 1785ms | 1498ms | 2129ms | 227.4ms | 15 |

## Environment

A measurement without its conditions cannot be reproduced or compared against a later one.

| | |
|---|---|
| Spark | 3.5.9 |
| Java | 17.0.20 |
| ClickHouse | _not captured_ |
| Cores | 10 |
| Executor memory | _not captured_ |
| Executor overhead | _not captured_ |
| Git commit | 3bd7c51 |
| Cache policy | warm |

## All configurations

Every configuration is listed, not only the pair that produces the largest gap.

| config | median | mean | sd | cv | n |
|---|---|---|---|---|---|
| `naive_app` | 2122ms | 2145ms | 228.3ms | 10.6% | 15 |
| `optimized` | 1745ms | 1785ms | 227.4ms | 12.7% | 15 |
| `spark_default` | 2285ms | 2337ms | 382.5ms | 16.4% | 15 |

## Ablation

Marginal contribution of each step over the one before it. A single headline percentage says the tuning worked; this says which part of it did.

| step | marginal change |
|---|---|
| `naive_app` | +7.1% |
| `optimized` | +17.8% |

Cumulative order attributes credit in one arbitrary sequence. The leave-one-out rows (`L*`) bracket it from the other side.

## How to read this

- Improvement is computed from **medians**, not means. Docker on a laptop produces occasional outliers, and a median keeps one disturbed run from moving the figure.
- The headline **names its baseline**. "Improved Spark performance by X%" would invite the reader to assume the largest available gap.
- Configurations are run in **alternating order** with warm-ups excluded, so cache and JIT drift is not attributed to whichever ran first.
- Every measured run passed a **correctness gate** comparing its output against the baseline's. A configuration that computed something else is discarded rather than reported as fast.
- Compression appears only in Experiment B. Mixing a physical-layout change into an execution measurement would make the number unattributable to either.
- Postgres, ClickHouse and Kafka share the same Docker VM as Spark. Memory contention is real and is why the spread is reported alongside the median.
