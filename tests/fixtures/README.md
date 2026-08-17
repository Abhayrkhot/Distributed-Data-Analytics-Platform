# Test fixtures and golden dataset

Small, deliberately awkward data. Every row exists for a stated reason; if you cannot say why a
row is here, it should not be here.

CSV rather than Parquet on purpose: §33 requires the golden dataset to be **human-verifiable**, and
a Parquet diff tells a reviewer nothing. `FixtureLoader` converts to Parquet at test time for the
paths that need it.

## Contract

**The expected outputs in `tests/golden/` were computed by hand before the transformations
existed.** That ordering is the point: the transformation must satisfy the specification, not the
specification get back-filled from whatever the transformation happened to produce. §33 forbids
regenerating expected output to make a test pass — if silver changes, either the change is a bug or
the contract genuinely moved, and either way a human decides which.

### Bronze — normalize only, reject nothing

Heterogeneous sources are conformed onto one schema. No filtering: bronze is what arrived.

| Bronze column | yellow | green |
|---|---|---|
| `pickup_ts` / `dropoff_ts` | `tpep_pickup_datetime` / `tpep_dropoff_datetime` | `lpep_*` |
| `vendor_id`, `passenger_count`, `trip_distance_mi` | direct | direct |
| `pickup_location_id` / `dropoff_location_id` | `PULocationID` / `DOLocationID` | same |
| `cbd_congestion_fee` | 2025+ only, else null | absent → null |
| `airport_fee` | `Airport_fee` | absent → null |
| `ehail_fee` / `trip_type` | absent → null | direct |
| `source` | `'yellow'` | `'green'` |

### Silver — reject, dedupe, derive, enrich

Rejection rules, applied in this order:

1. `pickup_ts` or `dropoff_ts` is null
2. `dropoff_ts <= pickup_ts`
3. `fare_amount < 0` or `total_amount < 0`
4. `trip_distance_mi < 0` or `> 300`

Then deduplicate on `trip_key`.

Derived columns:

```
trip_duration_min = (dropoff_ts - pickup_ts) seconds / 60.0
avg_speed_mph     = trip_distance_mi / (trip_duration_min / 60.0)    -- null when duration is 0
tip_pct           = tip_amount / fare_amount                          -- null when fare is 0
```

`tip_pct` is null rather than zero or infinity for a zero-fare trip. A zero-fare trip has no
meaningful tip percentage, and encoding that as `0.0` would silently drag down every average that
includes it.

Enrichment joins `taxi_zone_lookup` on pickup and dropoff location id. An id absent from the lookup
yields borough and zone `Unknown` — it is not dropped, because losing a trip because its zone is
unrecognized would be a worse error than reporting it as unknown.

Payment type labels: `1=credit_card`, `2=cash`, `3=no_charge`, `4=dispute`, `5=unknown`,
`6=voided`.

## Row inventory

### `yellow_tripdata_2024-01.csv` — 12 rows, 8 survive

| # | Pickup | Purpose | Silver |
|---|---|---|---|
| 1 | 01-15 08:30 | baseline valid trip, credit card | ✓ |
| 2 | 01-15 09:00 | valid, cash, zero tip | ✓ |
| 3 | 01-15 08:30 | **exact duplicate of row 1** | dropped (dedupe) |
| 4 | 01-15 23:45 | **crosses midnight** into 01-16 | ✓ |
| 5 | 01-16 10:00 | **null passenger_count** | ✓ |
| 6 | 01-16 11:00 | **zero distance** — valid, speed 0 | ✓ |
| 7 | 01-16 12:00 | **negative fare** | rejected (rule 3) |
| 8 | 01-16 13:00 | **dropoff before pickup** | rejected (rule 2) |
| 9 | 01-17 07:00 | **unknown pickup zone** (264) | ✓ as Unknown |
| 10 | 01-17 08:00 | **zero fare with a tip** → `tip_pct` null | ✓ |
| 11 | 01-17 09:00 | long trip, 120 min / 60 mi, tolls + airport fee | ✓ |
| 12 | 01-17 10:00 | **null dropoff_ts** | rejected (rule 1) |

Row 3 is an *exact* copy deliberately. A partial duplicate would make the golden output depend on
the dedupe tiebreaker, which is a separate concern with its own unit test — the golden file should
not silently encode it.

### `yellow_tripdata_2025-01.csv` — 3 rows, all survive

Carries `cbd_congestion_fee`, the column NYC added in 2025. This is the real schema-evolution case:
registering this against the 2024 schema must classify as `additive`.

### `green_tripdata_2024-01.csv` — 4 rows, 3 survive

Uses `lpep_*` timestamps, plus `ehail_fee` and `trip_type`, which yellow does not have. Row 4 has a
500-mile distance and is rejected by rule 4.

### `taxi_zone_lookup.csv` — 9 zones

Covers Manhattan, Queens, Brooklyn, and both `Unknown` sentinels (264 `NV`, 265 `Outside of NYC`)
that real TLC data uses.

## Totals

| Stage | Rows |
|---|---|
| bronze | 19 (12 + 3 + 4, nothing rejected) |
| silver | 14 (8 + 3 + 3) |
| rejected | 4 — negative fare, inverted timestamps, null dropoff, 500-mile trip |
| deduplicated | 1 |

19 − 4 rejected − 1 duplicate = 14. This arithmetic is asserted directly by the cross-system
reconciliation check (§35), so the numbers here are not decoration.

## A note on `agg_zone_hourly`

Every silver row in this fixture falls in a distinct `(date, hour, pickup_location_id)` group, so
`agg_zone_hourly` is 1:1 with silver and its 14 rows all have `trip_count = 1`. That aggregate is
therefore a pass-through here and does **not** exercise grouping.

Multi-row grouping is covered by the other three: `agg_borough_od` has two Manhattan→Manhattan
trips on 2024-01-15 and two on 2025-01-05; `agg_daily_kpi` has two rows for yellow vendor 2 on
2024-01-15 and two for vendor 1 on 2024-01-17; `agg_payment_daily` has a three-trip credit-card
group on 2024-01-17. Stated explicitly so nobody mistakes the pass-through for evidence that
grouping works.

That 2024-01-17 credit-card group is also where the null policy surfaces in gold: it contains the
zero-fare trip whose `tip_pct` is null, and `avg_tip_pct` is 0.2 — the average of the two non-null
values, not 0.133. In `agg_daily_kpi` the same row sits alone in `(2024-01-17, yellow, 2)`, whose
`avg_tip_pct` is therefore null rather than zero.
