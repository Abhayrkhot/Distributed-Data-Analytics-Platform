-- ============================================================================
--  Analytical marts (ClickHouse)
--
--  Physical design choices here are the ones Phase 8 measures:
--    * PARTITION BY toYYYYMM(...) so month-scoped queries prune whole parts
--    * ORDER BY leads with the column most queries filter on, then time
--    * per-column codecs (Delta/DoubleDelta + ZSTD) on monotonic/low-entropy
--      columns, which is where most of the on-disk saving comes from
--    * LowCardinality on small string domains so they compare as integers
-- ============================================================================

CREATE DATABASE IF NOT EXISTS analytics;

-- ---------------------------------------------------------------------------
-- Zone dimension (from taxi_zone_lookup.csv). ReplacingMergeTree so a reload
-- of the same lookup file is idempotent rather than duplicating rows.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics.dim_taxi_zone
(
    location_id   UInt16,
    borough       LowCardinality(String),
    zone_name     String,
    service_zone  LowCardinality(String),
    loaded_at     DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(loaded_at)
ORDER BY location_id;

-- ---------------------------------------------------------------------------
-- Silver fact table. Grain: one row per completed trip.
-- This is the table the benchmark queries hit, so its layout matters most.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics.fact_trip
(
    trip_key             UInt64,
    source               LowCardinality(String),     -- 'yellow' | 'green'
    vendor_id            LowCardinality(String),
    pickup_ts            DateTime      CODEC(DoubleDelta, ZSTD(1)),
    dropoff_ts           DateTime      CODEC(DoubleDelta, ZSTD(1)),
    pickup_date          Date          CODEC(Delta, ZSTD(1)),
    passenger_count      UInt8,
    trip_distance_mi     Float32       CODEC(Gorilla, ZSTD(1)),
    trip_duration_min    Float32       CODEC(Gorilla, ZSTD(1)),
    avg_speed_mph        Float32       CODEC(Gorilla, ZSTD(1)),
    pickup_location_id   UInt16,
    dropoff_location_id  UInt16,
    pickup_borough       LowCardinality(String),
    pickup_zone          LowCardinality(String),
    dropoff_borough      LowCardinality(String),
    dropoff_zone         LowCardinality(String),
    payment_type         LowCardinality(String),
    rate_code            LowCardinality(String),
    fare_amount          Decimal(10, 2),
    tip_amount           Decimal(10, 2),
    tolls_amount         Decimal(10, 2),
    surcharge_amount     Decimal(10, 2),
    congestion_surcharge Decimal(10, 2),
    -- Present only in 2025+ files; defaulted for older partitions. This column
    -- is the schema-evolution case the ingest layer has to handle for real.
    cbd_congestion_fee   Decimal(10, 2) DEFAULT 0,
    total_amount         Decimal(10, 2),
    tip_pct              Float32,
    -- Lineage: which run produced this row, joinable back to control.etl_run.
    etl_run_id           UInt64,
    ingested_at          DateTime DEFAULT now()
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(pickup_ts)
ORDER BY (pickup_location_id, pickup_ts)
TTL pickup_ts + INTERVAL 5 YEAR
SETTINGS index_granularity = 8192;

-- Skip indexes: let filtered scans drop whole granules without reading them.
ALTER TABLE analytics.fact_trip
    ADD INDEX IF NOT EXISTS idx_distance trip_distance_mi TYPE minmax GRANULARITY 4;

ALTER TABLE analytics.fact_trip
    ADD INDEX IF NOT EXISTS idx_dropoff_loc dropoff_location_id TYPE set(300) GRANULARITY 4;

-- ---------------------------------------------------------------------------
-- Gold aggregate: revenue and volume per pickup zone per hour.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics.agg_zone_hourly
(
    pickup_date        Date CODEC(Delta, ZSTD(1)),
    pickup_hour        UInt8,
    pickup_location_id UInt16,
    pickup_borough     LowCardinality(String),
    pickup_zone        LowCardinality(String),
    trip_count         UInt64,
    total_revenue      Decimal(18, 2),
    avg_fare           Decimal(12, 4),
    avg_distance_mi    Float32,
    avg_duration_min   Float32,
    avg_tip_pct        Float32,
    etl_run_id         UInt64
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(pickup_date)
ORDER BY (pickup_location_id, pickup_date, pickup_hour);

-- ---------------------------------------------------------------------------
-- Gold aggregate: borough-to-borough origin/destination matrix.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics.agg_borough_od
(
    pickup_date      Date CODEC(Delta, ZSTD(1)),
    pickup_borough   LowCardinality(String),
    dropoff_borough  LowCardinality(String),
    trip_count       UInt64,
    total_revenue    Decimal(18, 2),
    avg_distance_mi  Float32,
    avg_duration_min Float32,
    etl_run_id       UInt64
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(pickup_date)
ORDER BY (pickup_borough, dropoff_borough, pickup_date);

-- ---------------------------------------------------------------------------
-- Gold aggregate: payment mix per day.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics.agg_payment_daily
(
    pickup_date    Date CODEC(Delta, ZSTD(1)),
    source         LowCardinality(String),
    payment_type   LowCardinality(String),
    trip_count     UInt64,
    total_revenue  Decimal(18, 2),
    total_tips     Decimal(18, 2),
    avg_tip_pct    Float32,
    revenue_share  Float32,
    etl_run_id     UInt64
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(pickup_date)
ORDER BY (pickup_date, source, payment_type);

-- ---------------------------------------------------------------------------
-- Gold aggregate: headline daily KPIs (also mirrored to Postgres serving).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics.agg_daily_kpi
(
    pickup_date       Date CODEC(Delta, ZSTD(1)),
    source            LowCardinality(String),
    vendor_id         LowCardinality(String),
    trip_count        UInt64,
    total_revenue     Decimal(18, 2),
    avg_fare          Decimal(12, 4),
    avg_distance_mi   Float32,
    avg_duration_min  Float32,
    avg_speed_mph     Float32,
    avg_tip_pct       Float32,
    etl_run_id        UInt64
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(pickup_date)
ORDER BY (pickup_date, source, vendor_id);

-- ---------------------------------------------------------------------------
-- Streaming sink: 5-minute tumbling windows from the Kafka consumer.
--
-- The version column MUST be deterministic. An earlier design used
-- `updated_at DateTime DEFAULT now()`, which is a bug: replaying the same
-- microbatch would stamp a different version each time, so which physical row
-- survived the merge was arbitrary and the convergence claim was unfounded.
--
-- Instead:  version = stream_epoch * 2^32 + batch_id
--
--   * batch_id is Spark's foreachBatch id, replayed identically from a
--     checkpoint -- so a redelivered batch writes byte-identical rows with an
--     identical version and the merge is a no-op rather than a coin flip.
--   * batch_id alone is NOT globally unique: a fresh checkpoint restarts
--     numbering, and a new query writing batch_id=0 would lose the version
--     comparison against an existing batch_id=42 and silently fail to replace
--     it. stream_epoch (monotonic, allocated in Postgres) dominates the
--     ordering so a newer query always wins.
--
-- Physical duplicates may transiently exist before background merges run. That
-- is expected and not claimed away -- correctness checks use FINAL or explicit
-- version-aware aggregation. What converges deterministically is the logical
-- query result, not the physical row set at an arbitrary instant.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics.stream_trip_window
(
    window_start    DateTime CODEC(DoubleDelta, ZSTD(1)),
    window_end      DateTime CODEC(DoubleDelta, ZSTD(1)),
    pickup_borough  LowCardinality(String),
    trip_count      UInt64,
    total_revenue   Decimal(18, 2),
    avg_fare        Decimal(12, 4),
    avg_distance_mi Float32,
    -- Replay provenance: (stream_query_id, batch_id) is the microbatch identity.
    stream_query_id String,
    batch_id        UInt64,
    stream_epoch    UInt64,
    version         UInt64
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMMDD(window_start)
ORDER BY (window_start, window_end, pickup_borough)
TTL window_start + INTERVAL 30 DAY;

-- ---------------------------------------------------------------------------
-- Convenience view joining the fact to its zone dimension.
-- ---------------------------------------------------------------------------
CREATE VIEW IF NOT EXISTS analytics.v_trip_enriched AS
SELECT f.*, z.service_zone AS pickup_service_zone
FROM analytics.fact_trip AS f
LEFT JOIN analytics.dim_taxi_zone AS z
       ON f.pickup_location_id = z.location_id;
