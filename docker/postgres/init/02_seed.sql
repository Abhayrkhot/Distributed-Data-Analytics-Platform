-- ============================================================================
--  Seed data: the governance catalog and the data-quality rule set.
--
--  Rules are declarative on purpose. The DQ engine reads this table and
--  evaluates whatever is enabled, so tightening a threshold or adding a check
--  is a SQL change, not a code change and a rebuild.
--
--  Every rule states its threshold TYPE and its NULL policy explicitly. A bare
--  `0.05` would be ambiguous (5% failures tolerated, or 95% success required?),
--  and Spark's three-valued logic makes `NULL > 0` neither true nor false --
--  so both are recorded rather than left to the reader or to the engine's
--  default behaviour. Default posture is fail-closed: NULL counts as a
--  violation unless there is a documented reason otherwise.
-- ============================================================================

-- ---------------------------------------------------------------------------
-- Governance catalog
-- ---------------------------------------------------------------------------
INSERT INTO control.dataset_catalog
    (dataset_name, layer, storage_system, storage_path, data_format,
     partition_keys, owner, pii_class, retention_days, description)
VALUES
    ('raw.yellow_tripdata', 'bronze', 'filesystem', '/data/raw', 'parquet',
     '{}', 'data-platform', 'low', 90,
     'NYC TLC yellow taxi trip records as downloaded, untouched.'),

    ('raw.green_tripdata', 'bronze', 'filesystem', '/data/raw', 'parquet',
     '{}', 'data-platform', 'low', 90,
     'NYC TLC green taxi trip records. Schema differs from yellow.'),

    ('raw.taxi_zone_lookup', 'reference', 'filesystem', '/data/raw', 'csv',
     '{}', 'data-platform', 'none', NULL,
     'Zone/borough reference dimension, 265 rows.'),

    ('bronze.trip_raw', 'bronze', 'filesystem', '/data/bronze/trip_raw', 'parquet',
     '{source,ingest_date}', 'data-platform', 'low', 90,
     'Normalized union of yellow and green with provenance columns added.'),

    ('silver.trip_clean', 'silver', 'filesystem', '/data/silver/trip_clean', 'parquet',
     '{pickup_year,pickup_month}', 'data-platform', 'low', 365,
     'Validated, deduplicated, zone-enriched trips on a conformed schema.'),

    ('gold.agg_zone_hourly', 'gold', 'clickhouse', 'analytics.agg_zone_hourly', 'mergetree',
     '{toYYYYMM(pickup_date)}', 'analytics-eng', 'none', 1825,
     'Revenue and volume per pickup zone per hour.'),

    ('gold.agg_borough_od', 'gold', 'clickhouse', 'analytics.agg_borough_od', 'mergetree',
     '{toYYYYMM(pickup_date)}', 'analytics-eng', 'none', 1825,
     'Borough-to-borough origin/destination matrix.'),

    ('gold.agg_payment_daily', 'gold', 'clickhouse', 'analytics.agg_payment_daily', 'mergetree',
     '{toYYYYMM(pickup_date)}', 'analytics-eng', 'none', 1825,
     'Daily payment-type mix and tip behaviour.'),

    ('gold.agg_daily_kpi', 'gold', 'clickhouse', 'analytics.agg_daily_kpi', 'mergetree',
     '{toYYYYMM(pickup_date)}', 'analytics-eng', 'none', 1825,
     'Headline daily KPIs by source and vendor.'),

    ('gold.fact_trip', 'gold', 'clickhouse', 'analytics.fact_trip', 'mergetree',
     '{toYYYYMM(pickup_ts)}', 'analytics-eng', 'low', 1825,
     'Trip-grain fact table serving ad-hoc analytics and the benchmark queries.'),

    ('stream.trip_events', 'stream', 'kafka', 'taxi.trips.raw', 'json',
     '{}', 'data-platform', 'low', 7,
     'Trip events replayed onto Kafka for the streaming pipeline.'),

    ('stream.trip_window', 'stream', 'clickhouse', 'analytics.stream_trip_window', 'mergetree',
     '{toYYYYMMDD(window_start)}', 'data-platform', 'none', 30,
     'Five-minute tumbling window aggregates from the streaming consumer.');

-- ---------------------------------------------------------------------------
-- Data quality rules (19)
-- ---------------------------------------------------------------------------
INSERT INTO control.dq_rule
    (rule_name, dataset_name, rule_type, target_column, rule_params,
     severity, threshold_type, threshold_value, null_policy)
VALUES
    -- Structural integrity: must hold or downstream joins are meaningless.
    ('bronze_pickup_ts_not_null', 'bronze.trip_raw', 'not_null', 'pickup_ts',
     '{}'::jsonb, 'FAIL', 'max_violation_fraction', 0, 'violation'),
    ('bronze_dropoff_ts_not_null', 'bronze.trip_raw', 'not_null', 'dropoff_ts',
     '{}'::jsonb, 'FAIL', 'max_violation_fraction', 0, 'violation'),
    ('bronze_source_accepted', 'bronze.trip_raw', 'accepted_values', 'source',
     '{"values": ["yellow", "green"]}'::jsonb,
     'FAIL', 'max_violation_fraction', 0, 'violation'),

    -- Silver: business plausibility. Amounts are required, so NULL is a violation.
    ('silver_trip_key_unique', 'silver.trip_clean', 'unique', 'trip_key',
     '{}'::jsonb, 'FAIL', 'max_violation_fraction', 0, 'violation'),
    ('silver_fare_non_negative', 'silver.trip_clean', 'range', 'fare_amount',
     '{"min": 0, "max": 10000}'::jsonb,
     'FAIL', 'max_violation_fraction', 0, 'violation'),
    ('silver_total_non_negative', 'silver.trip_clean', 'range', 'total_amount',
     '{"min": 0, "max": 10000}'::jsonb,
     'FAIL', 'max_violation_fraction', 0, 'violation'),
    ('silver_distance_plausible', 'silver.trip_clean', 'range', 'trip_distance_mi',
     '{"min": 0, "max": 300}'::jsonb,
     'FAIL', 'max_violation_fraction', 0, 'violation'),
    ('silver_duration_plausible', 'silver.trip_clean', 'range', 'trip_duration_min',
     '{"min": 0.5, "max": 1440}'::jsonb,
     'FAIL', 'max_violation_fraction', 0, 'violation'),

    -- A few GPS-glitch trips report implausible speeds; warn rather than fail.
    ('silver_speed_plausible', 'silver.trip_clean', 'range', 'avg_speed_mph',
     '{"min": 0, "max": 100}'::jsonb,
     'WARN', 'max_violation_fraction', 0.005, 'violation'),

    -- passenger_count is genuinely nullable in TLC data, so NULL rows are
    -- excluded from the denominator instead of counted as failures. Capped by
    -- an absolute count: the concern is a batch of bad rows, not a ratio.
    ('silver_passenger_count_sane', 'silver.trip_clean', 'range', 'passenger_count',
     '{"min": 0, "max": 9}'::jsonb,
     'WARN', 'max_violation_count', 1000, 'ignore'),

    -- Referential integrity against the zone dimension. TLC uses sentinel
    -- location ids (264/265 = "Unknown"), so a tolerance is expected.
    ('silver_pickup_zone_known', 'silver.trip_clean', 'referential', 'pickup_location_id',
     '{"ref_dataset": "raw.taxi_zone_lookup", "ref_column": "LocationID"}'::jsonb,
     'WARN', 'max_violation_fraction', 0.02, 'violation'),
    ('silver_dropoff_zone_known', 'silver.trip_clean', 'referential', 'dropoff_location_id',
     '{"ref_dataset": "raw.taxi_zone_lookup", "ref_column": "LocationID"}'::jsonb,
     'WARN', 'max_violation_fraction', 0.02, 'violation'),

    -- Cross-field logic a column-wise check cannot express.
    ('silver_dropoff_after_pickup', 'silver.trip_clean', 'expression', NULL,
     '{"expression": "dropoff_ts > pickup_ts"}'::jsonb,
     'FAIL', 'max_violation_fraction', 0, 'violation'),
    ('silver_total_covers_fare', 'silver.trip_clean', 'expression', NULL,
     '{"expression": "total_amount >= fare_amount - 0.01"}'::jsonb,
     'WARN', 'max_violation_fraction', 0.01, 'violation'),
    -- tip_pct is undefined for zero-fare trips; those rows are excluded rather
    -- than counted against the rule.
    ('silver_tip_pct_bounded', 'silver.trip_clean', 'expression', NULL,
     '{"expression": "tip_pct >= 0 AND tip_pct <= 2.0"}'::jsonb,
     'WARN', 'max_violation_fraction', 0.005, 'ignore'),

    -- Volume regression: catches a silently truncated upstream file.
    ('silver_row_count_stable', 'silver.trip_clean', 'row_count_delta', NULL,
     '{"max_drop_pct": 0.25}'::jsonb,
     'WARN', 'max_violation_fraction', 0, 'violation'),

    -- Freshness relative to the period the file claims to cover.
    ('silver_freshness', 'silver.trip_clean', 'freshness', 'pickup_ts',
     '{"max_age_days": 3650}'::jsonb,
     'WARN', 'max_violation_fraction', 0, 'violation'),

    -- Gold aggregates must not lose money relative to silver.
    ('gold_revenue_non_negative', 'gold.agg_zone_hourly', 'range', 'total_revenue',
     '{"min": 0, "max": 100000000}'::jsonb,
     'FAIL', 'max_violation_fraction', 0, 'violation'),
    ('gold_trip_count_positive', 'gold.agg_zone_hourly', 'range', 'trip_count',
     '{"min": 1, "max": 10000000}'::jsonb,
     'FAIL', 'max_violation_fraction', 0, 'violation');

-- ---------------------------------------------------------------------------
-- Lineage nodes for known datasets and jobs. Edges are written at runtime.
-- ---------------------------------------------------------------------------
INSERT INTO control.lineage_node (node_name, node_type, node_layer)
SELECT dataset_name, 'dataset', layer FROM control.dataset_catalog;

INSERT INTO control.lineage_node (node_name, node_type, node_layer) VALUES
    ('BatchIngestJob',     'job', 'bronze'),
    ('SilverTransformJob', 'job', 'silver'),
    ('GoldAggregateJob',   'job', 'gold'),
    ('StreamIngestJob',    'job', 'stream'),
    ('BenchmarkJob',       'job', 'gold');
