-- ============================================================
-- ClickHouse Serving Layer Schema
-- Lambda Architecture: merges speed + batch views
-- ============================================================

-- ------------------------------------------------------------
-- 1. RAW EVENTS (speed layer — written by Kafka consumer)
--    Uses ReplacingMergeTree so duplicate event_ids are
--    deduplicated on background merges.
-- ------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS clickstream;

USE clickstream;

CREATE TABLE IF NOT EXISTS raw_events
(
    event_id   String,
    user_id    String,
    product_id String,
    event_type LowCardinality(String),
    event_time DateTime,
    ingested_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMMDD(event_time)
ORDER BY (event_time, event_id)
TTL event_time + INTERVAL 30 DAY
SETTINGS index_granularity = 8192;


-- ------------------------------------------------------------
-- 2. FLASH SALE ALERTS (speed layer — written by Kafka consumer
--    reading from the alerts-notifications topic)
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS flash_sale_alerts
(
    window_start DateTime,
    window_end   DateTime,
    product_id   String,
    view_count   UInt32,
    purchase_count UInt32,
    action       String,
    ingested_at  DateTime DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(window_start)
ORDER BY (window_start, product_id)
TTL window_start + INTERVAL 7 DAY
SETTINGS index_granularity = 8192;


-- ------------------------------------------------------------
-- 3. DAILY BATCH REPORTS — top products (batch layer)
--    Written by the Airflow DAG after Spark batch completes.
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS batch_top_products
(
    report_date  Date,
    product_id   String,
    view_count   UInt64,
    rank         UInt8,
    loaded_at    DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(loaded_at)
PARTITION BY toYYYYMM(report_date)
ORDER BY (report_date, rank)
SETTINGS index_granularity = 8192;


-- ------------------------------------------------------------
-- 4. DAILY BATCH REPORTS — user segments (batch layer)
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS batch_user_segments
(
    report_date Date,
    user_id     String,
    views       UInt64,
    purchases   UInt64,
    segment     LowCardinality(String),
    loaded_at   DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(loaded_at)
PARTITION BY toYYYYMM(report_date)
ORDER BY (report_date, user_id)
SETTINGS index_granularity = 8192;


-- ============================================================
-- MATERIALIZED VIEWS  (serving views — queried by dashboard)
-- ============================================================

-- Real-time: events per product per hour (last 24h)
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_product_events_hourly
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMMDD(hour)
ORDER BY (hour, product_id, event_type)
AS
SELECT
    toStartOfHour(event_time) AS hour,
    product_id,
    event_type,
    count()                   AS event_count
FROM raw_events
GROUP BY hour, product_id, event_type;


-- Real-time: per-user session summary (rolling 24h)
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_user_summary_realtime
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMMDD(window_day)
ORDER BY (window_day, user_id)
AS
SELECT
    toDate(event_time)                                         AS window_day,
    user_id,
    countIf(event_type = 'view')                               AS views,
    countIf(event_type = 'purchase')                           AS purchases,
    countIf(event_type = 'add_to_cart')                        AS add_to_carts
FROM raw_events
GROUP BY window_day, user_id;


-- ============================================================
-- CONVENIENCE VIEWS for the dashboard API
-- ============================================================

-- Top 10 viewed products in the last hour (speed layer)
CREATE VIEW IF NOT EXISTS v_top_products_realtime AS
SELECT
    product_id,
    sumIf(event_count, event_type = 'view')     AS view_count,
    sumIf(event_count, event_type = 'purchase') AS purchase_count
FROM mv_product_events_hourly
WHERE hour >= now() - INTERVAL 1 HOUR
GROUP BY product_id
ORDER BY view_count DESC
LIMIT 10;


-- Funnel: views → add_to_cart → purchase (last 24h)
CREATE VIEW IF NOT EXISTS v_funnel_24h AS
SELECT
    event_type,
    sum(event_count) AS total
FROM mv_product_events_hourly
WHERE hour >= now() - INTERVAL 24 HOUR
GROUP BY event_type
ORDER BY total DESC;


-- Recent flash sale alerts (last 6h)
CREATE VIEW IF NOT EXISTS v_recent_alerts AS
SELECT
    window_start,
    window_end,
    product_id,
    view_count,
    purchase_count,
    action
FROM flash_sale_alerts
WHERE window_start >= now() - INTERVAL 6 HOUR
ORDER BY window_start DESC
LIMIT 50;


-- User segment counts for today (batch layer)
CREATE VIEW IF NOT EXISTS v_segment_counts_today AS
SELECT
    segment,
    count() AS user_count
FROM batch_user_segments
WHERE report_date = today()
GROUP BY segment
ORDER BY user_count DESC;