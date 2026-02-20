CREATE DATABASE IF NOT EXISTS oneshop;

USE oneshop;

-- 1. Queue Table (Connects to Kafka)
-- Matches Debezium Avro format
CREATE TABLE IF NOT EXISTS purchases_queue (
    id              Int32,
    user_id         Nullable(Int64),
    item_id         Nullable(Int64),
    campaign_id     Nullable(String),
    status          Nullable(Int32),
    quantity        Nullable(Int32),
    purchase_price  Nullable(Float64),
    deleted         Nullable(Boolean),
    created_at      Nullable(Int64),
    updated_at      Nullable(Int64)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'dbz_purchases.public.purchases',
    kafka_group_name = 'clickhouse-purchases-consumer-v9',
    kafka_format = 'AvroConfluent',
    format_avro_schema_registry_url = 'http://schema-registry:8081',
    kafka_handle_error_mode = 'stream';

-- 2. Destination Table (Stores Data)
CREATE TABLE IF NOT EXISTS purchases (
    id              Int32,
    user_id         Int64,
    item_id         Int64,
    campaign_id     String,
    status          Int16,
    quantity        Int32,
    purchase_price  Decimal(12, 2),
    deleted         Boolean DEFAULT false,
    created_at      DateTime64(3),
    updated_at      DateTime64(3),
    ingested_at     DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(created_at)
ORDER BY id;

-- 3. Dead Letter Queue Pattern for Errors
-- This catches Avro schema mismatches or parsing errors from the Kafka engine
CREATE TABLE IF NOT EXISTS purchases_errors (
    _error String,
    _raw_message String,
    ingested_at DateTime DEFAULT now()
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(ingested_at)
ORDER BY ingested_at;

CREATE MATERIALIZED VIEW IF NOT EXISTS purchases_errors_mv TO purchases_errors AS
SELECT _error, _raw_message
FROM purchases_queue
WHERE _error != '';

-- 4. Materialized View (Moves data from Queue to Destination)
CREATE MATERIALIZED VIEW IF NOT EXISTS purchases_mv TO purchases AS
SELECT
    id,
    ifNull(user_id, 0) AS user_id,
    ifNull(item_id, 0) AS item_id,
    ifNull(campaign_id, '') AS campaign_id,
    CAST(ifNull(status, 1) AS Int16) AS status,
    ifNull(quantity, 0) AS quantity,
    CAST(ifNull(purchase_price, 0.0) AS Decimal(12, 2)) AS purchase_price,
    ifNull(deleted, false) AS deleted,
    CAST(fromUnixTimestamp64Micro(ifNull(created_at, 0)) AS DateTime64(3)) AS created_at,
    CAST(fromUnixTimestamp64Micro(ifNull(updated_at, 0)) AS DateTime64(3)) AS updated_at
FROM purchases_queue
WHERE _error = '';
