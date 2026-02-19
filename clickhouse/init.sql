CREATE DATABASE IF NOT EXISTS oneshop;

USE oneshop;

-- 1. Queue Table (Connects to Kafka)
-- Matches Debezium Avro format
CREATE TABLE IF NOT EXISTS purchases_queue (
    id              Int32,
    user_id         Int64,
    item_id         Int64,
    campaign_id     String,
    quantity        Int32,
    purchase_price  Double,
    created_at      String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'dbz-purchases.public.purchases',
    kafka_group_name = 'clickhouse-purchases-consumer',
    kafka_format = 'AvroConfluent',
    format_avro_schema_registry_url = 'http://schema-registry:8081',
    kafka_handle_error_mode = 'stream';

-- 2. Destination Table (Stores Data)
CREATE TABLE IF NOT EXISTS purchases (
    id              Int32,
    user_id         Int64,
    item_id         Int64,
    campaign_id     String,
    quantity        Int32,
    purchase_price  Double,
    created_at      DateTime64(3),
    ingested_at     DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (created_at, id);

-- 3. Materialized View (Moves data from Queue to Destination)
CREATE MATERIALIZED VIEW IF NOT EXISTS purchases_mv TO purchases AS
SELECT
    id,
    user_id,
    item_id,
    campaign_id,
    quantity,
    purchase_price,
    toDateTime64(created_at, 3) AS created_at
FROM purchases_queue;
