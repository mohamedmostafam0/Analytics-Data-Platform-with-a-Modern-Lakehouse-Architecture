-- Bronze Layer: Raw ingested data
CREATE DATABASE IF NOT EXISTS bronze;

CREATE TABLE IF NOT EXISTS bronze.users (
    id BIGINT,
    first_name STRING,
    last_name STRING,
    email STRING,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    ingested_at TIMESTAMP
)
USING iceberg
PARTITIONED BY (days(created_at))
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'snappy',
    'comment' = 'Dimension table for user information'
);

CREATE TABLE IF NOT EXISTS bronze.items (
    id BIGINT,
    name STRING,
    category STRING,
    price DECIMAL(7,2),
    inventory INT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    ingested_at TIMESTAMP
)
USING iceberg
PARTITIONED BY (category)
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'snappy',
    'comment' = 'Dimension table for product items'
);

CREATE TABLE IF NOT EXISTS bronze.purchases (
    id BIGINT,
    user_id BIGINT,
    item_id BIGINT,
    quantity INT,
    purchase_price DECIMAL(12,2),
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    ingested_at TIMESTAMP
)
USING iceberg
PARTITIONED BY (days(created_at))
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'snappy',
    'comment' = 'Fact table for purchase transactions'
);

CREATE TABLE IF NOT EXISTS bronze.pageviews (
    user_id BIGINT,
    url STRING,
    channel STRING,
    received_at TIMESTAMP,
    ingested_at TIMESTAMP,
    source_file STRING
)
USING iceberg
PARTITIONED BY (days(received_at))
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'snappy',
    'comment' = 'Fact table for purchase transactions',
    'write.spark.fanout.enabled' = 'true',
    'write.distribution-mode' = 'none'
);

CREATE TABLE IF NOT EXISTS bronze.pageviews_dlq (
    ingestion_time timestamp,
    source_file string,
    raw_record string,
    user_id bigint,
    url string
)
USING iceberg
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'snappy',
    'comment' = 'Dead Letter Queue for malformed pageviews'
);
