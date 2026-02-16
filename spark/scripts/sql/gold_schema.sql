-- Gold Layer: Aggregated analytical data
CREATE DATABASE IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS gold.top_selling_items (
    item_id BIGINT,
    item_name STRING,
    item_category STRING,
    total_revenue DECIMAL(24,2)
)
USING iceberg
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'snappy',
    'comment' = 'Top selling items by total revenue'
);

CREATE TABLE IF NOT EXISTS gold.sales_performance_24h (
    purchase_hour INT,
    total_revenue DECIMAL(24,2)
)
USING iceberg
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'snappy',
    'comment' = 'Sales performance aggregated by hour (last 24h)'
);

CREATE TABLE IF NOT EXISTS gold.top_converting_items (
    item_id BIGINT,
    item_name STRING,
    item_category STRING,
    unique_pageview_users BIGINT,
    unique_purchase_users BIGINT,
    total_purchases BIGINT,
    total_pageviews BIGINT,
    conversion_rate DOUBLE
)
USING iceberg
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'snappy',
    'comment' = 'Items with highest pageview-to-purchase conversion rate'
);

CREATE TABLE IF NOT EXISTS gold.pageviews_by_channel (
    channel STRING,
    total_pageviews BIGINT
)
USING iceberg
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'snappy',
    'comment' = 'Pageview counts aggregated by traffic channel'
);

CREATE TABLE IF NOT EXISTS gold.user_engagement_segments (
    user_id BIGINT,
    email STRING,
    full_name STRING,
    total_pageviews BIGINT,
    active_days BIGINT,
    last_active_date DATE,
    days_since_last_active BIGINT,
    engagement_segment STRING,
    segment_date DATE
)
USING iceberg
PARTITIONED BY (segment_date)
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'snappy',
    'comment' = 'Daily user engagement segments partitioned by computation date'
);
