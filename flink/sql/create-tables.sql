-- Source table: login events
CREATE TABLE IF NOT EXISTS login_events (
  user_id STRING,
  `timestamp` TIMESTAMP(3),
  ip STRING,
  device STRING,
  platform STRING,
  user_agent STRING,
  WATERMARK FOR `timestamp` AS `timestamp` - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'login-events',
  'properties.bootstrap.servers' = 'kafka:9092',
  'format' = 'json',
  'scan.startup.mode' = 'earliest-offset',
  'json.timestamp-format.standard' = 'ISO-8601'
);

-- Static Geo enrichment table
CREATE TABLE IF NOT EXISTS ip_geo (
  ip_prefix STRING,
  country STRING,
  city STRING
) WITH (
  'connector' = 'filesystem',
  'path' = 'file:///opt/flink/ip_geo.csv',
  'format' = 'csv',
  'csv.ignore-parse-errors' = 'true',
  'csv.allow-comments' = 'true'
);

-- Enriched login events table
CREATE TABLE IF NOT EXISTS login_events_enriched (
  user_id STRING,
  `timestamp` TIMESTAMP(3),
  ip STRING,
  platform STRING,
  device_type STRING,
  country STRING,
  city STRING,
  PRIMARY KEY (user_id, `timestamp`) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'login-events-enriched',
  'properties.bootstrap.servers' = 'kafka:9092',
  'key.format' = 'json',
  'value.format' = 'json',
  'value.json.timestamp-format.standard' = 'ISO-8601'
);

-- Anomalies output table
CREATE TABLE IF NOT EXISTS login_anomalies (
  user_id STRING,
  `timestamp` TIMESTAMP(3),
  reason STRING,
  country STRING,
  PRIMARY KEY (user_id, `timestamp`) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'login-anomalies',
  'properties.bootstrap.servers' = 'kafka:9092',
  'key.format' = 'json',
  'value.format' = 'json'
);