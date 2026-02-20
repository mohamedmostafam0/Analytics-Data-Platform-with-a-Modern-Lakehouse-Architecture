-- Statement Sets combine multiple INSERT statements into a single Flink job graph,
-- which optimization phases use to share source reads and operators.
EXECUTE STATEMENT SET
BEGIN

-- 1. Enrichment + classification job
INSERT INTO login_events_enriched
SELECT
  user_id,
  `timestamp`,
  ip,
  platform,
  CASE
    WHEN LOWER(device) LIKE '%iphone%' OR LOWER(device) LIKE '%android%' THEN 'mobile'
    ELSE 'desktop'
  END AS device_type,
  g.country,
  g.city
FROM login_events
LEFT JOIN ip_geo AS g ON ip LIKE CONCAT(g.ip_prefix, '%');

-- 2. Anomaly detection job: new country in past 7 days
-- BEST PRACTICE: Flink stream-to-stream joins must be bounded to prevent state explosion (OOM).
-- Using an Event-Time Interval Left Join forces Flink to only keep 7 days of state logically.
INSERT INTO login_anomalies
SELECT
  e.user_id,
  e.`timestamp`,
  'Login from new country' AS reason,
  e.country
FROM login_events_enriched e
LEFT JOIN login_events_enriched recent
  ON e.user_id = recent.user_id
  AND e.country = recent.country
  AND recent.`timestamp` BETWEEN e.`timestamp` - INTERVAL '7' DAY AND e.`timestamp` - INTERVAL '1' SECOND
WHERE recent.user_id IS NULL;

END;