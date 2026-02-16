INSERT INTO iceberg.gold.user_engagement_segments
SELECT
    u.id AS user_id,
    u.email,
    u.full_name,
    COUNT(p.page) AS total_pageviews,
    COUNT(DISTINCT CAST(p.received_at AS DATE)) AS active_days,
    MAX(CAST(p.received_at AS DATE)) AS last_active_date,
    date_diff('day', MAX(CAST(p.received_at AS DATE)), CURRENT_DATE) AS days_since_last_active,
    CASE
        WHEN COUNT(p.page) >= 50 AND date_diff('day', MAX(CAST(p.received_at AS DATE)), CURRENT_DATE) <= 3 THEN 'high_engagement'
        WHEN COUNT(p.page) BETWEEN 10 AND 49 AND date_diff('day', MAX(CAST(p.received_at AS DATE)), CURRENT_DATE) <= 7 THEN 'medium_engagement'
        ELSE 'low_engagement'
    END AS engagement_segment,
    CURRENT_DATE AS segment_date
FROM iceberg.silver.users u
LEFT JOIN iceberg.silver.pageviews_by_items p ON u.id = p.user_id
WHERE u.valid_email = TRUE
GROUP BY u.id, u.email, u.full_name