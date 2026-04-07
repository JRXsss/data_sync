-- Table: behavior_data.shopline_traffic_attribution
-- Description: 事件级流量归因表。
-- Partition by: date_utc9
-- Cluster by: client_id, event_id
-- Source: behavior_data.shopline_behavior_web_raw_data
-- Schedule: Daily, incremental MERGE (replace last 2 days)
--
-- DDL (first time setup):
-- CREATE OR REPLACE TABLE `${GCP_PROJECT_ID}.behavior_data.shopline_traffic_attribution`
-- PARTITION BY date_utc9
-- CLUSTER BY client_id, event_id AS
-- <run the MERGE below>

MERGE `${GCP_PROJECT_ID}.behavior_data.shopline_traffic_attribution` AS a
USING (

WITH event_data AS (
  SELECT
    *,
    IF(
      COALESCE(utm_source, utm_medium, utm_campaign, utm_term, utm_content, gclid) IS NOT NULL,
      STRUCT(
        CASE WHEN gclid IS NOT NULL AND utm_source IS NULL THEN 'google' ELSE utm_source END AS source,
        CASE WHEN gclid IS NOT NULL AND utm_medium IS NULL THEN 'cpc'    ELSE utm_medium END AS medium,
        utm_campaign AS campaign,
        gclid        AS gclid,
        utm_content  AS content,
        utm_term     AS term,
        CASE WHEN is_self_referrer IS FALSE AND is_payment_referrer IS FALSE THEN referer ELSE NULL END AS referer
      ),
      NULL
    ) AS hit_traffic_source
  FROM (
    SELECT
      DATE(event_time, 'Asia/Shanghai') AS date_utc8,
      DATE(event_time, 'Asia/Tokyo')    AS date_utc9,
      event_time,
      client_id,
      event_id,
      event_name,
      referer,
      utm_source,
      utm_medium,
      utm_campaign,
      utm_term,
      utm_content,
      gclid,
      REGEXP_CONTAINS(LOWER(COALESCE(referer, '')), r'https?://([^/]+\.)?${BRAND_DOMAIN}')
        AS is_self_referrer,
      REGEXP_CONTAINS(LOWER(COALESCE(referer, '')), r'komoju\.com|paypal\.com|pay\.shopline\.com|stripe\.com')
        AS is_payment_referrer,
      REGEXP_CONTAINS(
        LOWER(COALESCE(referer, '')),
        r'google\.|bing\.|yahoo\.|facebook\.|instagram\.|youtube\.|tiktok\.|twitter\.|linkedin\.|weibo\.|android-app://'
      ) AS is_external_referrer
    FROM `${GCP_PROJECT_ID}.behavior_data.shopline_behavior_web_raw_data`
    WHERE date_utc8 >= CURRENT_DATE('Asia/Shanghai') - 8
  )
),

tagged AS (
  SELECT * FROM `${GCP_PROJECT_ID}.function.tag_client_source`((SELECT * FROM event_data))
)

SELECT DISTINCT *
FROM tagged
WHERE date_utc8 >= CURRENT_DATE('Asia/Shanghai') - 1

) AS b
ON a.event_id = b.event_id

WHEN MATCHED THEN
  UPDATE SET
    date_utc8                      = b.date_utc8,
    date_utc9                      = b.date_utc9,
    event_time                     = b.event_time,
    client_id                      = b.client_id,
    hit_traffic_source             = b.hit_traffic_source,
    event_name                     = b.event_name,
    first_traffic_source           = b.first_traffic_source,
    last_traffic_source            = b.last_traffic_source,
    last_non_direct_traffic_source = b.last_non_direct_traffic_source

WHEN NOT MATCHED THEN
  INSERT (
    date_utc8,
    date_utc9,
    event_time,
    client_id,
    event_id,
    hit_traffic_source,
    event_name,
    first_traffic_source,
    last_traffic_source,
    last_non_direct_traffic_source
  )
  VALUES (
    b.date_utc8,
    b.date_utc9,
    b.event_time,
    b.client_id,
    b.event_id,
    b.hit_traffic_source,
    b.event_name,
    b.first_traffic_source,
    b.last_traffic_source,
    b.last_non_direct_traffic_source
  );
