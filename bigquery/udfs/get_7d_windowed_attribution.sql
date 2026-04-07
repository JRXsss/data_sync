-- TVF: function.get_7d_windowed_attribution
-- Description: 7 天滑动窗口归因核心逻辑。
--              对每个事件计算窗口内的首次触点、末次触点、末次非直接触点。
-- Returns: TABLE
-- Parameters:
--   lookback_days INT64  -- 向前取数天数，用于保证窗口完整性（建议 8）
--   target_days   INT64  -- 实际输出的目标天数（建议 1～2）

CREATE OR REPLACE TABLE FUNCTION `${GCP_PROJECT_ID}.function.get_7d_windowed_attribution`(
  lookback_days INT64,
  target_days   INT64
)
AS (

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
        CASE
          WHEN is_self_referrer IS FALSE AND is_payment_referrer IS FALSE THEN referer
          ELSE NULL
        END AS referer
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
    WHERE date_utc8 >= CURRENT_DATE('Asia/Shanghai') - lookback_days
  )
),

windowed AS (
  SELECT
    date_utc8,
    date_utc9,
    event_time,
    client_id,
    event_id,
    hit_traffic_source,
    event_name,
    FIRST_VALUE(hit_traffic_source IGNORE NULLS) OVER (
      PARTITION BY client_id
      ORDER BY UNIX_SECONDS(event_time)
      RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW
    ) AS first_traffic_source,
    LAST_VALUE(hit_traffic_source IGNORE NULLS) OVER (
      PARTITION BY client_id
      ORDER BY UNIX_SECONDS(event_time)
      RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW
    ) AS last_traffic_source,
    LAST_VALUE(hit_traffic_source IGNORE NULLS) OVER (
      PARTITION BY client_id
      ORDER BY UNIX_SECONDS(event_time)
      RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW
    ) AS last_non_direct_traffic_source
  FROM event_data
)

SELECT DISTINCT *
FROM windowed
WHERE date_utc8 >= CURRENT_DATE('Asia/Shanghai') - target_days

);
