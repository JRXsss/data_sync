-- Table: behavior_data.shopline_traffic_attribution
-- Description: 事件级流量归因表。对每个事件，用 7 天滑动窗口计算首次触点、末次触点、末次非直接触点。
--              hit_traffic_source 优先级：UTM 参数 > gclid > 可用 referer。
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
    -- 每行原始流量来源：有 UTM/gclid 则填充，否则为 NULL（= direct）
    IF(
      COALESCE(utm_source, utm_medium, utm_campaign, utm_term, utm_content, gclid) IS NOT NULL,
      STRUCT(
        CASE WHEN gclid IS NOT NULL AND utm_source IS NULL THEN 'google' ELSE utm_source END AS source,
        CASE WHEN gclid IS NOT NULL AND utm_medium IS NULL THEN 'cpc'    ELSE utm_medium END AS medium,
        utm_campaign AS campaign,
        gclid        AS gclid,
        utm_content  AS content,
        utm_term     AS term,
        -- 排除自己域名和外部支付来源
        CASE
          WHEN is_self_referrer IS FALSE AND is_payment_referrer IS FALSE
          THEN referer
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
      -- referer 分类标记
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

windowed AS (
  SELECT
    date_utc8,
    date_utc9,
    event_time,
    client_id,
    event_id,
    hit_traffic_source,
    event_name,
    -- ① first_touch：7 天窗口内（含当前事件）最早的非 NULL 来源
    FIRST_VALUE(hit_traffic_source IGNORE NULLS) OVER (
      PARTITION BY client_id
      ORDER BY UNIX_SECONDS(event_time)
      RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW
    ) AS first_traffic_source,
    -- ② last_touch：7 天窗口内（含当前事件）最新的非 NULL 来源
    LAST_VALUE(hit_traffic_source IGNORE NULLS) OVER (
      PARTITION BY client_id
      ORDER BY UNIX_SECONDS(event_time)
      RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW
    ) AS last_traffic_source,
    -- ③ last non-direct：7 天窗口内，当前事件本身也参与（与 last_touch 同逻辑，语义上为末次非 NULL）
    LAST_VALUE(hit_traffic_source IGNORE NULLS) OVER (
      PARTITION BY client_id
      ORDER BY UNIX_SECONDS(event_time)
      RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW
    ) AS last_non_direct_traffic_source
  FROM event_data
)

SELECT DISTINCT *
FROM windowed
WHERE date_utc8 >= CURRENT_DATE('Asia/Shanghai') - 1

) AS b

ON a.event_id = b.event_id

WHEN MATCHED THEN
  UPDATE SET
    date_utc8                    = b.date_utc8,
    date_utc9                    = b.date_utc9,
    event_time                   = b.event_time,
    client_id                    = b.client_id,
    hit_traffic_source           = b.hit_traffic_source,
    event_name                   = b.event_name,
    first_traffic_source         = b.first_traffic_source,
    last_traffic_source          = b.last_traffic_source,
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
