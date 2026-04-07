-- Table: behavior_data.shopline_behavior_web_raw_data
-- Description: Shopline 网站行为事件原始宽表，每行一个事件，解析 UTM 参数和 click ID。
-- Partition by: date_utc8
-- Source: From_mysql.shopline_event_record_origin
-- Schedule: Daily, incremental (replace last 2 days)
--
-- DDL (first time setup):
-- CREATE OR REPLACE TABLE `${GCP_PROJECT_ID}.behavior_data.shopline_behavior_web_raw_data`
-- PARTITION BY date_utc8 AS
-- <run the SELECT below>

DELETE `${GCP_PROJECT_ID}.behavior_data.shopline_behavior_web_raw_data`
WHERE date_utc8 >= CURRENT_DATE('Asia/Shanghai') - 1;

INSERT `${GCP_PROJECT_ID}.behavior_data.shopline_behavior_web_raw_data`
SELECT
  DISTINCT
  event_id,
  client_id,
  event_name,
  event_time,
  referer,
  DATE(TIMESTAMP_ADD(CAST(event_time AS TIMESTAMP), INTERVAL 1 HOUR)) AS date_utc9,
  DATE(event_time) AS date_utc8,
  -- device info
  JSON_VALUE(event_content, '$.navigator.userAgent') AS user_agent,
  COALESCE(
    JSON_VALUE(event_data, '$.url'),
    JSON_VALUE(event_content, '$.window.location.href')
  ) AS url,
  -- UTM parameters
  REGEXP_EXTRACT(
    COALESCE(JSON_VALUE(event_data, '$.url'), JSON_VALUE(event_content, '$.window.location.href')),
    r'(?i)[?&]utm_source=([^&#]*)'
  ) AS utm_source,
  REGEXP_EXTRACT(
    COALESCE(JSON_VALUE(event_data, '$.url'), JSON_VALUE(event_content, '$.window.location.href')),
    r'(?i)[?&]utm_medium=([^&#]*)'
  ) AS utm_medium,
  REGEXP_EXTRACT(
    COALESCE(JSON_VALUE(event_data, '$.url'), JSON_VALUE(event_content, '$.window.location.href')),
    r'(?i)[?&]utm_campaign=([^&#]*)'
  ) AS utm_campaign,
  REGEXP_EXTRACT(
    COALESCE(JSON_VALUE(event_data, '$.url'), JSON_VALUE(event_content, '$.window.location.href')),
    r'(?i)[?&]utm_term=([^&#]*)'
  ) AS utm_term,
  REGEXP_EXTRACT(
    COALESCE(JSON_VALUE(event_data, '$.url'), JSON_VALUE(event_content, '$.window.location.href')),
    r'(?i)[?&]utm_content=([^&#]*)'
  ) AS utm_content,
  -- order data
  JSON_VALUE(event_data, '$.orderSeq') AS order_seq,
  SAFE_CAST(JSON_VALUE(event_data, '$.value') AS INT64) AS value,
  -- click IDs
  REGEXP_EXTRACT(
    COALESCE(JSON_VALUE(event_data, '$.url'), JSON_VALUE(event_content, '$.window.location.href')),
    r'(?i)[?&]gclid=([^&#]*)'
  ) AS gclid,
  REGEXP_EXTRACT(
    COALESCE(JSON_VALUE(event_data, '$.url'), JSON_VALUE(event_content, '$.window.location.href')),
    r'(?i)[?&]fbclid=([^&#]*)'
  ) AS fbclid,
  REGEXP_EXTRACT(
    COALESCE(JSON_VALUE(event_data, '$.url'), JSON_VALUE(event_content, '$.window.location.href')),
    r'(?i)[?&]msclkid=([^&#]*)'
  ) AS msclkid,
  REGEXP_EXTRACT(
    COALESCE(JSON_VALUE(event_data, '$.url'), JSON_VALUE(event_content, '$.window.location.href')),
    r'(?i)[?&]ttclid=([^&#]*)'
  ) AS ttclid
FROM `${GCP_PROJECT_ID}.From_mysql.shopline_event_record_origin`
WHERE
  create_time >= TIMESTAMP(DATETIME(CURRENT_DATE('Asia/Shanghai') - 2), 'Asia/Shanghai')
  AND DATE(event_time, 'Asia/Shanghai') >= CURRENT_DATE('Asia/Shanghai') - 1;
