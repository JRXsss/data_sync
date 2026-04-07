-- Table: behavior_data.shopline_behavior_web
-- Description: 事件级行为宽表，关联归因结果，输出首次触点和末次触点的渠道/活动分类。
--              依赖 UDF: function.get_channel, function.get_channel_type, function.get_campaign_type
-- Partition by: date_utc9
-- Cluster by: client_id
-- Source: behavior_data.shopline_behavior_web_raw_data
--         behavior_data.shopline_traffic_attribution
-- Schedule: Daily, incremental MERGE (replace last 2 days)
--
-- DDL (first time setup):
-- CREATE OR REPLACE TABLE `${GCP_PROJECT_ID}.behavior_data.shopline_behavior_web`
-- PARTITION BY date_utc9
-- CLUSTER BY client_id AS
-- <run the MERGE below>

MERGE `${GCP_PROJECT_ID}.behavior_data.shopline_behavior_web` AS T
USING (

WITH raw_data AS (
  SELECT DISTINCT
    date_utc8,
    date_utc9,
    event_time,
    event_id,
    event_name,
    client_id,
    referer,
    url,
    order_seq,
    value
  FROM `${GCP_PROJECT_ID}.behavior_data.shopline_behavior_web_raw_data`
  WHERE date_utc8 >= CURRENT_DATE('Asia/Shanghai') - 2
)

SELECT
  a.date_utc8,
  a.date_utc9,
  a.event_time,
  a.event_id,
  a.event_name,
  a.client_id,
  a.referer,
  a.order_seq,
  a.value,
  -- 首次触点归因
  `${GCP_PROJECT_ID}.function.get_channel`(
    b.first_traffic_source.source,
    b.first_traffic_source.medium,
    b.first_traffic_source.campaign,
    b.first_traffic_source.referer
  ) AS first_channel,
  `${GCP_PROJECT_ID}.function.get_channel_type`(
    `${GCP_PROJECT_ID}.function.get_channel`(
      b.first_traffic_source.source,
      b.first_traffic_source.medium,
      b.first_traffic_source.campaign,
      b.first_traffic_source.referer
    )
  ) AS first_channel_type,
  `${GCP_PROJECT_ID}.function.get_campaign_type`(
    `${GCP_PROJECT_ID}.function.get_channel`(
      b.first_traffic_source.source,
      b.first_traffic_source.medium,
      b.first_traffic_source.campaign,
      b.first_traffic_source.referer
    ),
    b.first_traffic_source.campaign
  ) AS first_campaign_type,
  b.first_traffic_source.source   AS first_utm_source,
  b.first_traffic_source.medium   AS first_utm_medium,
  b.first_traffic_source.campaign AS first_utm_campaign,
  b.first_traffic_source.gclid    AS first_gclid,
  b.first_traffic_source.content  AS first_utm_content,
  b.first_traffic_source.term     AS first_utm_term,
  b.first_traffic_source.referer  AS first_referer,
  -- 末次非直接触点归因
  `${GCP_PROJECT_ID}.function.get_channel`(
    b.last_non_direct_traffic_source.source,
    b.last_non_direct_traffic_source.medium,
    b.last_non_direct_traffic_source.campaign,
    b.last_non_direct_traffic_source.referer
  ) AS last_channel,
  `${GCP_PROJECT_ID}.function.get_channel_type`(
    `${GCP_PROJECT_ID}.function.get_channel`(
      b.last_non_direct_traffic_source.source,
      b.last_non_direct_traffic_source.medium,
      b.last_non_direct_traffic_source.campaign,
      b.last_non_direct_traffic_source.referer
    )
  ) AS last_channel_type,
  `${GCP_PROJECT_ID}.function.get_campaign_type`(
    `${GCP_PROJECT_ID}.function.get_channel`(
      b.last_non_direct_traffic_source.source,
      b.last_non_direct_traffic_source.medium,
      b.last_non_direct_traffic_source.campaign,
      b.last_non_direct_traffic_source.referer
    ),
    b.last_traffic_source.campaign
  ) AS last_campaign_type,
  b.last_non_direct_traffic_source.source   AS last_utm_source,
  b.last_non_direct_traffic_source.medium   AS last_utm_medium,
  b.last_non_direct_traffic_source.campaign AS last_utm_campaign,
  b.last_non_direct_traffic_source.gclid    AS last_gclid,
  b.last_non_direct_traffic_source.content  AS last_utm_content,
  b.last_non_direct_traffic_source.term     AS last_utm_term,
  b.last_non_direct_traffic_source.referer  AS last_referer
FROM raw_data a
LEFT JOIN (
  SELECT *
  FROM `${GCP_PROJECT_ID}.behavior_data.shopline_traffic_attribution`
  WHERE date_utc8 >= CURRENT_DATE('Asia/Shanghai') - 2
) b ON a.event_id = b.event_id

) AS S
ON T.event_id = S.event_id

WHEN MATCHED THEN
  UPDATE SET
    T.date_utc8          = S.date_utc8,
    T.date_utc9          = S.date_utc9,
    T.event_time         = S.event_time,
    T.event_name         = S.event_name,
    T.client_id          = S.client_id,
    T.referer            = S.referer,
    T.order_seq          = S.order_seq,
    T.value              = S.value,
    T.first_channel      = S.first_channel,
    T.first_channel_type = S.first_channel_type,
    T.first_campaign_type = S.first_campaign_type,
    T.first_utm_source   = S.first_utm_source,
    T.first_utm_medium   = S.first_utm_medium,
    T.first_utm_campaign = S.first_utm_campaign,
    T.first_gclid        = S.first_gclid,
    T.first_utm_content  = S.first_utm_content,
    T.first_utm_term     = S.first_utm_term,
    T.first_referer      = S.first_referer,
    T.last_channel       = S.last_channel,
    T.last_channel_type  = S.last_channel_type,
    T.last_campaign_type = S.last_campaign_type,
    T.last_utm_source    = S.last_utm_source,
    T.last_utm_medium    = S.last_utm_medium,
    T.last_utm_campaign  = S.last_utm_campaign,
    T.last_gclid         = S.last_gclid,
    T.last_utm_content   = S.last_utm_content,
    T.last_utm_term      = S.last_utm_term,
    T.last_referer       = S.last_referer

WHEN NOT MATCHED THEN
  INSERT (
    date_utc8, date_utc9, event_time, event_id, event_name, client_id,
    referer, order_seq, value,
    first_channel, first_channel_type, first_campaign_type,
    first_utm_source, first_utm_medium, first_utm_campaign,
    first_gclid, first_utm_content, first_utm_term, first_referer,
    last_channel, last_channel_type, last_campaign_type,
    last_utm_source, last_utm_medium, last_utm_campaign,
    last_gclid, last_utm_content, last_utm_term, last_referer
  )
  VALUES (
    S.date_utc8, S.date_utc9, S.event_time, S.event_id, S.event_name, S.client_id,
    S.referer, S.order_seq, S.value,
    S.first_channel, S.first_channel_type, S.first_campaign_type,
    S.first_utm_source, S.first_utm_medium, S.first_utm_campaign,
    S.first_gclid, S.first_utm_content, S.first_utm_term, S.first_referer,
    S.last_channel, S.last_channel_type, S.last_campaign_type,
    S.last_utm_source, S.last_utm_medium, S.last_utm_campaign,
    S.last_gclid, S.last_utm_content, S.last_utm_term, S.last_referer
  );
