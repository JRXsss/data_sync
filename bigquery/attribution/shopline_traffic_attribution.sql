-- Table: behavior_data.shopline_traffic_attribution
-- Description: 事件级流量归因表。归因逻辑由 TVF function.get_7d_windowed_attribution 封装。
-- Partition by: date_utc9
-- Cluster by: client_id, event_id
-- Source: behavior_data.shopline_behavior_web_raw_data (via TVF)
-- Schedule: Daily, incremental MERGE (replace last 2 days)
--
-- DDL (first time setup):
-- CREATE OR REPLACE TABLE `${GCP_PROJECT_ID}.behavior_data.shopline_traffic_attribution`
-- PARTITION BY date_utc9
-- CLUSTER BY client_id, event_id AS
-- <run the MERGE below>

MERGE `${GCP_PROJECT_ID}.behavior_data.shopline_traffic_attribution` AS a
USING (
  SELECT DISTINCT *
  FROM `${GCP_PROJECT_ID}.function.get_7d_windowed_attribution`(8, 1)
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
