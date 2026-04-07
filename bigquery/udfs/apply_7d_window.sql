-- TVF: function.apply_7d_window
-- Description: 对已构建好 hit_traffic_source 的事件流，应用 7 天滑动窗口，
--              计算每个事件的首次触点、末次触点、末次非直接触点。
-- Input:  ANY TABLE — 需包含字段: date_utc8, date_utc9, event_time, client_id,
--                                  event_id, event_name, hit_traffic_source
-- Output: TABLE (原字段 + first_traffic_source, last_traffic_source, last_non_direct_traffic_source)
-- Usage:
--   SELECT * FROM `${GCP_PROJECT_ID}.function.apply_7d_window`((SELECT * FROM event_data))

CREATE OR REPLACE TABLE FUNCTION `${GCP_PROJECT_ID}.function.apply_7d_window`(
  input_table ANY TABLE
)
AS (
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
  FROM input_table
);
