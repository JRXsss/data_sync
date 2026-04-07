-- TVF: function.tag_client_source
-- Description: 对事件流中的每一行，基于历史行为上下文补全流量来源标签。
-- Input:  ANY TABLE — 需包含字段: date_utc8, date_utc9, event_time, client_id,
--                                  event_id, event_name, hit_traffic_source
-- Output: TABLE (原字段 + first_traffic_source, last_traffic_source, last_non_direct_traffic_source)

CREATE OR REPLACE TABLE FUNCTION `${GCP_PROJECT_ID}.function.tag_client_source`(
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
