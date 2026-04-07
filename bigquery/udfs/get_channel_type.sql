-- UDF: function.get_channel_type
-- Description: 根据渠道名（get_channel 的输出）返回渠道类型。
-- Returns: STRING ('Paid' | 'Organic' | 'Direct' | 'Social' | 'Others')

CREATE OR REPLACE FUNCTION `${GCP_PROJECT_ID}.function.get_channel_type`(
  channel STRING
) AS (
  CASE
    WHEN channel IN ('Google', 'Meta', 'Bing', 'Criteo', 'Aff', 'CPS', 'Yahoo') THEN 'Paid'
    WHEN channel IN ('Push', 'SEO')                                               THEN 'Organic'
    WHEN channel = 'Direct'                                                       THEN 'Direct'
    WHEN channel = 'SNS'                                                          THEN 'Social'
    ELSE 'Others'
  END
);
