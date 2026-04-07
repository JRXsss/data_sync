-- UDF: function.get_campaign_type
-- Description: 根据渠道名（get_channel 的输出）和 utm_campaign 返回活动类型。
--              Google 和 Meta 各有独立的分类规则，其他渠道返回 NULL。
-- Returns: STRING | NULL

CREATE OR REPLACE FUNCTION `${GCP_PROJECT_ID}.function.get_campaign_type`(
  channel STRING,
  utm_campaign STRING
) AS (
  CASE
    -- Google
    WHEN channel = 'Google' AND REGEXP_CONTAINS(LOWER(COALESCE(utm_campaign, '')), r'.*pmax.*')
      THEN 'Pmax'
    WHEN channel = 'Google'
         AND REGEXP_CONTAINS(LOWER(COALESCE(utm_campaign, '')), r'.*search.*')
         AND REGEXP_CONTAINS(LOWER(COALESCE(utm_campaign, '')), r'.*brand.*')
      THEN 'Search - Brand'
    WHEN channel = 'Google'
         AND REGEXP_CONTAINS(LOWER(COALESCE(utm_campaign, '')), r'.*search.*')
         AND NOT REGEXP_CONTAINS(LOWER(COALESCE(utm_campaign, '')), r'.*brand.*')
      THEN 'Search - non Brand'
    WHEN channel = 'Google' AND REGEXP_CONTAINS(LOWER(COALESCE(utm_campaign, '')), r'.*pla.*')
      THEN 'PLA'
    WHEN channel = 'Google' AND REGEXP_CONTAINS(LOWER(COALESCE(utm_campaign, '')), r'.*dg.*')
      THEN 'DG'
    WHEN channel = 'Google'
         AND REGEXP_CONTAINS(LOWER(COALESCE(utm_campaign, '')), r'.*aci.*|.*app.*')
         AND REGEXP_CONTAINS(LOWER(COALESCE(utm_campaign, '')), r'.*ios.*')
      THEN 'ACi-IOS'
    WHEN channel = 'Google'
         AND REGEXP_CONTAINS(LOWER(COALESCE(utm_campaign, '')), r'.*aci.*|.*app.*')
         AND REGEXP_CONTAINS(LOWER(COALESCE(utm_campaign, '')), r'.*android.*')
      THEN 'ACi-Android'
    -- Meta
    WHEN channel = 'Meta' AND REGEXP_CONTAINS(LOWER(COALESCE(utm_campaign, '')), r'.*asc.*')
      THEN 'ASC'
    WHEN channel = 'Meta' AND REGEXP_CONTAINS(LOWER(COALESCE(utm_campaign, '')), r'.*daba.*')
      THEN 'daba'
    WHEN channel = 'Meta' AND REGEXP_CONTAINS(LOWER(COALESCE(utm_campaign, '')), r'.*dpa.*')
      THEN 'dpa'
    WHEN channel = 'Meta' AND REGEXP_CONTAINS(LOWER(COALESCE(utm_campaign, '')), r'.*awareness.*')
      THEN 'Awareness'
    WHEN channel = 'Meta' AND REGEXP_CONTAINS(LOWER(COALESCE(utm_campaign, '')), r'.*ins.*')
      THEN 'ins-push'
    WHEN channel = 'Meta'
         AND REGEXP_CONTAINS(LOWER(COALESCE(utm_campaign, '')), r'.*aci.*|.*app.*')
         AND REGEXP_CONTAINS(LOWER(COALESCE(utm_campaign, '')), r'.*ios.*')
      THEN 'ACi-IOS'
    WHEN channel = 'Meta'
         AND REGEXP_CONTAINS(LOWER(COALESCE(utm_campaign, '')), r'.*aci.*|.*app.*')
         AND REGEXP_CONTAINS(LOWER(COALESCE(utm_campaign, '')), r'.*android.*')
      THEN 'ACi-Android'
    ELSE NULL
  END
);
