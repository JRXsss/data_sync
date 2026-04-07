-- UDF: function.get_channel
-- Description: 根据 utm_source / utm_medium / utm_campaign / referer 判断流量渠道。
--              优先级：付费广告来源 > 自然搜索/社媒 > Direct > Others
-- Returns: STRING (渠道名)

CREATE OR REPLACE FUNCTION `${GCP_PROJECT_ID}.function.get_channel`(
  utm_source STRING,
  utm_medium STRING,
  utm_campaign STRING,
  referer STRING
) AS (
  CASE
    WHEN REGEXP_CONTAINS(LOWER(COALESCE(utm_source, '')), r'.*google.*')
         AND REGEXP_CONTAINS(LOWER(COALESCE(utm_campaign, '')), r'.*pmax.*|.*search.*|.*pla.*|.*dg.*')
      THEN 'Google'
    WHEN REGEXP_CONTAINS(LOWER(COALESCE(utm_source, '')), r'.*meta.*|.*facebook.*')
         OR REGEXP_CONTAINS(LOWER(COALESCE(utm_medium, '')), r'.*meta.*|.*facebook.*')
      THEN 'Meta'
    WHEN REGEXP_CONTAINS(LOWER(COALESCE(utm_source, '')), r'.*bing.*')
      THEN 'Bing'
    WHEN REGEXP_CONTAINS(LOWER(COALESCE(utm_source, '')), r'.*criteo.*')
      THEN 'Criteo'
    WHEN REGEXP_CONTAINS(LOWER(COALESCE(utm_source, '')), r'.*aff.*')
      THEN 'Aff'
    WHEN REGEXP_CONTAINS(LOWER(COALESCE(utm_source, '')), r'.*smartpush.*')
      THEN 'Push'
    WHEN REGEXP_CONTAINS(LOWER(COALESCE(utm_source, '')), r'.*yahoo.*')
         OR LOWER(utm_source) = 'r'
         OR LOWER(utm_medium) = 'r'
      THEN 'Yahoo'
    WHEN REGEXP_CONTAINS(LOWER(COALESCE(utm_source, '')), r'.*brainx.*')
      THEN 'CPS'
    WHEN REGEXP_CONTAINS(LOWER(COALESCE(utm_source, '')), r'.*tiktok.*')
      THEN 'Tiktok'
    WHEN REGEXP_CONTAINS(LOWER(COALESCE(referer, '')), r'.*google.*|.*bing.*|.*yahoo.*')
      THEN 'SEO'
    WHEN REGEXP_CONTAINS(LOWER(COALESCE(referer, '')), r'.*facebook.*|.*instagram.*|.*youtube.*')
         OR REGEXP_CONTAINS(LOWER(COALESCE(utm_source, '')), r'igshopping')
         OR (LOWER(utm_source) = 'ig' AND LOWER(utm_medium) = 'social')
      THEN 'SNS'
    WHEN referer IS NULL
         AND COALESCE(utm_source, utm_medium, utm_campaign) IS NULL
      THEN 'Direct'
    ELSE 'Others'
  END
);
