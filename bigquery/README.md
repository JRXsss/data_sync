# BigQuery Layer

本目录存放 BigQuery 侧的所有 SQL 定义，包括数据处理表和 UDF。

## 整体架构

```
MySQL (Shopline)
    │
    │  [Python sync scripts]
    ▼
BigQuery: From_mysql.shopline_event_record_origin
    │
    │  [1] raw_data/shopline_behavior_web_raw_data.sql
    ▼
behavior_data.shopline_behavior_web_raw_data   (分区: date_utc8)
    │
    │  [2] attribution/shopline_traffic_attribution.sql
    ▼
behavior_data.shopline_traffic_attribution     (分区: date_utc9, 聚簇: client_id, event_id)
    │
    │  [3] aggregated/shopline_behavior_web.sql
    ▼
behavior_data.shopline_behavior_web            (分区: date_utc9, 聚簇: client_id)
    │
    ▼
Looker Studio
```

## 目录结构

```
bigquery/
├── raw_data/
│   └── shopline_behavior_web_raw_data.sql   # Step 1: 解析 UTM / click ID
├── attribution/
│   └── shopline_traffic_attribution.sql     # Step 2: 7 天滑动窗口归因
├── aggregated/
│   └── shopline_behavior_web.sql            # Step 3: 关联归因，输出渠道分类
└── udfs/
    ├── get_channel.sql                      # 渠道名判断
    ├── get_channel_type.sql                 # 渠道类型（Paid/Organic/Direct/Social）
    └── get_campaign_type.sql                # 活动类型
```

## 占位符说明

所有 SQL 文件中使用以下占位符，部署前需替换：

| 占位符 | 说明 | 示例 |
|---|---|---|
| `${GCP_PROJECT_ID}` | GCP 项目 ID | `my-project-123` |
| `${BRAND_DOMAIN}` | 品牌自有域名（用于过滤 self-referrer） | `example\.com` |

## 归因模型说明

### 流量来源优先级

1. UTM 参数（utm_source / utm_medium / utm_campaign）
2. gclid（自动补充 source=google, medium=cpc）
3. 可用 referer（排除自有域名和支付中间页）
4. 以上均无 → Direct（NULL）

### 归因窗口

- 窗口长度：**7 天**滑动窗口（`RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW`）
- 粒度：事件级（每个 event_id 一行）

### 三种归因字段

| 字段 | 含义 |
|---|---|
| `first_traffic_source` | 窗口内最早的非 NULL 来源（首次触点） |
| `last_traffic_source` | 窗口内最新的非 NULL 来源 |
| `last_non_direct_traffic_source` | 同上（末次非直接触点） |

## 定时任务执行顺序

每日按以下顺序执行（BigQuery Scheduled Query）：

1. `shopline_behavior_web_raw_data`：DELETE + INSERT，增量刷新最近 2 天
2. `shopline_traffic_attribution`：MERGE，增量刷新最近 2 天（查询最近 8 天用于计算窗口）
3. `shopline_behavior_web`：MERGE，增量刷新最近 2 天

## 部署步骤（新环境）

1. 替换所有 SQL 文件中的占位符
2. 在 BigQuery 控制台依次执行 `udfs/` 下的三个 UDF DDL
3. 手动执行三张表的 DDL（注释中的 `CREATE OR REPLACE TABLE`）做全量初始化
4. 在 BigQuery Scheduled Queries 中配置三个定时任务，执行顺序同上
