import os
from decimal import Decimal
import pymysql
import pandas as pd
from google.cloud import bigquery

MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_PORT = int(os.getenv("MYSQL_PORT") or "3306")
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
MYSQL_DB = os.getenv("MYSQL_DB") or "caguuu_erp"

# project-fddee9ed-d147-4ffe-b75.From_mysql.shopline_event_record_origin
# BQ_PROJECT = (os.getenv("BQ_PROJECT") or "").strip().strip("`")
# BQ_DATASET = (os.getenv("BQ_DATASET") or "").strip().strip("`")
# BQ_TABLE = (os.getenv("BQ_TABLE") or "shopline_event_record_origin").strip().strip("`")
BQ_PROJECT = 'project-fddee9ed-d147-4ffe-b75'
BQ_DATASET = 'From_mysql'
BQ_TABLE = 'shopline_event_record_origin'
BQ_WATERMARK_TABLE = (os.getenv("BQ_WATERMARK_TABLE") or "sync_watermark").strip().strip("`")

MODE = os.getenv("MODE", "incremental")  # full / incremental

if not all([MYSQL_HOST, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DB, BQ_PROJECT, BQ_DATASET]):
    raise ValueError(
        "Missing required env vars: "
        "MYSQL_HOST/MYSQL_USER/MYSQL_PASSWORD/MYSQL_DB/BQ_PROJECT/BQ_DATASET"
    )

print(f"BQ_PROJECT={BQ_PROJECT}, BQ_DATASET={BQ_DATASET}, BQ_TABLE={BQ_TABLE}, MODE={MODE}")

bq = bigquery.Client(project=BQ_PROJECT)
TARGET_TABLE_ID = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"
STG_TABLE_ID = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}_stg"
WATERMARK_TABLE_ID = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_WATERMARK_TABLE}"


def get_mysql_conn():
    return pymysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DB,
        charset="utf8mb4",
    )


def ensure_tables():
    target_sql = f"""
    CREATE TABLE IF NOT EXISTS `{TARGET_TABLE_ID}` (
      id INT64,
      event_id STRING,
      event_name STRING,
      event_time TIMESTAMP,
      client_id STRING,
      customer_id STRING,
      login_em STRING,
      referer STRING,
      event_data STRING,
      event_content STRING,
      create_time TIMESTAMP
    )
    PARTITION BY DATE(create_time)
    CLUSTER BY event_name, client_id, customer_id
    """

    stg_sql = f"""
    CREATE OR REPLACE TABLE `{STG_TABLE_ID}` (
      id INT64,
      event_id STRING,
      event_name STRING,
      event_time TIMESTAMP,
      client_id STRING,
      customer_id STRING,
      login_em STRING,
      referer STRING,
      event_data STRING,
      event_content STRING,
      create_time TIMESTAMP
    )
    """

    watermark_sql = f"""
    CREATE TABLE IF NOT EXISTS `{WATERMARK_TABLE_ID}` (
      source_table STRING,
      last_synced_id INT64,
      updated_at TIMESTAMP
    )
    """

    bq.query(target_sql).result()
    bq.query(stg_sql).result()
    bq.query(watermark_sql).result()


def get_last_synced_id():
    sql = f"""
    SELECT last_synced_id
    FROM `{WATERMARK_TABLE_ID}`
    WHERE source_table = @source_table
    QUALIFY ROW_NUMBER() OVER (PARTITION BY source_table ORDER BY updated_at DESC) = 1
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("source_table", "STRING", BQ_TABLE)
        ]
    )
    rows = list(bq.query(sql, job_config=job_config).result())
    if rows and rows[0]["last_synced_id"] is not None:
        return int(rows[0]["last_synced_id"])
    return 0


def update_last_synced_id(last_id: int):
    sql = f"""
    INSERT INTO `{WATERMARK_TABLE_ID}` (source_table, last_synced_id, updated_at)
    VALUES (@source_table, @last_synced_id, CURRENT_TIMESTAMP())
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("source_table", "STRING", BQ_TABLE),
            bigquery.ScalarQueryParameter("last_synced_id", "INT64", last_id),
        ]
    )
    bq.query(sql, job_config=job_config).result()


def fetch_mysql_data(mode: str, last_synced_id: int | None) -> pd.DataFrame:
    conn = get_mysql_conn()
    try:
        base_sql = """
        SELECT
          id,
          event_id,
          event_name,
          event_time,
          client_id,
          customer_id,
          login_em,
          referer,
          event_data,
          event_content,
          create_time
        FROM shopline_event_record_origin
        """
        if mode == "full":
            sql = base_sql + " ORDER BY id ASC"
            params = ()
        else:
            sql = base_sql + " WHERE id > %s ORDER BY id ASC"
            params = (last_synced_id,)

        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description]

        df = pd.DataFrame(rows, columns=cols)
        print(df.head(5).to_dict("records"))
        return df
    finally:
        conn.close()


def normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    if "id" in df.columns:
        df["id"] = pd.to_numeric(df["id"], errors="coerce").astype("Int64")

    for col in ["event_time", "create_time"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce", utc=False)

    for col in ["event_id", "event_name", "client_id", "customer_id", "login_em", "referer", "event_data", "event_content"]:
        if col in df.columns:
            df[col] = df[col].astype("string")

    return df


def row_to_json(row: dict) -> dict:
    result = {}
    for k, v in row.items():
        if pd.isna(v):
            result[k] = None
        elif isinstance(v, pd.Timestamp):
            # TIMESTAMP 用 ISO 格式更稳
            result[k] = v.strftime("%Y-%m-%d %H:%M:%S")
        elif isinstance(v, Decimal):
            result[k] = str(v)
        elif hasattr(v, "item"):
            result[k] = v.item()
        else:
            result[k] = v
    return result


def load_staging(df: pd.DataFrame):
    if df.empty:
        print("No rows to load into staging.")
        return

    records = [row_to_json(r) for r in df.to_dict(orient="records")]

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        schema=[
            bigquery.SchemaField("id", "INT64"),
            bigquery.SchemaField("event_id", "STRING"),
            bigquery.SchemaField("event_name", "STRING"),
            bigquery.SchemaField("event_time", "TIMESTAMP"),
            bigquery.SchemaField("client_id", "STRING"),
            bigquery.SchemaField("customer_id", "STRING"),
            bigquery.SchemaField("login_em", "STRING"),
            bigquery.SchemaField("referer", "STRING"),
            bigquery.SchemaField("event_data", "STRING"),
            bigquery.SchemaField("event_content", "STRING"),
            bigquery.SchemaField("create_time", "TIMESTAMP"),
        ],
    )

    bq.load_table_from_json(records, STG_TABLE_ID, job_config=job_config).result()
    print(f"Loaded {len(records)} rows into staging.")


def merge_target():
    sql = f"""
    MERGE `{TARGET_TABLE_ID}` T
    USING `{STG_TABLE_ID}` S
    ON T.id = S.id
    WHEN MATCHED THEN UPDATE SET
      event_id = S.event_id,
      event_name = S.event_name,
      event_time = S.event_time,
      client_id = S.client_id,
      customer_id = S.customer_id,
      login_em = S.login_em,
      referer = S.referer,
      event_data = S.event_data,
      event_content = S.event_content,
      create_time = S.create_time
    WHEN NOT MATCHED THEN
      INSERT (
        id, event_id, event_name, event_time, client_id,
        customer_id, login_em, referer, event_data, event_content, create_time
      )
      VALUES (
        S.id, S.event_id, S.event_name, S.event_time, S.client_id,
        S.customer_id, S.login_em, S.referer, S.event_data, S.event_content, S.create_time
      )
    """
    bq.query(sql).result()
    print("Merged staging into target.")


def main():
    ensure_tables()

    last_synced_id = 0
    if MODE != "full":
        last_synced_id = get_last_synced_id()
        print(f"last_synced_id={last_synced_id}")

    df = fetch_mysql_data(MODE, last_synced_id)
    print(f"fetched_rows={len(df)}")

    if df.empty:
        print("No rows to sync.")
        return

    df = normalize_df(df)
    load_staging(df)
    merge_target()

    if MODE != "full":
        new_last_id = int(df["id"].max())
        update_last_synced_id(new_last_id)
        print(f"updated_last_synced_id={new_last_id}")

    print("Sync finished.")


if __name__ == "__main__":
    main()
