import os
from decimal import Decimal
import pymysql
import pandas as pd
from google.cloud import bigquery

MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_PORT = int(os.getenv("MYSQL_PORT") or "3306")
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
MYSQL_DB = os.getenv("MYSQL_DB") or "gaia_fulfillment"

# BQ_PROJECT = (os.getenv("BQ_PROJECT") or "").strip().strip("`")
# BQ_DATASET = (os.getenv("BQ_DATASET") or "").strip().strip("`")
# BQ_TABLE = (os.getenv("BQ_TABLE") or "banma_sku_sales_detail").strip().strip("`")
# `project-fddee9ed-d147-4ffe-b75.From_mysql.banma_sku_sales_detail`
BQ_PROJECT = 'project-fddee9ed-d147-4ffe-b75'
BQ_DATASET = 'From_mysql'
BQ_TABLE = 'banma_sku_sales_detail'

MODE = os.getenv("MODE", "incremental")  # full / incremental
SYNC_DATE = os.getenv("SYNC_DATE") or None

if not all([MYSQL_HOST, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DB, BQ_PROJECT, BQ_DATASET]):
    raise ValueError(
        "Missing required env vars: "
        "MYSQL_HOST/MYSQL_USER/MYSQL_PASSWORD/MYSQL_DB/BQ_PROJECT/BQ_DATASET"
    )

print(f"BQ_PROJECT={BQ_PROJECT}, BQ_DATASET={BQ_DATASET}, BQ_TABLE={BQ_TABLE}, MODE={MODE}, SYNC_DATE={SYNC_DATE}")

bq = bigquery.Client(project=BQ_PROJECT)
TARGET_TABLE_ID = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"
STG_TABLE_ID = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}_stg"


def get_mysql_conn():
    return pymysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DB,
        charset="utf8mb4",
    )


def get_sync_date_for_mysql(conn):
    if SYNC_DATE:
        return SYNC_DATE

    sql = "SELECT DATE_SUB(CURDATE(), INTERVAL 1 DAY) AS sync_date"
    with conn.cursor() as cur:
        cur.execute(sql)
        row = cur.fetchone()
        return str(row[0])


def ensure_tables():
    target_sql = f"""
    CREATE TABLE IF NOT EXISTS `{TARGET_TABLE_ID}` (
      id INT64,
      sku_code STRING,
      sku_count INT64,
      banma_order_id STRING,
      order_no STRING,
      supplier_id STRING,
      supplier_name STRING,
      platform STRING,
      order_time DATETIME,
      sale_amount NUMERIC,
      create_time DATETIME,
      update_time DATETIME,
      banma_order_detail_id STRING,
      pay_currency STRING,
      is_cancel BOOL,
      spu_code STRING,
      country STRING,
      erp_supplier_code STRING,
      erp_supplier_name STRING
    )
    PARTITION BY DATE(update_time)
    CLUSTER BY banma_order_detail_id, sku_code
    """

    stg_sql = f"""
    CREATE OR REPLACE TABLE `{STG_TABLE_ID}` (
      id INT64,
      sku_code STRING,
      sku_count INT64,
      banma_order_id STRING,
      order_no STRING,
      supplier_id STRING,
      supplier_name STRING,
      platform STRING,
      order_time DATETIME,
      sale_amount NUMERIC,
      create_time DATETIME,
      update_time DATETIME,
      banma_order_detail_id STRING,
      pay_currency STRING,
      is_cancel BOOL,
      spu_code STRING,
      country STRING,
      erp_supplier_code STRING,
      erp_supplier_name STRING
    )
    """
    bq.query(target_sql).result()
    bq.query(stg_sql).result()


def fetch_mysql_data(mode: str, sync_date: str | None) -> pd.DataFrame:
    conn = get_mysql_conn()
    try:
        base_sql = """
        SELECT
          id,
          sku_code,
          sku_count,
          banma_order_id,
          order_no,
          supplier_id,
          supplier_name,
          platform,
          order_time,
          sale_amount,
          create_time,
          update_time,
          banma_order_detail_id,
          pay_currency,
          is_cancel,
          spu_code,
          country,
          erp_supplier_code,
          erp_supplier_name
        FROM banma_sku_sales_detail
        """
        if mode == "full":
            sql = base_sql + " ORDER BY id ASC"
            params = ()
        else:
            sql = base_sql + " WHERE DATE(update_time) = %s ORDER BY id ASC"
            params = (sync_date,)

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

    for col in ["id", "sku_count"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    for col in ["order_time", "create_time", "update_time"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    string_cols = [
        "sku_code", "banma_order_id", "order_no", "supplier_id", "supplier_name",
        "platform", "banma_order_detail_id", "pay_currency", "spu_code", "country",
        "erp_supplier_code", "erp_supplier_name"
    ]
    for col in string_cols:
        if col in df.columns:
            df[col] = df[col].astype("string")

    if "is_cancel" in df.columns:
        df["is_cancel"] = df["is_cancel"].map(lambda x: None if pd.isna(x) else bool(x))

    return df


def row_to_json(row: dict) -> dict:
    result = {}
    for k, v in row.items():
        if pd.isna(v):
            result[k] = None
        elif isinstance(v, pd.Timestamp):
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
            bigquery.SchemaField("sku_code", "STRING"),
            bigquery.SchemaField("sku_count", "INT64"),
            bigquery.SchemaField("banma_order_id", "STRING"),
            bigquery.SchemaField("order_no", "STRING"),
            bigquery.SchemaField("supplier_id", "STRING"),
            bigquery.SchemaField("supplier_name", "STRING"),
            bigquery.SchemaField("platform", "STRING"),
            bigquery.SchemaField("order_time", "DATETIME"),
            bigquery.SchemaField("sale_amount", "NUMERIC"),
            bigquery.SchemaField("create_time", "DATETIME"),
            bigquery.SchemaField("update_time", "DATETIME"),
            bigquery.SchemaField("banma_order_detail_id", "STRING"),
            bigquery.SchemaField("pay_currency", "STRING"),
            bigquery.SchemaField("is_cancel", "BOOL"),
            bigquery.SchemaField("spu_code", "STRING"),
            bigquery.SchemaField("country", "STRING"),
            bigquery.SchemaField("erp_supplier_code", "STRING"),
            bigquery.SchemaField("erp_supplier_name", "STRING"),
        ],
    )

    bq.load_table_from_json(records, STG_TABLE_ID, job_config=job_config).result()
    print(f"Loaded {len(records)} rows into staging.")


def merge_target():
    sql = f"""
    MERGE `{TARGET_TABLE_ID}` T
    USING `{STG_TABLE_ID}` S
    ON T.banma_order_detail_id = S.banma_order_detail_id
    WHEN MATCHED THEN UPDATE SET
      id = S.id,
      sku_code = S.sku_code,
      sku_count = S.sku_count,
      banma_order_id = S.banma_order_id,
      order_no = S.order_no,
      supplier_id = S.supplier_id,
      supplier_name = S.supplier_name,
      platform = S.platform,
      order_time = S.order_time,
      sale_amount = S.sale_amount,
      create_time = S.create_time,
      update_time = S.update_time,
      pay_currency = S.pay_currency,
      is_cancel = S.is_cancel,
      spu_code = S.spu_code,
      country = S.country,
      erp_supplier_code = S.erp_supplier_code,
      erp_supplier_name = S.erp_supplier_name
    WHEN NOT MATCHED THEN
      INSERT (
        id, sku_code, sku_count, banma_order_id, order_no, supplier_id,
        supplier_name, platform, order_time, sale_amount, create_time, update_time,
        banma_order_detail_id, pay_currency, is_cancel, spu_code, country,
        erp_supplier_code, erp_supplier_name
      )
      VALUES (
        S.id, S.sku_code, S.sku_count, S.banma_order_id, S.order_no, S.supplier_id,
        S.supplier_name, S.platform, S.order_time, S.sale_amount, S.create_time, S.update_time,
        S.banma_order_detail_id, S.pay_currency, S.is_cancel, S.spu_code, S.country,
        S.erp_supplier_code, S.erp_supplier_name
      )
    """
    bq.query(sql).result()
    print("Merged staging into target.")


def main():
    ensure_tables()

    sync_date = None
    if MODE != "full":
        conn = get_mysql_conn()
        try:
            sync_date = get_sync_date_for_mysql(conn)
        finally:
            conn.close()
        print(f"sync_date={sync_date}")

    df = fetch_mysql_data(MODE, sync_date)
    print(f"fetched_rows={len(df)}")

    if df.empty:
        print("No rows to sync.")
        return

    df = normalize_df(df)
    load_staging(df)
    merge_target()
    print("Sync finished.")


if __name__ == "__main__":
    main()
