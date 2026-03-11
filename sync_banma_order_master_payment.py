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

# `project-fddee9ed-d147-4ffe-b75.From_mysql.banma_order_master_payment`
# BQ_PROJECT = (os.getenv("BQ_PROJECT") or "").strip().strip("`")
# BQ_DATASET = (os.getenv("BQ_DATASET") or "").strip().strip("`")

# BQ_TABLE = (os.getenv("BQ_TABLE") or "banma_order_master_payment").strip().strip("`")
BQ_PROJECT = 'project-fddee9ed-d147-4ffe-b75'
BQ_DATASET = 'From_mysql'
BQ_TABLE = 'banma_order_master_payment'


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


def get_sync_start_for_mysql(conn):
    if SYNC_DATE:
        # 支持传 2026-03-10 或 2026-03-10 00:00:00
        if len(SYNC_DATE) == 10:
            return f"{SYNC_DATE} 00:00:00"
        return SYNC_DATE

    sql = """
    SELECT CONCAT(DATE_SUB(CURDATE(), INTERVAL 1 DAY), ' 00:00:00') AS sync_start
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        row = cur.fetchone()
    return str(row[0])[0])


def ensure_tables():
    target_sql = f"""
    CREATE TABLE IF NOT EXISTS `{TARGET_TABLE_ID}` (
      id INT64,
      order_id INT64,
      pay_amount NUMERIC,
      pay_currency STRING,
      pay_amount_usd NUMERIC,
      pay_amount_cny NUMERIC,
      pay_freight NUMERIC,
      pay_freight_currency STRING,
      pay_freight_usd NUMERIC,
      pay_freight_cny NUMERIC,
      refund_amount NUMERIC,
      refund_currency STRING,
      refund_amount_usd NUMERIC,
      refund_amount_cny NUMERIC,
      discount_amount NUMERIC,
      discount_currency STRING,
      used_point NUMERIC,
      product_cost NUMERIC,
      freight_cost NUMERIC,
      pay_time DATETIME,
      pay_channel STRING,
      pay_status STRING,
      refund_status STRING,
      original_pay_time DATETIME,
      is_cod BOOL,
      tx_no STRING,
      purchase_freight NUMERIC,
      create_time DATETIME,
      update_time DATETIME
    )
    PARTITION BY DATE(update_time)
    CLUSTER BY order_id
    """

    stg_sql = f"""
    CREATE OR REPLACE TABLE `{STG_TABLE_ID}` (
      id INT64,
      order_id INT64,
      pay_amount NUMERIC,
      pay_currency STRING,
      pay_amount_usd NUMERIC,
      pay_amount_cny NUMERIC,
      pay_freight NUMERIC,
      pay_freight_currency STRING,
      pay_freight_usd NUMERIC,
      pay_freight_cny NUMERIC,
      refund_amount NUMERIC,
      refund_currency STRING,
      refund_amount_usd NUMERIC,
      refund_amount_cny NUMERIC,
      discount_amount NUMERIC,
      discount_currency STRING,
      used_point NUMERIC,
      product_cost NUMERIC,
      freight_cost NUMERIC,
      pay_time DATETIME,
      pay_channel STRING,
      pay_status STRING,
      refund_status STRING,
      original_pay_time DATETIME,
      is_cod BOOL,
      tx_no STRING,
      purchase_freight NUMERIC,
      create_time DATETIME,
      update_time DATETIME
    )
    """
    bq.query(target_sql).result()
    bq.query(stg_sql).result()


def fetch_mysql_data(mode: str, sync_start: str | None) -> pd.DataFrame:
    conn = get_mysql_conn()
    try:
        base_sql = """
        SELECT
          id,
          order_id,
          pay_amount,
          pay_currency,
          pay_amount_usd,
          pay_amount_cny,
          pay_freight,
          pay_freight_currency,
          pay_freight_usd,
          pay_freight_cny,
          refund_amount,
          refund_currency,
          refund_amount_usd,
          refund_amount_cny,
          discount_amount,
          discount_currency,
          used_point,
          product_cost,
          freight_cost,
          pay_time,
          pay_channel,
          pay_status,
          refund_status,
          original_pay_time,
          is_cod,
          tx_no,
          purchase_freight,
          create_time,
          update_time
        FROM banma_order_master_payment
        """
        if mode == "full":
            sql = base_sql + " ORDER BY order_id ASC"
            params = ()
        else:
            sql = base_sql + " WHERE update_time >= %s ORDER BY order_id ASC"
            params = (sync_start,)

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

    for col in ["id", "order_id"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    for col in ["pay_time", "original_pay_time", "create_time", "update_time"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    for col in [
        "pay_currency",
        "pay_freight_currency",
        "refund_currency",
        "discount_currency",
        "pay_channel",
        "pay_status",
        "refund_status",
        "tx_no",
    ]:
        if col in df.columns:
            df[col] = df[col].astype("string")

    if "is_cod" in df.columns:
        df["is_cod"] = df["is_cod"].map(lambda x: None if pd.isna(x) else bool(x))

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
            bigquery.SchemaField("order_id", "INT64"),
            bigquery.SchemaField("pay_amount", "NUMERIC"),
            bigquery.SchemaField("pay_currency", "STRING"),
            bigquery.SchemaField("pay_amount_usd", "NUMERIC"),
            bigquery.SchemaField("pay_amount_cny", "NUMERIC"),
            bigquery.SchemaField("pay_freight", "NUMERIC"),
            bigquery.SchemaField("pay_freight_currency", "STRING"),
            bigquery.SchemaField("pay_freight_usd", "NUMERIC"),
            bigquery.SchemaField("pay_freight_cny", "NUMERIC"),
            bigquery.SchemaField("refund_amount", "NUMERIC"),
            bigquery.SchemaField("refund_currency", "STRING"),
            bigquery.SchemaField("refund_amount_usd", "NUMERIC"),
            bigquery.SchemaField("refund_amount_cny", "NUMERIC"),
            bigquery.SchemaField("discount_amount", "NUMERIC"),
            bigquery.SchemaField("discount_currency", "STRING"),
            bigquery.SchemaField("used_point", "NUMERIC"),
            bigquery.SchemaField("product_cost", "NUMERIC"),
            bigquery.SchemaField("freight_cost", "NUMERIC"),
            bigquery.SchemaField("pay_time", "DATETIME"),
            bigquery.SchemaField("pay_channel", "STRING"),
            bigquery.SchemaField("pay_status", "STRING"),
            bigquery.SchemaField("refund_status", "STRING"),
            bigquery.SchemaField("original_pay_time", "DATETIME"),
            bigquery.SchemaField("is_cod", "BOOL"),
            bigquery.SchemaField("tx_no", "STRING"),
            bigquery.SchemaField("purchase_freight", "NUMERIC"),
            bigquery.SchemaField("create_time", "DATETIME"),
            bigquery.SchemaField("update_time", "DATETIME"),
        ],
    )

    bq.load_table_from_json(records, STG_TABLE_ID, job_config=job_config).result()
    print(f"Loaded {len(records)} rows into staging.")


def merge_target():
    sql = f"""
    MERGE `{TARGET_TABLE_ID}` T
    USING `{STG_TABLE_ID}` S
    ON T.order_id = S.order_id
    WHEN MATCHED THEN UPDATE SET
      id = S.id,
      pay_amount = S.pay_amount,
      pay_currency = S.pay_currency,
      pay_amount_usd = S.pay_amount_usd,
      pay_amount_cny = S.pay_amount_cny,
      pay_freight = S.pay_freight,
      pay_freight_currency = S.pay_freight_currency,
      pay_freight_usd = S.pay_freight_usd,
      pay_freight_cny = S.pay_freight_cny,
      refund_amount = S.refund_amount,
      refund_currency = S.refund_currency,
      refund_amount_usd = S.refund_amount_usd,
      refund_amount_cny = S.refund_amount_cny,
      discount_amount = S.discount_amount,
      discount_currency = S.discount_currency,
      used_point = S.used_point,
      product_cost = S.product_cost,
      freight_cost = S.freight_cost,
      pay_time = S.pay_time,
      pay_channel = S.pay_channel,
      pay_status = S.pay_status,
      refund_status = S.refund_status,
      original_pay_time = S.original_pay_time,
      is_cod = S.is_cod,
      tx_no = S.tx_no,
      purchase_freight = S.purchase_freight,
      create_time = S.create_time,
      update_time = S.update_time
    WHEN NOT MATCHED THEN
      INSERT (
        id, order_id, pay_amount, pay_currency, pay_amount_usd, pay_amount_cny,
        pay_freight, pay_freight_currency, pay_freight_usd, pay_freight_cny,
        refund_amount, refund_currency, refund_amount_usd, refund_amount_cny,
        discount_amount, discount_currency, used_point, product_cost, freight_cost,
        pay_time, pay_channel, pay_status, refund_status, original_pay_time,
        is_cod, tx_no, purchase_freight, create_time, update_time
      )
      VALUES (
        S.id, S.order_id, S.pay_amount, S.pay_currency, S.pay_amount_usd, S.pay_amount_cny,
        S.pay_freight, S.pay_freight_currency, S.pay_freight_usd, S.pay_freight_cny,
        S.refund_amount, S.refund_currency, S.refund_amount_usd, S.refund_amount_cny,
        S.discount_amount, S.discount_currency, S.used_point, S.product_cost, S.freight_cost,
        S.pay_time, S.pay_channel, S.pay_status, S.refund_status, S.original_pay_time,
        S.is_cod, S.tx_no, S.purchase_freight, S.create_time, S.update_time
      )
    """
    bq.query(sql).result()
    print("Merged staging into target.")


def main():
    ensure_tables()

    sync_start = None
    if MODE != "full":
        conn = get_mysql_conn()
        try:
            sync_start = get_sync_start_for_mysql(conn)
        finally:
            conn.close()
    
    print(f"sync_start={sync_start}")
    
    df = fetch_mysql_data(MODE, sync_start)
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
