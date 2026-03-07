import os
import pymysql
import pandas as pd
from google.cloud import bigquery

MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_PORT = int(os.getenv("MYSQL_PORT") or "3306")
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
MYSQL_DB = os.getenv("MYSQL_DB", "gaia_fulfillment")

 # `project-fddee9ed-d147-4ffe-b75.From_mysql.banma_order_master
BQ_PROJECT = 'project-fddee9ed-d147-4ffe-b75'
BQ_DATASET = 'From_mysql'
BQ_TABLE = os.getenv("BQ_TABLE", "banma_order_master")

MODE = os.getenv("MODE", "incremental")  # full / incremental
SYNC_DATE = os.getenv("SYNC_DATE")       # YYYY-MM-DD，可空

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
    with conn.cursor() as cur:
        cur.execute("SELECT DATE_SUB(CURDATE(), INTERVAL 1 DAY)")
        row = cur.fetchone()
        return str(row[0])


def ensure_tables():
    target_sql = f"""
    CREATE TABLE IF NOT EXISTS `{TARGET_TABLE_ID}` (
      id INT64,
      order_id INT64,
      store_id INT64,
      store_name STRING,
      platform STRING,
      original_order_id STRING,
      display_order_id STRING,
      order_status INT64,
      shipping_type STRING,
      country_code STRING,
      country_name STRING,
      hold_status STRING,
      quantity INT64,
      user_identity STRING,
      order_time DATETIME,
      tags STRING,
      original_order_status STRING,
      original_order_time DATETIME,
      have_message BOOL,
      have_remark BOOL,
      latest_delivery_time DATETIME,
      create_time DATETIME,
      update_time DATETIME,
      flags STRING,
      email STRING,
      inventory_status INT64,
      inventory_mode STRING,
      buyer_name STRING,
      message STRING,
      shipping_time DATETIME,
      original_shipping_time DATETIME,
      original_tags STRING,
      buyer_phone STRING,
      requested_delivery_date DATETIME,
      requested_delivery_date_range DATETIME,
      risk_level STRING
    )
    PARTITION BY DATE(create_time)
    CLUSTER BY order_id, original_order_id
    """
    stg_sql = f"""
    CREATE OR REPLACE TABLE `{STG_TABLE_ID}` (
      id INT64,
      order_id INT64,
      store_id INT64,
      store_name STRING,
      platform STRING,
      original_order_id STRING,
      display_order_id STRING,
      order_status INT64,
      shipping_type STRING,
      country_code STRING,
      country_name STRING,
      hold_status STRING,
      quantity INT64,
      user_identity STRING,
      order_time DATETIME,
      tags STRING,
      original_order_status STRING,
      original_order_time DATETIME,
      have_message BOOL,
      have_remark BOOL,
      latest_delivery_time DATETIME,
      create_time DATETIME,
      update_time DATETIME,
      flags STRING,
      email STRING,
      inventory_status INT64,
      inventory_mode STRING,
      buyer_name STRING,
      message STRING,
      shipping_time DATETIME,
      original_shipping_time DATETIME,
      original_tags STRING,
      buyer_phone STRING,
      requested_delivery_date DATETIME,
      requested_delivery_date_range DATETIME,
      risk_level STRING
    )
    """
    bq.query(target_sql).result()
    bq.query(stg_sql).result()


def fetch_mysql_data(mode: str, sync_date: str | None) -> pd.DataFrame:
    conn = get_mysql_conn()
    try:
        base_sql = """
        SELECT
          id, order_id, store_id, store_name, platform, original_order_id,
          display_order_id, order_status, shipping_type, country_code, country_name,
          hold_status, quantity, user_identity, order_time, CAST(tags AS CHAR) AS tags,
          original_order_status, original_order_time, have_message, have_remark,
          latest_delivery_time, create_time, update_time, CAST(flags AS CHAR) AS flags,
          email, inventory_status, inventory_mode, buyer_name, message, shipping_time,
          original_shipping_time, CAST(original_tags AS CHAR) AS original_tags,
          buyer_phone, requested_delivery_date, requested_delivery_date_range, risk_level
        FROM banma_order_master
        """
        if mode == "full":
            sql = base_sql + " ORDER BY order_id ASC"
            params = ()
        else:
            sql = base_sql + " WHERE DATE(update_time) = %s ORDER BY order_id ASC"
            params = (sync_date,)

        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description]
        return pd.DataFrame(rows, columns=cols)
    finally:
        conn.close()


def normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    int_cols = [
        "id", "order_id", "store_id", "order_status", "quantity", "inventory_status"
    ]
    for col in int_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    bool_cols = ["have_message", "have_remark"]
    for col in bool_cols:
        if col in df.columns:
            df[col] = df[col].map(lambda x: None if pd.isna(x) else bool(x))

    dt_cols = [
        "order_time", "original_order_time", "latest_delivery_time",
        "create_time", "update_time", "shipping_time", "original_shipping_time",
        "requested_delivery_date", "requested_delivery_date_range"
    ]
    for col in dt_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    return df


def row_to_json(row: dict) -> dict:
    result = {}
    for k, v in row.items():
        if pd.isna(v):
            result[k] = None
        elif isinstance(v, pd.Timestamp):
            result[k] = v.strftime("%Y-%m-%d %H:%M:%S")
        elif hasattr(v, "item"):
            result[k] = v.item()
        else:
            result[k] = v
    return result


def load_staging(df: pd.DataFrame):
    records = [row_to_json(r) for r in df.to_dict(orient="records")]
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        schema=[
            bigquery.SchemaField("id", "INT64"),
            bigquery.SchemaField("order_id", "INT64"),
            bigquery.SchemaField("store_id", "INT64"),
            bigquery.SchemaField("store_name", "STRING"),
            bigquery.SchemaField("platform", "STRING"),
            bigquery.SchemaField("original_order_id", "STRING"),
            bigquery.SchemaField("display_order_id", "STRING"),
            bigquery.SchemaField("order_status", "INT64"),
            bigquery.SchemaField("shipping_type", "STRING"),
            bigquery.SchemaField("country_code", "STRING"),
            bigquery.SchemaField("country_name", "STRING"),
            bigquery.SchemaField("hold_status", "STRING"),
            bigquery.SchemaField("quantity", "INT64"),
            bigquery.SchemaField("user_identity", "STRING"),
            bigquery.SchemaField("order_time", "DATETIME"),
            bigquery.SchemaField("tags", "STRING"),
            bigquery.SchemaField("original_order_status", "STRING"),
            bigquery.SchemaField("original_order_time", "DATETIME"),
            bigquery.SchemaField("have_message", "BOOL"),
            bigquery.SchemaField("have_remark", "BOOL"),
            bigquery.SchemaField("latest_delivery_time", "DATETIME"),
            bigquery.SchemaField("create_time", "DATETIME"),
            bigquery.SchemaField("update_time", "DATETIME"),
            bigquery.SchemaField("flags", "STRING"),
            bigquery.SchemaField("email", "STRING"),
            bigquery.SchemaField("inventory_status", "INT64"),
            bigquery.SchemaField("inventory_mode", "STRING"),
            bigquery.SchemaField("buyer_name", "STRING"),
            bigquery.SchemaField("message", "STRING"),
            bigquery.SchemaField("shipping_time", "DATETIME"),
            bigquery.SchemaField("original_shipping_time", "DATETIME"),
            bigquery.SchemaField("original_tags", "STRING"),
            bigquery.SchemaField("buyer_phone", "STRING"),
            bigquery.SchemaField("requested_delivery_date", "DATETIME"),
            bigquery.SchemaField("requested_delivery_date_range", "DATETIME"),
            bigquery.SchemaField("risk_level", "STRING"),
        ],
    )
    bq.load_table_from_json(records, STG_TABLE_ID, job_config=job_config).result()


def merge_target():
    sql = f"""
    MERGE `{TARGET_TABLE_ID}` T
    USING `{STG_TABLE_ID}` S
    ON T.order_id = S.order_id
    WHEN MATCHED THEN UPDATE SET
      id = S.id,
      store_id = S.store_id,
      store_name = S.store_name,
      platform = S.platform,
      original_order_id = S.original_order_id,
      display_order_id = S.display_order_id,
      order_status = S.order_status,
      shipping_type = S.shipping_type,
      country_code = S.country_code,
      country_name = S.country_name,
      hold_status = S.hold_status,
      quantity = S.quantity,
      user_identity = S.user_identity,
      order_time = S.order_time,
      tags = S.tags,
      original_order_status = S.original_order_status,
      original_order_time = S.original_order_time,
      have_message = S.have_message,
      have_remark = S.have_remark,
      latest_delivery_time = S.latest_delivery_time,
      create_time = S.create_time,
      update_time = S.update_time,
      flags = S.flags,
      email = S.email,
      inventory_status = S.inventory_status,
      inventory_mode = S.inventory_mode,
      buyer_name = S.buyer_name,
      message = S.message,
      shipping_time = S.shipping_time,
      original_shipping_time = S.original_shipping_time,
      original_tags = S.original_tags,
      buyer_phone = S.buyer_phone,
      requested_delivery_date = S.requested_delivery_date,
      requested_delivery_date_range = S.requested_delivery_date_range,
      risk_level = S.risk_level
    WHEN NOT MATCHED THEN INSERT ROW
    """
    bq.query(sql).result()


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
