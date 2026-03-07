import os
import pymysql
import pandas as pd
from google.cloud import bigquery

# =========================
# Env
# =========================
MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
MYSQL_DB = os.getenv("MYSQL_DB", "caguuu_erp")

BQ_PROJECT = os.getenv("BQ_PROJECT")
BQ_DATASET = os.getenv("BQ_DATASET")
BQ_TABLE = os.getenv("BQ_TABLE", "shopline_order_statistics")

# 可选：指定重刷哪一天。格式 YYYY-MM-DD
# 如果不传，就默认刷“昨天”
SYNC_DATE = os.getenv("SYNC_DATE")

if not all([MYSQL_HOST, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DB, BQ_PROJECT, BQ_DATASET]):
    raise ValueError("Missing required env vars: MYSQL_HOST/MYSQL_USER/MYSQL_PASSWORD/MYSQL_DB/BQ_PROJECT/BQ_DATASET")

bq = bigquery.Client(project=BQ_PROJECT)
TARGET_TABLE_ID = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"


def get_mysql_conn():
    return pymysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DB,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
    )


def get_sync_date_sql():
    # 用 BigQuery 的 CURRENT_DATE() 统一控制默认日期
    if SYNC_DATE:
        return f"DATE '{SYNC_DATE}'"
    return "DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)"


def get_sync_date_for_mysql(conn):
    if SYNC_DATE:
        return SYNC_DATE

    sql = "SELECT DATE_SUB(CURDATE(), INTERVAL 1 DAY) AS sync_date"
    with conn.cursor() as cur:
        cur.execute(sql)
        row = cur.fetchone()
        return str(row["sync_date"])


def ensure_target_table():
    ddl = f"""
    CREATE TABLE IF NOT EXISTS `{TARGET_TABLE_ID}` (
      id INT64 NOT NULL,
      order_seq STRING NOT NULL,
      region STRING,
      province STRING,
      city STRING,
      sales_channel STRING,
      date_time DATETIME,
      gross_sales NUMERIC,
      discounts NUMERIC,
      net_sales NUMERIC,
      tax NUMERIC,
      express_tax_amount NUMERIC,
      member_point_amount NUMERIC,
      shipping NUMERIC,
      tips NUMERIC,
      refunds NUMERIC,
      total_sales NUMERIC,
      order_quantity INT64,
      return_quantity INT64,
      adjust_amount NUMERIC,
      refund_adjust_amt NUMERIC
    )
    PARTITION BY DATE(date_time)
    CLUSTER BY sales_channel, order_seq
    """
    bq.query(ddl).result()


def fetch_mysql_data(sync_date: str) -> pd.DataFrame:
    conn = get_mysql_conn()
    try:
        sql = """
        SELECT
          id,
          order_seq,
          region,
          province,
          city,
          sales_channel,
          date_time,
          gross_sales,
          discounts,
          net_sales,
          tax,
          express_tax_amount,
          member_point_amount,
          shipping,
          tips,
          refunds,
          total_sales,
          order_quantity,
          return_quantity,
          adjust_amount,
          refund_adjust_amt
        FROM shopline_order_statistics
        WHERE DATE(date_time) = %s
        ORDER BY id ASC
        """
        df = pd.read_sql(sql, conn, params=[sync_date])
        return df
    finally:
        conn.close()


def normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    df = df.where(pd.notnull(df), None)

    if "id" in df.columns:
        df["id"] = df["id"].astype("int64")

    for col in ["order_quantity", "return_quantity"]:
        if col in df.columns and df[col].notna().any():
            df[col] = df[col].astype("Int64")

    # MySQL DATETIME -> pandas datetime，后面写入 BQ DATETIME
    if "date_time" in df.columns:
        df["date_time"] = pd.to_datetime(df["date_time"], errors="coerce")

    string_cols = ["order_seq", "region", "province", "city", "sales_channel"]
    for col in string_cols:
        if col in df.columns:
            df[col] = df[col].astype("string")

    return df


def delete_bq_partition():
    sync_date_sql = get_sync_date_sql()
    sql = f"""
    DELETE FROM `{TARGET_TABLE_ID}`
    WHERE DATE(date_time) = {sync_date_sql}
    """
    bq.query(sql).result()
    print("Deleted target partition in BigQuery.")


def load_to_bq(df: pd.DataFrame):
    if df.empty:
        print("No rows found for the sync date. Skip load.")
        return

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        schema=[
            bigquery.SchemaField("id", "INT64", mode="REQUIRED"),
            bigquery.SchemaField("order_seq", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("region", "STRING"),
            bigquery.SchemaField("province", "STRING"),
            bigquery.SchemaField("city", "STRING"),
            bigquery.SchemaField("sales_channel", "STRING"),
            bigquery.SchemaField("date_time", "DATETIME"),
            bigquery.SchemaField("gross_sales", "NUMERIC"),
            bigquery.SchemaField("discounts", "NUMERIC"),
            bigquery.SchemaField("net_sales", "NUMERIC"),
            bigquery.SchemaField("tax", "NUMERIC"),
            bigquery.SchemaField("express_tax_amount", "NUMERIC"),
            bigquery.SchemaField("member_point_amount", "NUMERIC"),
            bigquery.SchemaField("shipping", "NUMERIC"),
            bigquery.SchemaField("tips", "NUMERIC"),
            bigquery.SchemaField("refunds", "NUMERIC"),
            bigquery.SchemaField("total_sales", "NUMERIC"),
            bigquery.SchemaField("order_quantity", "INT64"),
            bigquery.SchemaField("return_quantity", "INT64"),
            bigquery.SchemaField("adjust_amount", "NUMERIC"),
            bigquery.SchemaField("refund_adjust_amt", "NUMERIC"),
        ],
    )

    job = bq.load_table_from_dataframe(df, TARGET_TABLE_ID, job_config=job_config)
    job.result()
    print(f"Loaded {len(df)} rows into BigQuery.")


def main():
    ensure_target_table()

    conn = get_mysql_conn()
    try:
        sync_date = get_sync_date_for_mysql(conn)
    finally:
        conn.close()

    print(f"Sync date: {sync_date}")

    df = fetch_mysql_data(sync_date)
    print(f"MySQL fetched rows: {len(df)}")

    delete_bq_partition()

    if df.empty:
        print("No MySQL rows for sync date. BigQuery partition already cleaned.")
        return

    df = normalize_df(df)
    load_to_bq(df)
    print("Sync finished.")


if __name__ == "__main__":
    main()
