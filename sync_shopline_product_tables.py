import os
import math
from datetime import date, datetime
from decimal import Decimal

import pymysql
import pandas as pd
from google.cloud import bigquery

# =========================
# Env
# =========================
MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_PORT = int(os.getenv("MYSQL_PORT") or "3306")
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")

BQ_PROJECT = "project-fddee9ed-d147-4ffe-b75"
BQ_DATASET = "From_mysql"
MODE = os.getenv("MODE", "incremental")  # full / incremental

if not all([MYSQL_HOST, MYSQL_USER, MYSQL_PASSWORD]):
    raise ValueError("Missing required env vars: MYSQL_HOST / MYSQL_USER / MYSQL_PASSWORD")

print(f"MODE={MODE}")
bq = bigquery.Client(project=BQ_PROJECT)

# =========================
# Explicit BQ schemas
# =========================
SCHEMA_SHOPLINE_PRODUCT_SKU = [
    bigquery.SchemaField("id",                "INT64"),
    bigquery.SchemaField("spu_id",            "INT64"),
    bigquery.SchemaField("sku",               "STRING"),
    bigquery.SchemaField("sku_id",            "STRING"),
    bigquery.SchemaField("title",             "STRING"),
    bigquery.SchemaField("image",             "STRING"),   # JSON column, store as string
    bigquery.SchemaField("price",             "NUMERIC"),
    bigquery.SchemaField("compare_at_price",  "NUMERIC"),
    bigquery.SchemaField("barcode",           "STRING"),
    bigquery.SchemaField("weight",            "NUMERIC"),
    bigquery.SchemaField("weight_unit",       "STRING"),
    bigquery.SchemaField("taxable",           "BOOL"),
    bigquery.SchemaField("required_shipping", "BOOL"),
    bigquery.SchemaField("position",          "INT64"),
    bigquery.SchemaField("create_time",       "DATETIME"),
    bigquery.SchemaField("update_time",       "DATETIME"),
]

SCHEMA_GAIA_PRODUCT_PRODUCT = [
    bigquery.SchemaField("id",                   "INT64"),
    bigquery.SchemaField("product_name",         "STRING"),
    bigquery.SchemaField("product_code",         "STRING"),
    bigquery.SchemaField("leaf_category_id",     "INT64"),
    bigquery.SchemaField("thumbnail",            "STRING"),
    bigquery.SchemaField("supplier_id",          "INT64"),
    bigquery.SchemaField("description",          "STRING"),
    bigquery.SchemaField("maintainer",           "INT64"),
    bigquery.SchemaField("creator",              "INT64"),
    bigquery.SchemaField("status",               "INT64"),
    bigquery.SchemaField("create_time",          "DATETIME"),
    bigquery.SchemaField("update_time",          "DATETIME"),
    bigquery.SchemaField("require_sku_qty",      "INT64"),
    bigquery.SchemaField("description_hash",     "STRING"),
    bigquery.SchemaField("collocation_product",  "STRING"),
    bigquery.SchemaField("product_psd_link",     "STRING"),
]

SCHEMA_GAIA_PRODUCT_CATEGORY = [
    bigquery.SchemaField("id",               "INT64"),
    bigquery.SchemaField("category_name",    "STRING"),
    bigquery.SchemaField("category_code",    "STRING"),
    bigquery.SchemaField("parent_id",        "INT64"),
    bigquery.SchemaField("level",            "INT64"),
    bigquery.SchemaField("icon",             "STRING"),
    bigquery.SchemaField("description",      "STRING"),
    bigquery.SchemaField("is_leaf",          "BOOL"),
    bigquery.SchemaField("sort_order",       "INT64"),
    bigquery.SchemaField("category_owners",  "STRING"),   # JSON column
    bigquery.SchemaField("create_time",      "DATETIME"),
    bigquery.SchemaField("update_time",      "DATETIME"),
    bigquery.SchemaField("operation_owners", "STRING"),   # JSON column
]

SCHEMA_SHOPLINE_PRODUCT_SPU = [
    bigquery.SchemaField("id",               "INT64"),
    bigquery.SchemaField("spu",              "STRING"),
    bigquery.SchemaField("spu_id",           "STRING"),
    bigquery.SchemaField("handle",           "STRING"),
    bigquery.SchemaField("title",            "STRING"),
    bigquery.SchemaField("subtitle",         "STRING"),
    bigquery.SchemaField("body_html",        "STRING"),
    bigquery.SchemaField("image",            "STRING"),   # JSON column
    bigquery.SchemaField("images",           "STRING"),   # JSON column
    bigquery.SchemaField("vendor",           "STRING"),
    bigquery.SchemaField("product_type",     "STRING"),
    bigquery.SchemaField("product_category", "STRING"),
    bigquery.SchemaField("tags",             "STRING"),
    bigquery.SchemaField("status",           "STRING"),
    bigquery.SchemaField("created_at",       "DATETIME"),
    bigquery.SchemaField("updated_at",       "DATETIME"),
    bigquery.SchemaField("published_at",     "DATETIME"),
    bigquery.SchemaField("published_scope",  "STRING"),
    bigquery.SchemaField("template_path",    "STRING"),
    bigquery.SchemaField("path",             "STRING"),
    bigquery.SchemaField("create_time",      "DATETIME"),
    bigquery.SchemaField("update_time",      "DATETIME"),
]

# (mysql_db, mysql_table, bq_table, schema)
TABLES = [
    ("caguuu_erp",   "shopline_product_sku", "shopline_product_sku",  SCHEMA_SHOPLINE_PRODUCT_SKU),
    ("gaia_product", "product",              "gaia_product_product",  SCHEMA_GAIA_PRODUCT_PRODUCT),
    ("gaia_product", "category",             "gaia_product_category", SCHEMA_GAIA_PRODUCT_CATEGORY),
    ("caguuu_erp",   "shopline_product_spu", "shopline_product_spu",  SCHEMA_SHOPLINE_PRODUCT_SPU),
]


# =========================
# MySQL helpers
# =========================
def get_mysql_conn(db: str):
    return pymysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=db,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
    )


def fetch_full(mysql_db: str, table: str) -> pd.DataFrame:
    conn = get_mysql_conn(mysql_db)
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT * FROM `{table}`")
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description]
        df = pd.DataFrame(rows, columns=cols)
        print(f"  [{mysql_db}.{table}] full fetch: {len(df)} rows")
        return df
    finally:
        conn.close()


def fetch_incremental(mysql_db: str, table: str) -> pd.DataFrame:
    """取 update_time 近两天的数据"""
    conn = get_mysql_conn(mysql_db)
    try:
        sql = f"""
            SELECT * FROM `{table}`
            WHERE update_time >= DATE_SUB(CURDATE(), INTERVAL 1 DAY)
        """
        with conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description]
        df = pd.DataFrame(rows, columns=cols)
        print(f"  [{mysql_db}.{table}] incremental fetch: {len(df)} rows")
        return df
    finally:
        conn.close()


# =========================
# BigQuery helpers
# =========================
def bq_type(field: bigquery.SchemaField) -> str:
    return field.field_type


def ensure_target_table(target_id: str, schema: list):
    cols = ",\n  ".join(f"`{f.name}` {bq_type(f)}" for f in schema)
    bq.query(f"""
        CREATE TABLE IF NOT EXISTS `{target_id}` (
          {cols}
        )
        PARTITION BY DATE(update_time)
    """).result()


def ensure_staging_table(stg_id: str, schema: list):
    cols = ",\n  ".join(f"`{f.name}` {bq_type(f)}" for f in schema)
    bq.query(f"CREATE OR REPLACE TABLE `{stg_id}` (\n  {cols}\n)").result()


def build_merge_sql(target_id: str, stg_id: str, schema: list) -> str:
    all_cols   = [f.name for f in schema]
    non_id_cols = [f.name for f in schema if f.name != "id"]
    set_clause  = ",\n      ".join(f"`{c}` = S.`{c}`" for c in non_id_cols)
    ins_cols    = ", ".join(f"`{c}`" for c in all_cols)
    ins_vals    = ", ".join(f"S.`{c}`" for c in all_cols)
    return f"""
    MERGE `{target_id}` T
    USING `{stg_id}` S
    ON T.`id` = S.`id`
    WHEN MATCHED THEN UPDATE SET
      {set_clause}
    WHEN NOT MATCHED THEN
      INSERT ({ins_cols})
      VALUES ({ins_vals})
    """


def normalize_df(df: pd.DataFrame, bool_cols: set, int_cols: set) -> pd.DataFrame:
    """在 DataFrame 层做类型转换，避免 float→INT64 写入 BQ 报错。"""
    for col in int_cols:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: None if (x is None or (isinstance(x, float) and math.isnan(x))) else int(x))
    for col in bool_cols:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: None if (x is None or (isinstance(x, float) and math.isnan(x))) else bool(x))
    df = df.where(pd.notnull(df), None)
    return df


def row_to_json(row: dict, bool_cols: set) -> dict:
    result = {}
    for k, v in row.items():
        if v is None:
            result[k] = None
        elif isinstance(v, float) and math.isnan(v):
            result[k] = None
        elif isinstance(v, pd.Timestamp):
            result[k] = v.strftime("%Y-%m-%d %H:%M:%S")
        elif isinstance(v, datetime):
            result[k] = v.strftime("%Y-%m-%d %H:%M:%S")
        elif isinstance(v, date):
            result[k] = v.strftime("%Y-%m-%d")
        elif isinstance(v, Decimal):
            result[k] = str(v)
        elif hasattr(v, "item"):
            result[k] = v.item()
        else:
            result[k] = v
    return result


def load_records(records: list, table_id: str, schema: list, write_disposition: str):
    job_config = bigquery.LoadJobConfig(
        write_disposition=write_disposition,
        schema=schema,
    )
    bq.load_table_from_json(records, table_id, job_config=job_config).result()


# =========================
# Sync logic
# =========================
def sync_table(mysql_db: str, mysql_table: str, bq_table: str, schema: list):
    print(f"\n=== {mysql_db}.{mysql_table} -> {BQ_DATASET}.{bq_table}  [{MODE}] ===")
    target_id = f"{BQ_PROJECT}.{BQ_DATASET}.{bq_table}"
    stg_id    = f"{BQ_PROJECT}.{BQ_DATASET}.{bq_table}_stg"
    bool_cols = {f.name for f in schema if f.field_type == "BOOL"}
    int_cols  = {f.name for f in schema if f.field_type == "INT64"}

    if MODE == "full":
        df = fetch_full(mysql_db, mysql_table)
        if df.empty:
            print("  No rows, skipping.")
            return
        df = normalize_df(df, bool_cols, int_cols)
        records = [row_to_json(r, bool_cols) for r in df.to_dict(orient="records")]
        ensure_target_table(target_id, schema)
        load_records(records, target_id, schema, "WRITE_TRUNCATE")
        print(f"  Full load: {len(records)} rows -> {target_id}")

    else:  # incremental
        df = fetch_incremental(mysql_db, mysql_table)
        if df.empty:
            print("  No updated rows in the past 2 days, skipping.")
            return
        df = normalize_df(df, bool_cols, int_cols)
        records = [row_to_json(r, bool_cols) for r in df.to_dict(orient="records")]

        ensure_target_table(target_id, schema)
        ensure_staging_table(stg_id, schema)
        load_records(records, stg_id, schema, "WRITE_TRUNCATE")
        print(f"  Loaded {len(records)} rows into staging.")

        merge_sql = build_merge_sql(target_id, stg_id, schema)
        bq.query(merge_sql).result()
        print(f"  Merged staging into {target_id}.")


def main():
    for mysql_db, mysql_table, bq_table, schema in TABLES:
        sync_table(mysql_db, mysql_table, bq_table, schema)
    print("\nAll tables synced.")


if __name__ == "__main__":
    main()
