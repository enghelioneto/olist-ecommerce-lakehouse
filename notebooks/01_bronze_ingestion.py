# Databricks notebook source
# dbutils.fs.ls("/Volumes/olist_lakehouse/raw/landing")
from datetime import datetime, timezone
from uuid import uuid4

from pyspark.sql import functions as F

# COMMAND ----------

spark.conf.set("spark.sql.session.timeZone", "UTC")

catalog = "olist_lakehouse"
raw_schema = "raw"
bronze_schema = "bronze"
metadata_schema = "metadata"

raw_path = f"/Volumes/{catalog}/{raw_schema}/landing"
ingestion_audit_table = f"{catalog}.{metadata_schema}.ingestion_audit"

batch_started_at = datetime.now(timezone.utc)
batch_started_at_value = batch_started_at.strftime("%Y-%m-%d %H:%M:%S.%f")
current_batch_id = f"{batch_started_at:%Y%m%dT%H%M%S%fZ}_{uuid4().hex[:12]}"

files = {
    "customers": "olist_customers_dataset.csv",
    "geolocation": "olist_geolocation_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
    "order_payments": "olist_order_payments_dataset.csv",
    "order_reviews": "olist_order_reviews_dataset.csv",
    "orders": "olist_orders_dataset.csv",
    "products": "olist_products_dataset.csv",
    "sellers": "olist_sellers_dataset.csv",
    "product_category_translation": "product_category_name_translation.csv"
}

# COMMAND ----------

def read_csv_from_volume(file_name: str):
    """
    Reads a CSV file from the Databricks volume using all columns as string.

    In the Bronze layer, we avoid strong transformations.
    Type casting and business rules will be applied in the Silver layer.
    """
    file_path = f"{raw_path}/{file_name}"

    df = (
        spark.read
        .format("csv")
        .option("header", True)
        .option("inferSchema", False)
        .option("multiLine", True)
        .option("escape", '"')
        .option("quote", '"')
        .load(file_path)
    )

    return df

# display(read_csv_from_volume("olist_customers_dataset.csv"))

# COMMAND ----------

def get_raw_columns(df):
    return [column_name for column_name in df.columns if not column_name.startswith("_")]


def build_row_hash_expr(column_names, available_columns=None):
    available_columns = set(available_columns) if available_columns is not None else None

    return F.sha2(
        F.concat_ws(
            "||",
            *[
                (
                    F.coalesce(F.col(column_name).cast("string"), F.lit(""))
                    if available_columns is None or column_name in available_columns
                    else F.lit("")
                )
                for column_name in column_names
            ]
        ),
        256
    )


def add_bronze_metadata(df, file_name: str, batch_id: str):
    raw_columns = get_raw_columns(df)

    return (
        df
        .withColumn("_source_file", F.lit(file_name))
        .withColumn("_source_system", F.lit("olist_kaggle"))
        .withColumn("_batch_id", F.lit(batch_id))
        .withColumn("_ingested_at", F.to_timestamp(F.lit(batch_started_at_value)))
        .withColumn("_row_hash", build_row_hash_expr(raw_columns))
    )

# COMMAND ----------

def full_bronze_table_name(table_name: str):
    return f"{catalog}.{bronze_schema}.{table_name}"


def table_exists(full_table_name: str):
    return spark.catalog.tableExists(full_table_name)


def get_existing_row_hashes(full_table_name: str, raw_columns):
    existing_df = spark.table(full_table_name)
    computed_hashes = existing_df.select(
        build_row_hash_expr(raw_columns, existing_df.columns).alias("_row_hash")
    )

    if "_row_hash" in existing_df.columns:
        stored_hashes = existing_df.select(F.col("_row_hash"))
        return (
            stored_hashes
            .unionByName(computed_hashes)
            .where(F.col("_row_hash").isNotNull())
            .distinct()
        )

    return computed_hashes.where(F.col("_row_hash").isNotNull()).distinct()


def keep_new_rows_only(df, table_name: str, raw_columns):
    full_table_name = full_bronze_table_name(table_name)

    if not table_exists(full_table_name):
        return df

    existing_hashes = get_existing_row_hashes(full_table_name, raw_columns)
    return df.join(existing_hashes, on="_row_hash", how="left_anti")


def write_bronze(df, table_name: str):
    full_table_name = full_bronze_table_name(table_name)

    (
        df.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", True)
        .saveAsTable(full_table_name)
    )

    return full_table_name

# COMMAND ----------

def get_ingestion_status(source_row_count: int, appended_row_count: int):
    if source_row_count == 0:
        return "EMPTY_SOURCE"
    if appended_row_count == 0:
        return "SKIPPED_DUPLICATE"
    if appended_row_count < source_row_count:
        return "PARTIALLY_APPENDED"
    return "APPENDED"


def count_batch_rows(full_table_name: str, batch_id: str):
    table_df = spark.table(full_table_name)

    if "_batch_id" not in table_df.columns:
        return 0

    return table_df.where(F.col("_batch_id") == batch_id).count()


def build_ingestion_audit(
    table_name: str,
    file_name: str,
    source_row_count: int,
    appended_row_count: int,
    batch_id: str
):
    source_path = f"{raw_path}/{file_name}"
    skipped_row_count = source_row_count - appended_row_count
    status = get_ingestion_status(source_row_count, appended_row_count)

    return (
        spark.range(1)
        .select(
            F.lit(catalog).alias("catalog_name"),
            F.lit(bronze_schema).alias("schema_name"),
            F.lit(table_name).alias("table_name"),
            F.lit(file_name).alias("_source_file"),
            F.lit(source_path).alias("_source_path"),
            F.lit("olist_kaggle").alias("_source_system"),
            F.lit(batch_id).alias("_batch_id"),
            F.current_date().alias("_ingestion_date"),
            F.current_timestamp().alias("_logged_at"),
            F.lit(status).alias("status"),
            F.lit(source_row_count).alias("source_row_count"),
            F.lit(appended_row_count).alias("appended_row_count"),
            F.lit(skipped_row_count).alias("skipped_row_count"),
            F.lit(appended_row_count).alias("row_count")
        )
    )

# COMMAND ----------

def write_ingestion_audit(df):
    (
        df.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", True)
        .saveAsTable(ingestion_audit_table)
    )

# COMMAND ----------

print(f"Starting Bronze ingestion batch: {current_batch_id}")
print(f"Batch started at: {batch_started_at_value} UTC")

for table_name, file_name in files.items():
    print(f"Reading file: {file_name} | batch_id={current_batch_id}")

    df_raw = read_csv_from_volume(file_name)
    raw_columns = get_raw_columns(df_raw)
    df_bronze = add_bronze_metadata(df_raw, file_name, current_batch_id)
    df_to_append = keep_new_rows_only(df_bronze, table_name, raw_columns)

    source_row_count = df_raw.count()
    full_table_name = write_bronze(df_to_append, table_name)
    appended_row_count = count_batch_rows(full_table_name, current_batch_id)

    audit_df = build_ingestion_audit(
        table_name,
        file_name,
        source_row_count,
        appended_row_count,
        current_batch_id
    )
    write_ingestion_audit(audit_df)

    skipped_row_count = source_row_count - appended_row_count
    print(
        f"{full_table_name}: source_rows={source_row_count}, "
        f"appended_rows={appended_row_count}, skipped_rows={skipped_row_count}"
    )

# COMMAND ----------


