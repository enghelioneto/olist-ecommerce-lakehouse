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

def add_bronze_metadata(df, file_name: str, batch_id: str):
    raw_columns = [column_name for column_name in df.columns if not column_name.startswith("_")]

    return (
        df
        .withColumn("_source_file", F.lit(file_name))
        .withColumn("_source_system", F.lit("olist_kaggle"))
        .withColumn("_batch_id", F.lit(batch_id))
        .withColumn("_ingested_at", F.to_timestamp(F.lit(batch_started_at_value)))
        .withColumn(
            "_row_hash",
            F.sha2(
                F.concat_ws(
                    "||",
                    *[
                        F.coalesce(F.col(column_name).cast("string"), F.lit(""))
                        for column_name in raw_columns
                    ]
                ),
                256
            )
        )
    )

# COMMAND ----------

def write_bronze(df, table_name: str):
    full_table_name = f"{catalog}.{bronze_schema}.{table_name}"

    (
        df.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", True)
        .saveAsTable(full_table_name)
    )

    return full_table_name

# COMMAND ----------

def build_ingestion_audit(
    table_name: str,
    file_name: str,
    source_row_count: int,
    appended_row_count: int,
    batch_id: str
):
    source_path = f"{raw_path}/{file_name}"
    skipped_row_count = source_row_count - appended_row_count

    if source_row_count == 0:
        status = "EMPTY_SOURCE"
    elif appended_row_count == 0:
        status = "SKIPPED_DUPLICATE"
    elif appended_row_count < source_row_count:
        status = "PARTIALLY_APPENDED"
    else:
        status = "APPENDED"

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
    df_bronze = add_bronze_metadata(df_raw, file_name, current_batch_id)
    full_table_name = f"{catalog}.{bronze_schema}.{table_name}"

    source_row_count = df_raw.count()

    if spark.catalog.tableExists(full_table_name):
        existing_hashes = (
            spark.table(full_table_name)
            .select("_row_hash")
            .where(F.col("_row_hash").isNotNull())
            .distinct()
        )

        df_to_append = df_bronze.join(existing_hashes, on="_row_hash", how="left_anti")
    else:
        df_to_append = df_bronze

    full_table_name = write_bronze(df_to_append, table_name)
    appended_row_count = (
        spark.table(full_table_name)
        .where(F.col("_batch_id") == current_batch_id)
        .count()
    )

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

