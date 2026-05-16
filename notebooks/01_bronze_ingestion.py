# Databricks notebook source
# dbutils.fs.ls("/Volumes/olist_lakehouse/raw/landing")
from uuid import uuid4
from pyspark.sql import functions as F

# COMMAND ----------

catalog = "olist_lakehouse"
raw_schema = "raw"
bronze_schema = "bronze"

raw_path = "/Volumes/olist_lakehouse/raw/landing"
ingestion_audit_table = f"{catalog}.{raw_schema}.ingestion_audit"
current_batch_id = str(uuid4())

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
    return (
        df
        .withColumn("_source_file", F.lit(file_name))
        .withColumn("_source_system", F.lit("olist_kaggle"))
        .withColumn("_batch_id", F.lit(batch_id))
        .withColumn("_ingested_at_utc", F.current_timestamp())
        .withColumn(
            "_row_hash",    # nome da nova coluna hash
            F.sha2(         # aplica hash SHA-2
                F.concat_ws(
                    "||",   # separador usado entre os valores das colunas
                    *[      # percorre todas as colunas do DataFrame
                            # pega a coluna, converte pra string e troca null por string vazia
                        F.coalesce(F.col(c).cast("string"), F.lit(""))
                        for c in df.columns if not c.startswith("_")
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

def build_ingestion_audit(table_name: str, file_name: str, row_count: int, batch_id: str):
    source_path = f"{raw_path}/{file_name}"

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
            F.current_date().alias("_ingestion_date_utc"),
            F.current_timestamp().alias("_logged_at_utc"),
            F.lit(row_count).alias("row_count")
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

for table_name, file_name in files.items():
    # full_table_name = f"{catalog}.{bronze_schema}.{table_name}"

    print(f"Reading file: {file_name} | batch_id={current_batch_id}")

    df_raw = read_csv_from_volume(file_name)
    df_bronze = add_bronze_metadata(df_raw, file_name, current_batch_id)
    row_count = df_bronze.count()

    full_table_name = write_bronze(df_bronze, table_name)
    audit_df = build_ingestion_audit(table_name, file_name, row_count, current_batch_id)
    write_ingestion_audit(audit_df)

    print(f"Appended {row_count} rows into Bronze table: {full_table_name}")

# COMMAND ----------


