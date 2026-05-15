# Databricks notebook source
from pyspark.sql import Window
from pyspark.sql import functions as F

# COMMAND ----------

spark.conf.set("spark.sql.session.timeZone", "UTC")

catalog = "olist_lakehouse"
bronze_schema = "bronze"
silver_schema = "silver"
quarantine_schema = "quarantine"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{quarantine_schema}")

def read_bronze(table_name: str):
    return spark.table(f"{catalog}.{bronze_schema}.{table_name}")

def write_silver(df, table_name: str):
    full_table_name = f"{catalog}.{silver_schema}.{table_name}"

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", True)
        .saveAsTable(full_table_name)
    )

    print(f"Created silver table: {full_table_name}")

# COMMAND ----------

def write_quarantine(df, table_name: str):
    full_table_name = f"{catalog}.{quarantine_schema}.{table_name}"

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", True)
        .saveAsTable(full_table_name)
    )

    print(f"Created quarantine table: {full_table_name}")

# COMMAND ----------

def blank_string_as_null(column_name: str):
    trimmed_column = F.trim(F.col(column_name))
    return F.when(trimmed_column == "", F.lit(None)).otherwise(trimmed_column)

# COMMAND ----------

def normalized_string(column_name: str, alias: str = None, case: str = None):
    normalized_column = blank_string_as_null(column_name)

    if case == "lower":
        normalized_column = F.lower(normalized_column)
    elif case == "upper":
        normalized_column = F.upper(normalized_column)

    return normalized_column.alias(alias or column_name)

# COMMAND ----------

def cast_from_raw(column_name: str, target_type: str, alias: str = None):
    return blank_string_as_null(column_name).cast(target_type).alias(alias or column_name)

# COMMAND ----------

def timestamp_from_raw(column_name: str, alias: str = None):
    return F.to_timestamp(blank_string_as_null(column_name)).alias(alias or column_name)

# COMMAND ----------

def zip_prefix_from_raw(column_name: str, alias: str = None):
    return F.lpad(blank_string_as_null(column_name), 5, "0").alias(alias or column_name)

# COMMAND ----------

def lineage_columns():
    return [
        F.col("_source_file"),
        F.col("_source_path"),
        F.col("_source_system"),
        F.col("_batch_id"),
        F.col("_ingestion_date_utc"),
        F.col("_ingested_at_utc"),
        F.col("_row_hash")
    ]

# COMMAND ----------

def deduplicate_with_quarantine(df, business_key_columns, quarantine_table_name: str):
    window_spec = Window.partitionBy(*business_key_columns).orderBy(
        F.col("_ingested_at_utc").desc(),
        F.col("_batch_id").desc(),
        F.col("_row_hash").desc()
    )

    ranked_df = df.withColumn("_dedupe_rank", F.row_number().over(window_spec))

    clean_df = ranked_df.filter(F.col("_dedupe_rank") == 1).drop("_dedupe_rank")
    quarantined_df = (
        ranked_df
        .filter(F.col("_dedupe_rank") > 1)
        .withColumn("_quarantine_reason", F.lit("duplicate_business_key_kept_latest_ingestion"))
        .withColumn("_quarantined_at_utc", F.current_timestamp())
    )

    write_quarantine(quarantined_df, quarantine_table_name)

    return clean_df

# COMMAND ----------

df_customers = read_bronze("customers")

silver_customers = (
    df_customers
    .select(
        normalized_string("customer_id"),
        normalized_string("customer_unique_id"),
        zip_prefix_from_raw("customer_zip_code_prefix"),
        normalized_string("customer_city", case="lower"),
        normalized_string("customer_state", case="upper"),
        *lineage_columns()
    )
    .dropDuplicates(["customer_id"])
    .withColumn("_silver_processed_at_utc", F.current_timestamp())
)

write_silver(silver_customers, "customers")

# COMMAND ----------

df_orders = read_bronze("orders")

silver_orders = (
    df_orders
    .select(
        normalized_string("order_id"),
        normalized_string("customer_id"),
        normalized_string("order_status", case="lower"),
        timestamp_from_raw("order_purchase_timestamp"),
        timestamp_from_raw("order_approved_at"),
        timestamp_from_raw("order_delivered_carrier_date"),
        timestamp_from_raw("order_delivered_customer_date"),
        timestamp_from_raw("order_estimated_delivery_date"),
        *lineage_columns()
    )
    .dropDuplicates(["order_id"])
    .withColumn("_silver_processed_at_utc", F.current_timestamp())
)

write_silver(silver_orders, "orders")

# COMMAND ----------

df_order_items = read_bronze("order_items")

silver_order_items_typed = (
    df_order_items
    .select(
        normalized_string("order_id"),
        cast_from_raw("order_item_id", "int"),
        normalized_string("product_id"),
        normalized_string("seller_id"),
        timestamp_from_raw("shipping_limit_date"),
        cast_from_raw("price", "decimal(10,2)"),
        cast_from_raw("freight_value", "decimal(10,2)"),
        *lineage_columns()
    )
)

silver_order_items = (
    deduplicate_with_quarantine(
        silver_order_items_typed,
        ["order_id", "order_item_id"],
        "order_items"
    )
    .withColumn("_silver_processed_at_utc", F.current_timestamp())
)

write_silver(silver_order_items, "order_items")

# COMMAND ----------

df_order_payments = read_bronze("order_payments")

silver_order_payments = (
    df_order_payments
    .select(
        normalized_string("order_id"),
        cast_from_raw("payment_sequential", "int"),
        normalized_string("payment_type", case="lower"),
        cast_from_raw("payment_installments", "int"),
        cast_from_raw("payment_value", "decimal(10,2)"),
        *lineage_columns()
    )
    .dropDuplicates(["order_id", "payment_sequential"])
    .withColumn("_silver_processed_at_utc", F.current_timestamp())
)

write_silver(silver_order_payments, "order_payments")

# COMMAND ----------

df_order_reviews = read_bronze("order_reviews")

silver_order_reviews = (
    df_order_reviews
    .select(
        normalized_string("review_id"),
        normalized_string("order_id"),
        cast_from_raw("review_score", "int"),
        normalized_string("review_comment_title"),
        normalized_string("review_comment_message"),
        timestamp_from_raw("review_creation_date"),
        timestamp_from_raw("review_answer_timestamp"),
        *lineage_columns()
    )
    .dropDuplicates(["review_id", "order_id"])
    .withColumn("_silver_processed_at_utc", F.current_timestamp())
)

write_silver(silver_order_reviews, "order_reviews")

# COMMAND ----------

df_geolocation = read_bronze("geolocation")

silver_geolocation = (
    df_geolocation
    .select(
        zip_prefix_from_raw("geolocation_zip_code_prefix"),
        cast_from_raw("geolocation_lat", "double"),
        cast_from_raw("geolocation_lng", "double"),
        normalized_string("geolocation_city", case="lower"),
        normalized_string("geolocation_state", case="upper"),
        *lineage_columns()
    )
    .dropDuplicates([
        "geolocation_zip_code_prefix",
        "geolocation_lat",
        "geolocation_lng",
        "geolocation_city",
        "geolocation_state"
    ])
    .withColumn("_silver_processed_at_utc", F.current_timestamp())
)

write_silver(silver_geolocation, "geolocation")

# COMMAND ----------

df_products = read_bronze("products")

silver_products = (
    df_products
    .select(
        normalized_string("product_id"),
        normalized_string("product_category_name", case="lower"),
        cast_from_raw("product_name_lenght", "int", "product_name_length"),
        cast_from_raw("product_description_lenght", "int", "product_description_length"),
        cast_from_raw("product_photos_qty", "int"),
        cast_from_raw("product_weight_g", "double"),
        cast_from_raw("product_length_cm", "double"),
        cast_from_raw("product_height_cm", "double"),
        cast_from_raw("product_width_cm", "double"),
        *lineage_columns()
    )
    .dropDuplicates(["product_id"])
    .withColumn("_silver_processed_at_utc", F.current_timestamp())
)

write_silver(silver_products, "products")

# COMMAND ----------

df_sellers = read_bronze("sellers")

silver_sellers = (
    df_sellers
    .select(
        normalized_string("seller_id"),
        zip_prefix_from_raw("seller_zip_code_prefix"),
        normalized_string("seller_city", case="lower"),
        normalized_string("seller_state", case="upper"),
        *lineage_columns()
    )
    .dropDuplicates(["seller_id"])
    .withColumn("_silver_processed_at_utc", F.current_timestamp())
)

write_silver(silver_sellers, "sellers")

# COMMAND ----------

df_translation = read_bronze("product_category_translation")

silver_translation = (
    df_translation
    .select(
        normalized_string("product_category_name", case="lower"),
        normalized_string("product_category_name_english", case="lower"),
        *lineage_columns()
    )
    .dropDuplicates(["product_category_name"])
    .withColumn("_silver_processed_at_utc", F.current_timestamp())
)

write_silver(silver_translation, "product_category_translation")

# COMMAND ----------

silver_tables = [
    "customers",
    "geolocation",
    "order_items",
    "order_payments",
    "order_reviews",
    "orders",
    "product_category_translation",
    "products",
    "sellers"
]

for table_name in silver_tables:
    full_table_name = f"{catalog}.{silver_schema}.{table_name}"
    total_rows = spark.table(full_table_name).count()
    print(f"{full_table_name}: {total_rows}")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SHOW TABLES IN olist_lakehouse.silver;
