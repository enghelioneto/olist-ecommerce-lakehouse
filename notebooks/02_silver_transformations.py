# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------

catalog = "olist_lakehouse"
bronze_schema = "bronze"
silver_schema = "silver"

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

df_customers = read_bronze("customers")

silver_customers = (
    df_customers
    .select(
        F.col("customer_id"),
        F.col("customer_unique_id"),
        F.col("customer_zip_code_prefix"),
        F.lower(F.trim(F.col("customer_city"))).alias("customer_city"),
        F.upper(F.trim(F.col("customer_state"))).alias("customer_state"),
        F.col("_source_file"),
        F.col("_source_system"),
        F.col("_ingested_at_utc")
    )
    .dropDuplicates(["customer_id"])
    .withColumn("_silver_processed_at_utc", F.current_timestamp())
)

write_silver(silver_customers, "customers")
# display(silver_customers.limit(10))

# COMMAND ----------

df_orders = read_bronze("orders")

silver_orders = (
    df_orders
    .select(
        F.col("order_id"),
        F.col("customer_id"),
        F.lower(F.trim(F.col("order_status"))).alias("order_status"),
        F.to_timestamp("order_purchase_timestamp").alias("order_purchase_timestamp"),
        F.to_timestamp("order_approved_at").alias("order_approved_at"),
        F.to_timestamp("order_delivered_carrier_date").alias("order_delivered_carrier_date"),
        F.to_timestamp("order_delivered_customer_date").alias("order_delivered_customer_date"),
        F.to_timestamp("order_estimated_delivery_date").alias("order_estimated_delivery_date"),
        F.col("_source_file"),
        F.col("_source_system"),
        F.col("_ingested_at_utc")
    )
    .dropDuplicates(["order_id"])
    .withColumn("_silver_processed_at_utc", F.current_timestamp())
)

write_silver(silver_orders, "orders")

# COMMAND ----------

df_order_items = read_bronze("order_items")
# display(df_order_items.limit(1))
silver_order_items = (
    df_order_items
    .select(
        F.col("order_id"),
        F.col("order_item_id").cast("int").alias("order_item_id"),
        F.col("product_id"),
        F.col("seller_id"),
        F.to_timestamp("shipping_limit_date").alias("shipping_limit_date"),
        F.col("price").cast("decimal(12,2)").alias("price"),
        F.col("freight_value").cast("decimal(12,2)").alias("freight_value"),
        F.col("_source_file"),
        F.col("_source_system"),
        F.col("_ingested_at_utc")
    )
    .dropDuplicates(["order_id", "order_item_id"])
    .withColumn("_silver_processed_at_utc", F.current_timestamp())
)

# display(silver_order_items.limit(1))
write_silver(silver_order_items, "order_items")

# COMMAND ----------

df_order_payments = read_bronze("order_payments")
# display(df_order_payments.limit(100))

silver_order_payments = (
    df_order_payments
    .select(
        F.col("order_id"),
        F.col("payment_sequential").cast("int").alias("payment_sequential"),
        F.lower(F.trim(F.col("payment_type"))).alias("payment_type"),
        F.col("payment_installments").cast("int").alias("payment_installments"),
        F.col("payment_value").cast("decimal(12,2)").alias("payment_value"),
        F.col("_source_file"),
        F.col("_source_system"),
        F.col("_ingested_at_utc")
    )
    .dropDuplicates(["order_id", "payment_sequential"])
    .withColumn("_silver_processed_at_utc", F.current_timestamp())
)

write_silver(silver_order_payments, "order_payments")

# COMMAND ----------

df_order_reviews = read_bronze("order_reviews")
# display(df_order_reviews.limit(100))

silver_order_reviews = (
    df_order_reviews
    .select(
        F.col("review_id"),
        F.col("order_id"),
        F.col("review_score").cast("int").alias("review_score"),
        F.trim(F.col("review_comment_title")).alias("review_comment_title"),
        F.trim(F.col("review_comment_message")).alias("review_comment_message"),
        F.to_timestamp("review_creation_date").alias("review_creation_date"),
        F.to_timestamp("review_answer_timestamp").alias("review_answer_timestamp"),
        F.col("_source_file"),
        F.col("_source_system"),
        F.col("_ingested_at_utc")
    )
    .dropDuplicates(["review_id", "order_id"])
    .withColumn("_silver_processed_at_utc", F.current_timestamp())
)

write_silver(silver_order_reviews, "order_reviews")

# COMMAND ----------

df_geolocation = read_bronze("geolocation")
# display(df_geolocation.limit(10))

silver_geolocation = (
    df_geolocation
    .select(
        F.col("geolocation_zip_code_prefix"),
        F.col("geolocation_lat").cast("double").alias("geolocation_lat"),
        F.col("geolocation_lng").cast("double").alias("geolocation_lng"),
        F.lower(F.trim(F.col("geolocation_city"))).alias("geolocation_city"),
        F.upper(F.trim(F.col("geolocation_state"))).alias("geolocation_state"),
        F.col("_source_file"),
        F.col("_source_system"),
        F.col("_ingested_at_utc")
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

# display(silver_geolocation.limit(100))
write_silver(silver_geolocation, "geolocation")

# COMMAND ----------

df_products = read_bronze("products")
# display(df_products.limit(100))

silver_products = (
    df_products
    .select(
        F.col("product_id"),
        F.lower(F.trim(F.col("product_category_name"))).alias("product_category_name"),
        F.col("product_name_lenght").cast("int").alias("product_name_length"),
        F.col("product_description_lenght").cast("int").alias("product_description_length"),
        F.col("product_photos_qty").cast("int").alias("product_photos_qty"),
        F.col("product_weight_g").cast("double").alias("product_weight_g"),
        F.col("product_length_cm").cast("double").alias("product_length_cm"),
        F.col("product_height_cm").cast("double").alias("product_height_cm"),
        F.col("product_width_cm").cast("double").alias("product_width_cm"),
        F.col("_source_file"),
        F.col("_source_system"),
        F.col("_ingested_at_utc")
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
        F.col("seller_id"),
        F.col("seller_zip_code_prefix"),
        F.lower(F.trim(F.col("seller_city"))).alias("seller_city"),
        F.upper(F.trim(F.col("seller_state"))).alias("seller_state"),
        F.col("_source_file"),
        F.col("_source_system"),
        F.col("_ingested_at_utc")
    )
    .dropDuplicates(["seller_id"])
    .withColumn("_silver_processed_at_utc", F.current_timestamp())
)
display(silver_sellers.limit(10))
write_silver(silver_sellers, "sellers")

# COMMAND ----------

df_translation = read_bronze("product_category_translation")

silver_translation = (
    df_translation
    .select(
        F.lower(F.trim(F.col("product_category_name"))).alias("product_category_name"),
        F.lower(F.trim(F.col("product_category_name_english"))).alias("product_category_name_english"),
        F.col("_source_file"),
        F.col("_source_system"),
        F.col("_ingested_at_utc")
    )
    .dropDuplicates(["product_category_name"])
    .withColumn("_silver_processed_at_utc", F.current_timestamp())
)
# display(silver_translation.limit(10))
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
