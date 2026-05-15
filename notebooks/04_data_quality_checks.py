# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------

spark.conf.set("spark.sql.session.timeZone", "UTC")

catalog = "olist_lakehouse"
raw_schema = "raw"
silver_schema = "silver"
quarantine_schema = "quarantine"

# COMMAND ----------

audit_df = spark.table(f"{catalog}.{raw_schema}.ingestion_audit")
silver_order_items = spark.table(f"{catalog}.{silver_schema}.order_items")
quarantine_order_items = spark.table(f"{catalog}.{quarantine_schema}.order_items")

silver_order_items_null_business_key = silver_order_items.filter(
    F.col("order_id").isNull() | F.col("order_item_id").isNull()
).count()

silver_order_items_negative_amounts = silver_order_items.filter(
    (F.col("price") < 0) | (F.col("freight_value") < 0)
).count()

quarantine_order_items_count = quarantine_order_items.count()
audit_row_count = audit_df.count()

checks = [
    {
        "check_name": "bronze_ingestion_audit_has_rows",
        "layer": "raw",
        "status": "pass" if audit_row_count > 0 else "fail",
        "metric_value": audit_row_count,
        "details": "Checks whether the technical ingestion audit history is being recorded."
    },
    {
        "check_name": "silver_order_items_null_business_key",
        "layer": "silver",
        "status": "pass" if silver_order_items_null_business_key == 0 else "fail",
        "metric_value": silver_order_items_null_business_key,
        "details": "Business keys should be populated after Silver typing and normalization."
    },
    {
        "check_name": "silver_order_items_negative_amounts",
        "layer": "silver",
        "status": "pass" if silver_order_items_negative_amounts == 0 else "fail",
        "metric_value": silver_order_items_negative_amounts,
        "details": "Price and freight should not be negative in the curated layer."
    },
    {
        "check_name": "quarantine_order_items_rejected_rows",
        "layer": "quarantine",
        "status": "info",
        "metric_value": quarantine_order_items_count,
        "details": "Tracks how many order_items rows were rejected by deterministic deduplication."
    }
]

dq_results = (
    spark.createDataFrame(checks)
    .withColumn("_checked_at_utc", F.current_timestamp())
)

# COMMAND ----------

(
    dq_results.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", True)
    .saveAsTable(f"{catalog}.{raw_schema}.data_quality_results")
)

display(dq_results.orderBy("layer", "check_name"))
