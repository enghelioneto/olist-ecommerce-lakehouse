# Databricks notebook source

# dbutils.fs.ls("/Volumes/olist_lakehouse/raw/landing")

from pyspark.sql import functions as F

# COMMAND ----------

catalog = "olist_lakehouse"
bronze_schema = "bronze"

raw_path = "/Volumes/olist_lakehouse/raw/landing"

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

# COMMAND ---------

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

display(read_csv_from_volume("olist_customers_dataset.csv"))