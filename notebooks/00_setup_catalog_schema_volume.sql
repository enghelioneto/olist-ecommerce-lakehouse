-- Databricks notebook source

CREATE CATALOG IF NOT EXISTS olist_lakehouse
COMMENT 'End-to-end lakehouse project using the Olist Brazilian E-commerce dataset';

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS olist_lakehouse.raw
COMMENT 'Raw layer and landing area for source files';

CREATE SCHEMA IF NOT EXISTS olist_lakehouse.bronze
COMMENT 'Bronze layer: raw Delta tables ingested from CSV files';

CREATE SCHEMA IF NOT EXISTS olist_lakehouse.silver
COMMENT 'Silver layer: cleaned, typed and standardized Olist entities';

CREATE SCHEMA IF NOT EXISTS olist_lakehouse.gold
COMMENT 'Gold layer: analytical marts and business metrics';

CREATE SCHEMA IF NOT EXISTS olist_lakehouse.quarantine
COMMENT 'Rejected records isolated during data quality and Silver curation rules';

-- COMMAND ----------

CREATE VOLUME IF NOT EXISTS olist_lakehouse.raw.landing
COMMENT 'Landing volume for raw Olist CSV files';
