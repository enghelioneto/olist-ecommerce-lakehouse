# Olist E-commerce Lakehouse

End-to-end data project built with Databricks Free Edition, PySpark, SQL, Delta Lake and Medallion Architecture.

## Objective

Build a lakehouse pipeline using the Olist Brazilian E-commerce dataset, transforming raw CSV files into analytical gold tables and dashboards.

## Project Plan

The step-by-step learning and implementation roadmap is documented in [docs/project_plan.md](docs/project_plan.md).

## Architecture

CSV -> Volume -> Bronze -> Silver -> Gold -> SQL Analytics -> Dashboard

## Tech Stack

- Databricks Free Edition
- PySpark
- SQL
- Delta Lake
- Unity Catalog
- Medallion Architecture
- Databricks AI/BI Dashboards

## Dataset

Brazilian E-Commerce Public Dataset by Olist from Kaggle.

## Layers

### Bronze

Raw ingestion from CSV to Delta tables with technical metadata.

### Silver

Cleaned, typed and standardized entities.

### Gold

Analytical marts for sales, customers, products, delivery and reviews.

## Business Questions

- What is the monthly revenue?
- Which states generate more sales?
- What is the average delivery time?
- Which product categories perform better?
- How do delivery delays impact customer reviews?

## Data Quality

- Deduplication
- Type casting
- Null checks
- Referential integrity checks
- Business rule validation

## Dashboard

Executive dashboard built in Databricks AI/BI Dashboards.

## How to Run

1. Download the dataset from Kaggle.
2. Upload CSV files to Databricks Volume.
3. Run setup notebook.
4. Run Bronze ingestion.
5. Run Silver transformations.
6. Run Gold modeling.
7. Run analytical queries.
8. Create dashboard.
