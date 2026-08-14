# Retail Transaction Data Engineering Pipeline — Azure Synapse Analytics

## Overview

This project demonstrates an end-to-end Azure data engineering pipeline for retail transaction data using **Azure Synapse Analytics** and **Azure Data Lake Storage Gen2 (ADLS Gen2)**.

The pipeline follows a **Medallion Architecture**:

**HTTP/JSON Source → Bronze → Silver → Gold → Serverless SQL**

## Architecture

```text
GitHub-hosted JSON
       |
       | HTTP / Copy Data
       v
Azure Synapse Pipeline
       |
       v
ADLS Gen2 - Bronze
(Raw Parquet)
       |
       | PySpark / Spark Pool
       v
ADLS Gen2 - Silver
(Cleaned Parquet)
       |
       | Aggregation
       v
ADLS Gen2 - Gold
(Daily Revenue + Purchases)
       |
       | Serverless SQL
       v
SQL External Table
```

## Technologies Used

- Azure Synapse Analytics
- Azure Data Lake Storage Gen2 (ADLS Gen2)
- Synapse Pipelines
- Copy Data Activity
- HTTP / JSON ingestion
- Apache Spark / Synapse Spark Pool
- PySpark
- Parquet
- Serverless SQL
- Medallion Architecture
- ETL / ELT
- SQL

## Pipeline Steps

### 1. Bronze Layer — Data Ingestion

Retail transaction JSON data is sourced from a GitHub-hosted raw file.

A Synapse Pipeline uses the **Copy Data** activity with an HTTP source and stores the raw data in ADLS Gen2 Bronze storage.

### 2. Silver Layer — Data Cleaning

PySpark is used to transform the Bronze data:

- Filter only `purchase` events
- Remove records with missing `customer_id` or `amount`
- Convert event timestamps into dates
- Standardize payment methods to lowercase
- Cast transaction amounts to numeric type
- Select the required analytical columns
- Write the cleaned dataset to the Silver layer as Parquet

### 3. Gold Layer — Business Aggregation

The Silver dataset is aggregated by `event_date` to calculate:

- `daily_revenue`
- `total_purchases`

The resulting Gold dataset is stored as Parquet in ADLS Gen2.

### 4. Serverless SQL Layer

A Serverless SQL external data source and Parquet file format are configured in Synapse.

The Gold Parquet data is exposed through an external SQL table:

`dbo.daily_revenue`

This makes the curated data available for SQL-based analysis and stakeholder consumption.

## Key Learning Outcomes

- Built an end-to-end cloud data pipeline
- Implemented Bronze, Silver and Gold layers
- Performed API/HTTP-based ingestion
- Used PySpark for scalable data transformation
- Worked with Parquet files in ADLS Gen2
- Created analytical Gold-layer aggregations
- Queried data using Synapse Serverless SQL
- Understood the separation of ingestion, transformation and serving layers

## Future Enhancement

The next step is to automate the complete workflow on a scheduled basis so that:

**Source API → Bronze → Silver → Gold → SQL**

runs automatically whenever new data becomes available.

