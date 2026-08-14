from pyspark.sql.functions import col, to_date, lower

df_bronze = spark.read.parquet(
    "abfss://retail@storagesection2.dfs.core.windows.net/bronze/ukumar803/Data-Engineering-Projects/refs/heads/main/Azure_Data-Engineer_Project_2/"
)

display(df_bronze)

df_silver = (
    df_bronze
    .filter(col("event_type") == "purchase")
    .dropna(subset=["customer_id", "amount"])
    .withColumn("event_date", to_date(col("event_timestamp")))
    .withColumn("payment_method", lower(col("payment_method")))
    .withColumn("amount", col("amount").cast("float"))
    .select(
        "event_id",
        "customer_id",
        "event_date",
        "product_id",
        "product_category",
        "payment_method",
        "amount",
        "location"
    )
)

df_silver.write.mode("overwrite").parquet(
    "abfss://retail@storagesection2.dfs.core.windows.net/silver/"
)


from pyspark.sql.functions import sum as spark_sum, count

df_daily_revenue = (
    df_silver
    .groupBy("event_date")
    .agg(
        spark_sum("amount").alias("daily_revenue"),
        count("*").alias("total_purchases")
    )
)

df_daily_revenue.write.mode("overwrite").parquet(
    "abfss://retail@storagesection2.dfs.core.windows.net/gold/"
)

display(df_daily_revenue)


# ============================================================
# SQL CODE FROM THE PDF
# Azure Synapse Serverless SQL
# ============================================================

-- CREATE DATABASE RetailDB;

-- Step 1 — Create an external data source
CREATE EXTERNAL DATA SOURCE RetailData
WITH (
    LOCATION = 'https://storagesection2.dfs.core.windows.net/retail'
);

-- Step 2 — Create the external file format
CREATE EXTERNAL FILE FORMAT ParquetFormat
WITH (
    FORMAT_TYPE = PARQUET
);

-- Step 3 — Create your Gold external table
CREATE EXTERNAL TABLE dbo.daily_revenue
(
    event_date DATE,
    daily_revenue FLOAT,
    total_purchases INT
)
WITH
(
    LOCATION = 'gold/',
    DATA_SOURCE = RetailData,
    FILE_FORMAT = ParquetFormat
);
