# Azure Synapse Analytics - Retail Transaction Data Engineering Pipeline

from pyspark.sql import functions as F

# 1. Read Bronze data
bronze_path = (
    "abfss://retail@storagesection2.dfs.core.windows.net/"
    "bronze/ukumar803/Data-Engineering-Projects/"
    "refs/heads/main/Azure_Data-Engineer_Project_2/"
    "retail_transactions_bronze.parquet"
)

df_bronze = spark.read.parquet(bronze_path)
display(df_bronze)

# 2. Silver Layer - clean and transform data
df_silver = (
    df_bronze
    .filter(F.col("event_type") == "purchase")
    .dropna(subset=["customer_id", "amount"])
    .withColumn("event_date", F.to_date(F.col("event_timestamp")))
    .withColumn("payment_method", F.lower(F.col("payment_method")))
    .withColumn("amount", F.col("amount").cast("float"))
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

display(df_silver)

# 3. Write Silver data to ADLS Gen2
silver_path = (
    "abfss://retail@storagesection2.dfs.core.windows.net/silver/"
)

df_silver.write.mode("overwrite").parquet(silver_path)

# 4. Gold Layer - daily revenue and purchase count
df_daily_revenue = (
    df_silver
    .groupBy("event_date")
    .agg(
        F.sum("amount").alias("daily_revenue"),
        F.count("*").alias("total_purchases")
    )
    .orderBy("event_date")
)

display(df_daily_revenue)

# 5. Write Gold data to ADLS Gen2
gold_path = (
    "abfss://retail@storagesection2.dfs.core.windows.net/gold/"
)

df_daily_revenue.write.mode("overwrite").parquet(gold_path)

# 6. Validation
print("Bronze record count:", df_bronze.count())
print("Silver record count:", df_silver.count())
print("Gold record count:", df_daily_revenue.count())

df_daily_revenue.show(20, truncate=False)

# 7. Serverless SQL - run separately in Synapse Serverless SQL
#
# CREATE DATABASE RetailDB;
#
# CREATE EXTERNAL DATA SOURCE RetailData
# WITH (
#     LOCATION = 'https://storagesection2.dfs.core.windows.net/retail'
# );
#
# CREATE EXTERNAL FILE FORMAT ParquetFormat
# WITH (
#     FORMAT_TYPE = PARQUET
# );
#
# CREATE EXTERNAL TABLE dbo.daily_revenue
# (
#     event_date DATE,
#     daily_revenue FLOAT,
#     total_purchases INT
# )
# WITH
# (
#     LOCATION = 'gold/',
#     DATA_SOURCE = RetailData,
#     FILE_FORMAT = ParquetFormat
# );

# Future automation:
# API/JSON -> Bronze -> Silver -> Gold -> Serverless SQL
