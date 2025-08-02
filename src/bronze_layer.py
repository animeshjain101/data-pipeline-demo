# Bronze Layer - Raw Data Ingestion
# This will be a Databricks notebook

# Read raw CSV files
df = spark.read.csv("/Volumes/omnidata/bucket/data/data_pipeline_demo/raw/*.csv", header=True, inferSchema=True)

# Add ingestion metadata
from pyspark.sql.functions import current_timestamp, input_file_name

df_bronze = df.withColumn("ingestion_timestamp", current_timestamp()) \
              .withColumn("source_file", input_file_name())

# Write to bronze table
df_bronze.write.mode("append").saveAsTable("enterprise.sales.raw_sales")

print(f"Ingested {df_bronze.count()} records to bronze layer")