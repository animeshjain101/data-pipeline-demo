# Silver Layer - Clean and Transform
# This will be a Databricks notebook

from pyspark.sql.functions import col, to_date

# Read from bronze
df_bronze = spark.table("enterprise.sales.raw_sales")

# Clean and transform
df_silver = df_bronze.filter(col("price") > 0) \
    .filter(col("quantity") > 0) \
    .withColumn("transaction_date", to_date(col("transaction_date"))) \
    .withColumn("total_amount", col("price") * col("quantity")) \
    .dropDuplicates(["transaction_id"])

# Write to silver table
df_silver.write.mode("overwrite").saveAsTable("enterprise.sales.sales_cleaned")

print(f"Processed {df_silver.count()} records to silver layer")