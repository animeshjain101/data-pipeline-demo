# Gold Layer - Business Aggregations
# This will be a Databricks notebook

from pyspark.sql.functions import sum, count, avg, date_format

# Read from silver
df_silver = spark.table("enterprise.sales.sales_cleaned")

# Daily sales summary
daily_sales = df_silver.groupBy(
    date_format("transaction_date", "yyyy-MM-dd").alias("date"),
    "product"
).agg(
    sum("total_amount").alias("total_revenue"),
    count("transaction_id").alias("num_transactions"),
    avg("total_amount").alias("avg_transaction_value")
)

# Write to gold table
daily_sales.write.mode("overwrite").saveAsTable("gold.daily_sales_summary")

# Product performance
product_performance = df_silver.groupBy("product").agg(
    sum("total_amount").alias("total_revenue"),
    sum("quantity").alias("units_sold"),
    avg("price").alias("avg_price")
).orderBy("total_revenue", ascending=False)

product_performance.write.mode("overwrite").saveAsTable("enterprise.sales.product_performance")

print("Gold layer aggregations completed")