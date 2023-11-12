# Databricks notebook source
# Load variables from previous steps
username = spark.sql("SELECT regexp_replace(current_user(), '[^a-zA-Z0-9]', '_')").first()[0]
final_table_name = f"{username}_etl_quickstart_delta"

# Read the transformed data from the transformation step
transformed_batch_data = spark.read.table("temp_transformed_batch_data")

# Load process into final Delta table with schema overwrite
transformed_batch_data.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(final_table_name)

# COMMAND ----------


