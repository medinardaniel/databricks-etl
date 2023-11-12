# Databricks notebook source
# Import necessary libraries
from pyspark.sql import SparkSession

# Define variables for extraction
file_path = "/databricks-datasets/structured-streaming/events"  # Adjust as per your data source

# Extraction process for batch data
batch_data = spark.read.format("json").load(file_path)

# Store the raw batch data temporarily
batch_data.write.format("delta").mode("overwrite").saveAsTable("temp_raw_batch_data")


# COMMAND ----------


