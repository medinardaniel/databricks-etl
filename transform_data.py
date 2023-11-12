# Databricks notebook source
# Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import LongType  # Import LongType

# Read the data saved from the extraction step
raw_batch_data = spark.read.table("temp_raw_batch_data")

# Apply transformations
transformed_batch_data = raw_batch_data.withColumn("processing_time", current_timestamp())

# Cast the 'time' column to LongType to match the Delta table schema
transformed_batch_data = transformed_batch_data.withColumn("time", col("time").cast(LongType()))

# Store the transformed data temporarily
transformed_batch_data.write.format("delta").mode("overwrite").saveAsTable("temp_transformed_batch_data")


# COMMAND ----------


