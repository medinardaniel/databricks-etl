# Databricks notebook source
# MAGIC %md
# MAGIC Print Table Schema

# COMMAND ----------

# Load variables from previous steps
username = spark.sql("SELECT regexp_replace(current_user(), '[^a-zA-Z0-9]', '_')").first()[0]
table_name = f"{username}_etl_quickstart_delta"

# Read the data from the Delta table
data = spark.read.table(table_name)

# Print the schema of the table to find column names
data.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Visualize Data (Column: 'Action')

# COMMAND ----------

# MAGIC %pip install matplotlib
# MAGIC # Import necessary libraries
# MAGIC import matplotlib.pyplot as plt
# MAGIC from pyspark.sql import SparkSession
# MAGIC
# MAGIC # Initialize Spark session
# MAGIC spark = SparkSession.builder.appName("DataVisualization").getOrCreate()
# MAGIC
# MAGIC # Load variables from previous steps
# MAGIC username = spark.sql("SELECT regexp_replace(current_user(), '[^a-zA-Z0-9]', '_')").first()[0]
# MAGIC table_name = f"{username}_etl_quickstart_delta"
# MAGIC
# MAGIC # Read the data from the Delta table
# MAGIC data = spark.read.table(table_name)
# MAGIC
# MAGIC # Perform any necessary transformations or aggregations for visualization
# MAGIC # Example: Counting the number of records by a specific column
# MAGIC data_grouped = data.groupBy("action").count()
# MAGIC
# MAGIC # Convert to Pandas DataFrame for visualization
# MAGIC data_pandas = data_grouped.toPandas()
# MAGIC
# MAGIC # Plotting
# MAGIC plt.figure(figsize=(10, 6))
# MAGIC plt.bar(data_pandas["action"], data_pandas["count"])
# MAGIC plt.xlabel("Action")
# MAGIC plt.ylabel("Count")
# MAGIC plt.title("Visualization of Data")
# MAGIC plt.show()

# COMMAND ----------


