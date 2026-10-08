# Databricks notebook source
from pyspark.sql import functions as F

# CSV path
file_path = "/Volumes/bromze/default/kaajal/Global_Superstore2.csv"

# Read raw CSV
bronze_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(file_path)
)

# Check data
bronze_df.show(5)
bronze_df.printSchema()

# COMMAND ----------

bronze_path = "/Volumes/bromze/default/kaajal/bronze_global_superstore"

bronze_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save(bronze_path)

# COMMAND ----------

from pyspark.sql import functions as F

# CSV path
file_path = "/Volumes/bromze/default/kaajal/Global_Superstore2.csv"

# Read CSV
bronze_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(file_path)
)

# Show original columns
print("Original Columns:")
print(bronze_df.columns)


# Clean column names
for col_name in bronze_df.columns:

    new_name = (
        col_name
        .strip()
        .replace(" ", "_")
        .replace(",", "_")
        .replace(";", "_")
        .replace("{", "_")
        .replace("}", "_")
        .replace("(", "_")
        .replace(")", "_")
        .replace("\n", "_")
        .replace("\t", "_")
        .replace("=", "_")
    )

    bronze_df = bronze_df.withColumnRenamed(
        col_name,
        new_name
    )


# Check cleaned columns
print("Cleaned Columns:")
print(bronze_df.columns)


# Save Bronze as Delta
bronze_path = "/Volumes/bromze/default/kaajal/bronze_global_superstore"

bronze_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save(bronze_path)

print("Bronze layer created successfully!")