# Databricks notebook source
from pyspark.sql import functions as F

# Read Bronze
bronze_df = spark.read.format("delta").load(
    "/Volumes/bromze/default/kaajal/bronze_global_superstore"
)

# Remove duplicates
silver_df = bronze_df.dropDuplicates()

# Trim string columns
for col_name, data_type in silver_df.dtypes:

    if data_type == "string":
        silver_df = silver_df.withColumn(
            col_name,
            F.trim(F.col(col_name))
        )

# Convert dates
silver_df = silver_df.withColumn(
    "Order_Date",
    F.to_date("Order_Date")
)

silver_df = silver_df.withColumn(
    "Ship_Date",
    F.to_date("Ship_Date")
)

# Remove records with missing important values
silver_df = silver_df.dropna(
    subset=["Order_ID", "Customer_ID", "Product_ID"]
)

# Save Silver
silver_path = "/Volumes/bromze/default/kaajal/silver_global_superstore"

silver_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save(silver_path)

print("Silver layer created successfully!")

silver_df.show(10)
silver_df.printSchema()