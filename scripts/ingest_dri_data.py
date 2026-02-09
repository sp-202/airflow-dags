import os
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, abs as spark_abs

# --- 1. SESSION INITIALIZATION ---
run_id = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
unique_app_name = f"dri-daily-data-{run_id}"

spark = SparkSession.builder \
    .appName(unique_app_name) \
    .getOrCreate()

# --- 2. CONFIGURATION ---
# Source: The raw data dump you just created
source_table = "nav_raw_data.raw_data"

# Target: The DRI specific output table
target_s3_path = "s3a://dri-data/bronze/dri_output"
target_table = "dri_db.dri_prod"

print(f"Reading source data from Delta Table: {source_table}")

# --- 3. DATA INGESTION (FROM DELTA) ---
# Instead of JDBC, we read directly from your S3 Bronze layer
raw_ledger_df = spark.table(source_table)

# --- 4. TRANSFORMATION LOGIC ---

# 1. Filter for DRI Department first (highly efficient on Delta)
# 2. Normalize Quantity to Metric Tons
# 3. Create Product Groups based on Item Descriptions
dri_output = raw_ledger_df.filter(
        col("item_no").isin(['FGDRIGRDG1104', 'FGDRIFNDF1101']) 
    )
    .filter(col("entry_type_desc").isin("Output", "Sale"))
    .withColumn(
        "quantity_mt",
        col("quantity") / 1000 
    )
    .withColumn(
        "product_name",
        when(
            col("item_description").isin(
                "DRI Lump(A-Grade- Fe(M)-80%)", 
                "DRI Lump(B-Grade-<80%)", 
                "Iron-ore Lumps"
            ), 
            "DRI Lumps"
        )
        .when(col("item_description") == "Iron-Ore Fines", "DRI Fines")
        .otherwise(col("item_description"))
)

# --- 5. PRODUCTION WRITE ---
print(f"Saving processed DRI data to: {target_s3_path}")

# Ensure the DRI database exists
spark.sql("CREATE SCHEMA IF NOT EXISTS dri_db")

(
    dri_output.write
    .format("delta")
    .mode("overwrite")
    .option("path", target_s3_path)
    .option("overwriteSchema", "true") 
    .option("mergeSchema", "true")
    .saveAsTable(target_table)
)

print("DRI Ingestion Job Completed Successfully.")