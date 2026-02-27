import os
import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, when

# --- 1. SESSION INITIALIZATION ---
# App name includes timestamp for unique identification in Spark UI
run_id = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
spark = SparkSession.builder \
    .appName(f"incremental-lab-report-{run_id}") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# --- 2. CONFIGURATION ---
# Credentials are automatically loaded from envFrom secretRef in yaml
db_host = os.getenv("DB_HOST")
db_user = os.getenv("DB_USER")
db_pass = os.getenv("DB_PASS")
db_name = os.getenv("DB_NAME")
jdbc_url = f"jdbc:sqlserver://{db_host}:1433;databaseName={db_name};encrypt=true;trustServerCertificate=true"

# Target table in HMS (Hive Metastore)
target_table = "nav_raw_data.lab_report_data"
# Source table name in MS SQL
source_sql_table = "[dbo].[MIS Lab Report]" 

# --- 3. GET WATERMARK (Max SL. No.) ---
watermark = 0
try:
    # Safely check if the table exists in the Spark Catalog
    if spark.catalog.tableExists(target_table):
        # Use COALESCE to handle the case where the table exists but is empty
        result = spark.sql(f"SELECT COALESCE(MAX(sl_no), 0) FROM {target_table}").collect()
        watermark = result[0][0]
    else:
        print(f"Target table {target_table} does not exist yet. Starting full load.")
except Exception as e:
    print(f"Could not fetch watermark due to: {e}. Defaulting to 0.")
    watermark = 0

print(f"Incremental load starting from SL_No: {watermark}")

# --- 4. FETCH INCREMENTAL DATA FROM MSSQL ---
# Query uses the watermark to pull only new rows
incremental_query = f"""
(
    SELECT * FROM {source_sql_table}
    WHERE [SL. No.] > {watermark}
) AS subquery
"""

df_raw = spark.read.format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", incremental_query) \
    .option("user", db_user) \
    .option("password", db_pass) \
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
    .load()

# --- 5. DATA CLEANING & TRANSFORMATION ---
if df_raw.count() > 0:
    # 1. Clean column names: Lowercase, replace ' ' and '.' with '_'
    # 2. Handle the "Mining" department logic
    
    # Generate clean names mapping
    clean_cols = [c.lower().strip().replace(" ", "_").replace(".", "") for c in df_raw.columns]
    
    df_cleaned = df_raw.toDF(*clean_cols) \
        .withColumn("department", 
            when(col("department").contains("Mines PIT"), "Mining")
            .otherwise(col("department"))
        ) \
        .withColumn("ingestion_timestamp", F.current_timestamp())

    # --- 6. MERGE INTO DELTA TABLE ---
    # Fix for [UNABLE_TO_INFER_SCHEMA]: Explicitly create table using DataFrame API if it does not exist
    table_exists = spark.catalog.tableExists(target_table)
    
    if not table_exists:
        print(f"Creating Delta table {target_table} for the first time...")
        # Write empty DataFrame to initialize schema and location
        df_cleaned.write \
            .format("delta") \
            .mode("ignore") \
            .saveAsTable(target_table)

    # Now perform the merge
    df_cleaned.createOrReplaceTempView("batch_updates")
    
    # Merge on sl_no (our incremental primary key)
    spark.sql(f"""
        MERGE INTO {target_table} AS target
        USING batch_updates AS source
        ON target.sl_no = source.sl_no
        WHEN MATCHED THEN
            UPDATE SET *
        WHEN NOT MATCHED THEN
            INSERT *
    """)
    
    print(f"Successfully Upserted {df_cleaned.count()} records.")
else:
    print("No new lab report data found in MS SQL Server.")

spark.stop()