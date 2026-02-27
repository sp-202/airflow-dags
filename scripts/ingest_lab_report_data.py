import os
import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, when

# --- 1. SESSION INITIALIZATION ---
run_id = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
spark = SparkSession.builder \
    .appName(f"incremental-lab-report-{run_id}") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# --- 2. CONFIGURATION ---
db_host = os.getenv("DB_HOST")
db_user = os.getenv("DB_USER")
db_pass = os.getenv("DB_PASS")
db_name = os.getenv("DB_NAME")
jdbc_url = f"jdbc:sqlserver://{db_host}:1433;databaseName={db_name};encrypt=true;trustServerCertificate=true"

target_table = "nav_raw_data.lab_report_data"
source_sql_table = "[dbo].[MIS Lab Report]" 

# REMOVED the broken CREATE TABLE line here. 
# We handle creation in Step 6.

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
    # Rename columns to small_letters_with_underscores
    clean_cols = [c.lower().strip().replace(" ", "_").replace(".", "") for c in df_raw.columns]
    
    df_cleaned = df_raw.toDF(*clean_cols) \
        .withColumn("department", 
            when(col("department").contains("Mines PIT"), "Mining")
            .otherwise(col("department"))
        ) \
        .withColumn("ingestion_timestamp", F.current_timestamp())

    # --- 6. MERGE INTO DELTA TABLE ---
    df_cleaned.createOrReplaceTempView("batch_updates")
    
    # This block handles the "Table Not Exists" issue properly
    # It uses the schema derived from df_cleaned
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {target_table}
        USING DELTA
        AS SELECT * FROM batch_updates WHERE 1=0
    """)

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