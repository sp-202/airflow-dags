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

# Define the table name and the specific S3 storage path
target_table = "nav_raw_data.lab_report_data"
s3_delta_path = "s3a://nav-data/bronze/lab_mis_data"

# Source table name in MS SQL
source_sql_table = "[dbo].[ANRML$MIS Lab Report]" 

# --- 3. GET WATERMARK (Max SL. No.) ---
watermark = 0
try:
    if spark.catalog.tableExists(target_table):
        # We need the highest 'sl_no' currently in the Delta table
        result = spark.sql(f"SELECT COALESCE(MAX(sl_no), 0) FROM {target_table}").collect()
        watermark = result[0][0]
    else:
        print(f"Target table {target_table} does not exist yet. Starting full load.")
except Exception as e:
    print(f"Could not fetch watermark due to: {e}. Defaulting to 0.")
    watermark = 0

print(f"Incremental load starting from SL_No: {watermark}")

# --- 4. FETCH INCREMENTAL DATA FROM MSSQL ---
# NOTE: The WHERE clause must match the exact column name in MS SQL
incremental_query = f"""
(
    SELECT * FROM {source_sql_table}
    WHERE [Sl_ No_] > {watermark}
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
    # UPDATED CLEANING LOGIC TO FIX sl__no_ issue:
    
    # 1. Lowercase all
    df_cleaned = df_raw.toDF(*[c.lower() for c in df_raw.columns])
    
    # 2. Specifically rename 'sl_ no_' to 'sl_no' to match merge key
    df_cleaned = df_cleaned.withColumnRenamed("sl_ no_", "sl_no")
    
    # 3. Clean remaining columns (replace spaces/dots with underscores)
    new_cols = [c.replace(" ", "_").replace(".", "") for c in df_cleaned.columns]
    df_cleaned = df_cleaned.toDF(*new_cols)
    
    # 4. Handle transformations
    df_cleaned = df_cleaned \
        .withColumn("department", 
            when(col("department").contains("Mines PIT"), "Mining")
            .otherwise(col("department"))
        ) \
        .withColumn("ingestion_timestamp", F.current_timestamp())

    # --- 6. MERGE INTO DELTA TABLE ---
    table_exists = spark.catalog.tableExists(target_table)
    
    if not table_exists:
        print(f"Creating Delta table {target_table} at {s3_delta_path}...")
        # Write DataFrame to initialize schema and specific S3 location
        df_cleaned.write \
            .format("delta") \
            .mode("ignore") \
            .option("path", s3_delta_path) \
            .saveAsTable(target_table)

    # Perform the merge
    df_cleaned.createOrReplaceTempView("batch_updates")
    
    # Merge on 'sl_no'
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