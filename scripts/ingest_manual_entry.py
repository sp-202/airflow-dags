import os
import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# --- 1. SESSION INITIALIZATION ---
run_id = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
spark = SparkSession.builder \
    .appName(f"incremental-manual-entry-nav-{run_id}") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# --- 2. CONFIGURATION ---
db_host = os.getenv("DB_HOST")
db_user = os.getenv("DB_USER")
db_pass = os.getenv("DB_PASS")
jdbc_url = f"jdbc:sqlserver://{db_host}:1433;databaseName={os.getenv('DB_NAME')};encrypt=true;trustServerCertificate=true"

target_table = "nav_raw_data.manual_data"
s3_delta_path = "s3a://nav-manual-data/bronze/manual-data"

# --- 3. GET WATERMARK (Max Hex Timestamp) ---
try:
    # We fetch the highest hex_timestamp we have already stored
    last_hex = spark.sql(f"SELECT MAX(hex_timestamp) FROM {target_table}").collect()[0][0]
    # If table is empty, start from the lowest possible value
    watermark = f"0x{last_hex}" if last_hex else "0x0000000000000000"
except Exception:
    watermark = "0x0000000000000000"

print(f"Incremental load starting from Watermark: {watermark}")

# --- 4. FETCH INCREMENTAL DATA FROM MSSQL ---
# We use the 'timestamp' column in MSSQL for the filter
incremental_query = f"""
(
    SELECT *, CAST(timestamp AS VARBINARY(8)) as bin_ts
    FROM [dbo].[ANRML$Daily Production Details]
    WHERE [timestamp] > {watermark}
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
    # 1. Generate Hex String from Binary
    # 2. Rename columns: Replace spaces with underscores
    # 3. Drop the original binary timestamp
    df_cleaned = (
                df_raw
                .withColumn("hex_timestamp", F.hex(F.col("bin_ts")))
                # 1. Clean names (e.g., 'Department ' becomes 'Department')
                .toDF(*[c.replace(" ", "_") for c in df_raw.columns]) 
                .drop("timestamp", "bin_ts")
                .withColumn("department_name",
                    when(col("Department") == 1, "Beneficiation") # Fixed spelling: Beneficiation
                    .when(col("Department") == 2, "Pellet")
                    .when(col("Department") == 3, "DRI")
                    .when(col("Department") == 4, "Power")
                    .otherwise("Other") # Good practice to handle unknowns
                )
    )
    
    # --- 6. MERGE INTO DELTA TABLE ---
    # Register as temp view for SQL Merge
    df_cleaned.createOrReplaceTempView("batch_updates")
    
    # Ensure target table exists first (First run logic)
    # Using Entry_No as the Primary Key for matching
    spark.sql(f"""
        MERGE INTO {target_table} AS target
        USING batch_updates AS source
        ON target.Entry_No = source.Entry_No
        WHEN MATCHED THEN
            UPDATE SET *
        WHEN NOT MATCHED THEN
            INSERT *
    """)
    
    print(f"Successfully Upserted {df_cleaned.count()} records.")
else:
    print("No new updates found in SQL Server.")

spark.stop()