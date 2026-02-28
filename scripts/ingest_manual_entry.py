import os
import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# --- 1. SESSION INITIALIZATION ---
run_id = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
spark = SparkSession.builder \
    .appName(f"incremental-manual-entry-nav-{run_id}") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.databricks.delta.schema.autoMerge.enabled", "true") \
    .getOrCreate()

# --- 2. CONFIGURATION ---
db_host = os.getenv("DB_HOST")
db_user = os.getenv("DB_USER")
db_pass = os.getenv("DB_PASS")
db_name = os.getenv("DB_NAME")
jdbc_url = f"jdbc:sqlserver://{db_host}:1433;databaseName={db_name};encrypt=true;trustServerCertificate=true"

target_table = "nav_raw_data.manual_data"
s3_delta_path = "s3a://nav-manual-data/bronze/manual-data"

# --- 3. GET WATERMARK (Max Hex Timestamp) ---
try:
    last_hex = spark.sql(f"SELECT MAX(hex_timestamp) FROM {target_table}").collect()[0][0]
    watermark = f"0x{last_hex}" if last_hex else "0x0000000000000000"
except Exception as e:
    print(f"Table not found or empty, starting from zero. Error: {e}")
    watermark = "0x0000000000000000"

print(f"Incremental load starting from Watermark: {watermark}")

# --- 4. FETCH INCREMENTAL DATA FROM MSSQL ---
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

# --- 5. DATA CLEANING, TRANSFORM & DEDUPLICATION ---
if df_raw.count() > 0:
    # STEP 1: Clean column names (Fixes Arity Mismatch)
    clean_column_names = [c.replace(" ", "_") for c in df_raw.columns]
    df_cleaned_names = df_raw.toDF(*clean_column_names)

    # STEP 2: Transformations
    df_transformed = (
        df_cleaned_names
        .withColumn("hex_timestamp", F.hex(F.col("bin_ts")))
        .withColumn("department_name",
            F.when(F.col("Department") == 1, "Beneficiation")
            .when(F.col("Department") == 2, "Pellet")
            .when(F.col("Department") == 3, "DRI")
            .when(F.col("Department") == 4, "Power")
            .otherwise("Other")
        )
    )

    # STEP 3: DEDUPLICATE THE BATCH
    # If Entry_No appears twice in the same pull, take the one with the latest timestamp
    window_spec = Window.partitionBy("Entry_No").orderBy(F.col("hex_timestamp").desc())
    
    df_final = (
        df_transformed
        .withColumn("row_rank", F.row_number().over(window_spec))
        .filter(F.col("row_rank") == 1)
        .drop("row_rank", "timestamp", "bin_ts")
    )

    # --- 6. MERGE INTO DELTA TABLE ---
    df_final.createOrReplaceTempView("batch_updates")
    
    table_exists = spark._jsparkSession.catalog().tableExists(target_table)
    
    if not table_exists:
        print(f"Initial Load: Creating table {target_table}")
        df_final.write.format("delta").mode("overwrite").option("path", s3_delta_path).saveAsTable(target_table)
    else:
        # MERGE logic using a safer match
        # Using Entry_No + Posting_Date if Entry_No repeats across dates
        print(f"Merging {df_final.count()} deduplicated records...")
        spark.sql(f"""
            MERGE INTO {target_table} AS target
            USING batch_updates AS source
            ON target.Entry_No = source.Entry_No
            AND target.Date = source.Date
            WHEN MATCHED THEN
                UPDATE SET *
            WHEN NOT MATCHED THEN
                INSERT *
        """)
    
    print("ETL Job Completed Successfully.")
else:
    print("No new updates found.")

spark.stop()