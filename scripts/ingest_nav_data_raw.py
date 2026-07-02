import os
import datetime
import uuid
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, broadcast, when, abs as spark_abs

# --- 1. SAFE CREDENTIAL LOADING ---
def get_env_var(name, default=None):
    value = os.getenv(name, default)
    if value is None:
        # Log which one is missing for easier debugging in Airflow logs
        print(f"CRITICAL: Environment variable {name} is MISSING!")
        return "" # Return empty string to prevent NullPointerException in Java
    return value

db_host = get_env_var("DB_HOST")
db_port = get_env_var("DB_PORT", "1433")
db_name = get_env_var("DB_NAME")
db_user = get_env_var("DB_USER")
db_pass = get_env_var("DB_PASS")

# --- 2. JDBC CONFIGURATION ---
jdbc_url = (
    f"jdbc:sqlserver://{db_host}:{db_port};"
    f"databaseName={db_name};"
    "encrypt=true;"
    "trustServerCertificate=true"
)

jdbc_props = {
    "user": db_user,
    "password": db_pass,
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    "fetchsize": "10000"
}

# Create a unique suffix (Timestamp + short random ID)
run_id = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
unique_app_name = f"nav-raw-data-{run_id}"

# --- 3. SESSION & LOGIC ---
spark = SparkSession.builder \
    .appName(unique_app_name) \
    .getOrCreate()

# SQL Server stores timestamps in UTC, but business operates in WAT (UTC+1).
# Delta will store WAT, consistent with the logistics TAT pipeline.
WAT_TZ = "Africa/Lagos"
# spark.conf.set("spark.sql.session.timeZone", WAT_TZ)

# Date/time columns coming out of the ledger query that need UTC -> WAT conversion.
# (These are the OUTPUT/aliased column names, i.e. post-SELECT.)
TIMESTAMP_COLS = [
    "posting_date",
    "document_date",
    "expiration_date",
]

# Debug: Print the keys found (NEVER print the password)
print(f"Connecting to {db_host} as user: {db_user} on database: {db_name}")

s3_delta_path = "s3a://nav-data/bronze/raw_nav_data"
table_name = "nav_raw_data.raw_data"

# --- SCHEMA + TABLE-EXISTENCE CHECK ---
# Data was deleted from S3, so this run needs to safely rebuild from scratch.
# MERGE INTO a table that doesn't exist will error, so we branch on table_exists
# below and do a full overwrite load instead of a merge on the first run.
spark.sql("CREATE SCHEMA IF NOT EXISTS nav_raw_data")

table_exists = False
try:
    # spark.read.table() is LAZY — it will happily return a DataFrame even for
    # a stale metastore entry pointing at an empty/deleted Delta directory.
    # Force an action (.take(1)) so DELTA_TABLE_NOT_FOUND / empty-log errors
    # surface HERE instead of later inside the MERGE statement.
    spark.read.table(table_name).take(1)
    table_exists = True
    print(f"✅ Target table {table_name} found and readable — will run incremental MERGE.")
except Exception as e:
    table_exists = False
    print(f"⚠️  Target table {table_name} not found/readable ({type(e).__name__}) — will run full initial load.")

# 1. Get the Watermark
# If the table doesn't exist (or is otherwise unreadable, e.g. dangling metadata
# pointing at deleted S3 files), fall back to a full load from entry_no > 0.
if table_exists:
    try:
        last_entry_no = spark.sql(f"SELECT MAX(entry_no) FROM {table_name}").collect()[0][0] or 0
    except Exception as e:
        print(f"⚠️  Could not read watermark from existing table ({e}). Defaulting to full load.")
        last_entry_no = 0
        table_exists = False  # treat as first load if metadata is broken
else:
    last_entry_no = 0

print(f"Watermark (last_entry_no): {last_entry_no}")

# 2. Fetch Incremental Ledger Data (Fact)
incremental_ledger_query = f"""
(
    SELECT 
        [Entry No_]                AS entry_no,
        [Item No_]                 AS item_no,
        [Posting Date]             AS posting_date,
        [Entry Type]               AS entry_type,
        [Source No_]               AS source_no,
        [Document No_]             AS document_no,
        [Description]              AS description,
        [Location Code]            AS location_code,
        [Quantity]                 AS quantity,
        [Remaining Quantity]       AS remaining_quantity,
        [Invoiced Quantity]        AS invoiced_quantity,
        [Global Dimension 1 Code]  AS department,
        [Global Dimension 2 Code]  AS process_center_code,
        [Document Date]            AS document_date,
        [External Document No_]    AS external_doc_no,
        [Document Type]            AS document_type,
        [Document Line No_]        AS document_line_no,
        [Dimension Set ID]         AS dimension_set_id,
        [Qty_ per Unit of Measure] AS qty_per_uom,
        [Unit of Measure Code]     AS uom_code,
        [Purpose]                  AS purpose,
        [Supplier Name]            AS supplier_name,
        [Gen Prod_ Posting Group]  AS gen_prod_posting_group,
        [Vehicle No_]              AS vehicle_no,
        [Project Approval Name]    AS project_approval_name,
        [Capital Repairs]          AS is_capital_repair,
        [Exp_ Date]                AS expiration_date,
        [Trip No_]                 AS trip_no,
        [Inventory Posting Group]  AS inventory_posting_group,
        [User ID]                  AS user_id,
        [Item Type]                AS item_type,
        [Requisition Batch Name]   AS requisition_batch_name,
        [Part Number]              AS part_number,
        [Biometric Id]             AS biometric_id
    FROM [dbo].[ANRML$Item Ledger Entry]
    WHERE [Entry No_] > {last_entry_no}
) t
"""
ledger_updates_df = spark.read.jdbc(url=jdbc_url, table=incremental_ledger_query, properties=jdbc_props)

# 2b. CONVERT TIMESTAMPS UTC -> WAT
# SQL Server stores these in UTC; business operates in WAT (UTC+1, Africa/Lagos).
# Converting here so Delta stores WAT, consistent with the logistics TAT pipeline.
print("Converting timestamps UTC -> WAT (Africa/Lagos)...")
for ts_col in TIMESTAMP_COLS:
    if ts_col in ledger_updates_df.columns:
        ledger_updates_df = ledger_updates_df.withColumn(
            ts_col,
            F.from_utc_timestamp(F.col(ts_col).cast("timestamp"), WAT_TZ)
        )
print("✅ Timestamp conversion complete.")

# 3. Fetch Dimension Data (Item)
# We need this to get the 'uom' and 'item_description' columns
item_dim_query = """
(
    SELECT 
        [Base Unit of Measure] AS uom,
        [No_]                  AS item_no,
        [Description]          AS item_description
    FROM [dbo].[ANRML$Item]
) t
"""
item_df = spark.read.jdbc(url=jdbc_url, table=item_dim_query, properties=jdbc_props)

# read the process-center-code and its description to map
process_center_code_query = """
    (
        SELECT 
            [Code] as process_center_code,
            [Name] as department_process_center,
            [Shortcut Dimension 1 Code] as department_name
        FROM [dbo].[ANRML$Dimension Value]
        WHERE [Dimension Code] in ('PROCESS CENTER')
    ) t
    """
process_center_code_map = spark.read.jdbc(
    url=jdbc_url,
    table=process_center_code_query,
    properties=jdbc_props
)

# 4. Join Fact and Dimension
# This ensures the schema matches your existing Delta Table
incremental_final_df = (
    ledger_updates_df
    .join(broadcast(item_df), on="item_no", how="left")
    .join(broadcast(process_center_code_map), on="process_center_code", how="left")
    .withColumn(
        "entry_type_desc",
        when(col("entry_type") == 0, "Purchase")
        .when(col("entry_type") == 1, "Sale")
        .when(col("entry_type") == 2, "Positive Adjmt")
        .when(col("entry_type") == 3, "Negative Adjmt")
        .when(col("entry_type") == 4, "Transfer")
        .when(col("entry_type") == 5, "Consumption")
        .when(col("entry_type") == 6, "Output")
        .when(col("entry_type") == 8, "Assembly Consumption")
        .when(col("entry_type") == 9, "Assembly Output")
        .otherwise("Unknown")
    )
)

# 5. Write to Delta — full overwrite on first load, MERGE on subsequent runs
row_count = incremental_final_df.count()

if row_count == 0:
    print("No records found in MSSQL. Nothing to write.")
elif not table_exists:
    print(f"Performing full initial load ({row_count:,} rows)...")
    (
        incremental_final_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(table_name)
    )
    print(f"✅ Initial load complete — {row_count:,} rows written to {table_name}")
else:
    incremental_final_df.createOrReplaceTempView("incremental_batch")

    # Now 'uom' and 'item_description' exist in incremental_batch
    spark.sql(f"""
        MERGE INTO {table_name} AS target
        USING incremental_batch AS updates
        ON target.entry_no = updates.entry_no
        WHEN MATCHED THEN
            UPDATE SET *
        WHEN NOT MATCHED THEN
            INSERT *
    """)
    print(f"✅ Merge complete — {row_count:,} records upserted into {table_name}")

print(f"Timezone stored : WAT (Africa/Lagos)")
print("Incremental load completed successfully.")
spark.stop()