import os
import datetime
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# --- 1. SAFE CREDENTIAL LOADING ---
def get_env_var(name, default=None):
    value = os.getenv(name, default)
    if value is None:
        print(f"CRITICAL: Environment variable {name} is MISSING!")
        return "" 
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
    f"encrypt=true;"
    f"trustServerCertificate=true"
)

jdbc_props = {
    "user": db_user,
    "password": db_pass,
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    "fetchsize": "10000"
)

# Create a unique suffix
run_id = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
unique_app_name = f"nav-raw-data-{run_id}"

# --- 3. SESSION & TIMEZONE CONFIGURATION ---
spark = SparkSession.builder \
    .appName(unique_app_name) \
    .getOrCreate()

# Ensure session processes dates in West Africa Time matching business rules
spark.conf.set("spark.sql.session.timeZone", "Africa/Lagos")
WAT_TZ = "Africa/Lagos"

print(f"Connecting to {db_host} as user: {db_user} on database: {db_name}")

s3_delta_path = "s3a://nav-data/bronze/raw_nav_data"
table_name = "nav_raw_data.raw_data"

# --- 4. WATERMARK SELECTION ---
try:
    last_entry_no = spark.sql(f"SELECT MAX(entry_no) FROM {table_name}").collect()[0][0] or 0
except Exception:
    last_entry_no = 0

print(f"Current Watermark (entry_no): {last_entry_no}")

# --- 5. FETCH INCREMENTAL LEDGER DATA ---
incremental_ledger_query = f"""
(
    SELECT 
        [Entry No_]                 AS entry_no,
        [Item No_]                  AS item_no,
        [Posting Date]              AS posting_date,
        [Entry Type]                AS entry_type,
        [Source No_]                AS source_no,
        [Document No_]              AS document_no,
        [Description]               AS description,
        [Location Code]             AS location_code,
        [Quantity]                  AS quantity,
        [Remaining Quantity]       AS remaining_quantity,
        [Invoiced Quantity]         AS invoiced_quantity,
        [Global Dimension 1 Code]   AS department,
        [Global Dimension 2 Code]   AS process_center_code,
        [Document Date]             AS document_date,
        [External Document No_]     AS external_doc_no,
        [Document Type]             AS document_type,
        [Document Line No_]         AS document_line_no,
        [Dimension Set ID]          AS dimension_set_id,
        [Qty_ per Unit of Measure] AS qty_per_uom,
        [Unit of Measure Code]     AS uom_code,
        [Purpose]                   AS purpose,
        [Supplier Name]             AS supplier_name,
        [Gen Prod_ Posting Group]  AS gen_prod_posting_group,
        [Vehicle No_]               AS vehicle_no,
        [Project Approval Name]    AS project_approval_name,
        [Capital Repairs]           AS is_capital_repair,
        [Exp_ Date]                 AS expiration_date,
        [Trip No_]                  AS trip_no,
        [Inventory Posting Group]  AS inventory_posting_group,
        [User ID]                   AS user_id,
        [Item Type]                 AS item_type,
        [Requisition Batch Name]   AS requisition_batch_name,
        [Part Number]               AS part_number,
        [Biometric Id]             AS biometric_id
    FROM [dbo].[ANRML$Item Ledger Entry]
    WHERE [Entry No_] > {last_entry_no}
) t
"""
ledger_updates_df = spark.read.jdbc(url=jdbc_url, table=incremental_ledger_query, properties=jdbc_props)

incremental_count = ledger_updates_df.count()
print(f"New ledger records discovered: {incremental_count:,}")

# Optimization: Early exit if there are no new records, saving enrichment cluster time
if incremental_count == 0:
    print("No new records found in MSSQL. Pipeline closing cleanly.")
    spark.stop()
    exit(0)

# --- 6. UTC TO WAT TIMEOUT CONVERSION ---
# Convert incoming database UTC datetimes cleanly over to Africa/Lagos business reporting time
timestamp_cols = ["posting_date", "document_date", "expiration_date"]
for col_name in timestamp_cols:
    if col_name in ledger_updates_df.columns:
        ledger_updates_df = ledger_updates_df.withColumn(
            col_name,
            F.from_utc_timestamp(F.col(col_name).cast("timestamp"), WAT_TZ)
        )

# --- 7. FETCH ENRICHMENT DIMENSIONS ---
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

process_center_code_query = """
(
    SELECT 
        [Code] as process_center_code,
        [Name] as department_process_center,
        [Shortcut Dimension 1 Code] as department_name
    FROM [dbo].[ANRML$Dimension Value]
    WHERE [Dimension Code] = 'PROCESS CENTER'
) t
"""
process_center_code_map = spark.read.jdbc(url=jdbc_url, table=process_center_code_query, properties=jdbc_props)

# --- 8. JOIN & DECODE FACT WITH DIMENSIONS ---
incremental_final_df = (
    ledger_updates_df
    .join(F.broadcast(item_df), on="item_no", how="left")
    .join(F.broadcast(process_center_code_map), on="process_center_code", how="left")
    .withColumn(
        "entry_type_desc",
        F.when(F.col("entry_type") == 0, "Purchase")
        .when(F.col("entry_type") == 1, "Sale")
        .when(F.col("entry_type") == 2, "Positive Adjmt")
        .when(F.col("entry_type") == 3, "Negative Adjmt")
        .when(F.col("entry_type") == 4, "Transfer")
        .when(F.col("entry_type") == 5, "Consumption")
        .when(F.col("entry_type") == 6, "Output")
        .when(F.col("entry_type") == 8, "Assembly Consumption")
        .when(F.col("entry_type") == 9, "Assembly Output")
        .otherwise("Unknown")
    )
)

# --- 9. MERGE INTO DELTA LAKE TARGET ---
incremental_final_df.createOrReplaceTempView("incremental_batch")

spark.sql(f"""
    MERGE INTO {table_name} AS target
    USING incremental_batch AS updates
    ON target.entry_no = updates.entry_no
    WHEN MATCHED THEN
        UPDATE SET *
    WHEN NOT MATCHED THEN
        INSERT *
""")

print(f"Successfully merged {incremental_count:,} records into {table_name}.")
print("Incremental load completed successfully.")
spark.stop()