import os
import datetime
import uuid
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, abs as spark_abs

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

# Debug: Print the keys found (NEVER print the password)
print(f"Connecting to {db_host} as user: {db_user} on database: {db_name}")

s3_delta_path = "s3a://nav-data/bronze/raw_nav_data"
table_name = "nav_raw_data.raw_data"

# 1. Get the Watermark
try:
    last_entry_no = spark.sql(f"SELECT MAX(entry_no) FROM {table_name}").collect()[0][0] or 0
except Exception:
    last_entry_no = 0

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

# 4. Join Fact and Dimension
# This ensures the schema matches your existing Delta Table
incremental_final_df = (
    ledger_updates_df
    .join(item_df, on="item_no", how="left")
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

# 5. Merge into Delta
if incremental_final_df.count() > 0:
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
    print(f"Successfully merged {incremental_final_df.count()} records.")
else:
    print("No new records found in MSSQL.")

print("Incremental load completed successfully.")
spark.stop()