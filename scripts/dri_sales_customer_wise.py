import os
import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, broadcast, when

# =============================================================================
# DRI SALES (Fines / Lumps) -> Delta table for dashboards
#
# Source tables:
#   nav_raw_data.raw_data      (Delta bronze, built by the nav-raw-data job from
#                               [ANRML$Item Ledger Entry] + Item + Dimension Value)
#       -> entry_type_desc = 'Sale' AND item_no in the 4 DRI finished-goods items
#   [dbo].[ANRML$Customer]     (SQL Server via JDBC)
#       -> inner join on raw_data.source_no = Customer.[No_]
#
# Target: nav_gold.dri_sales_customer_wise  (one row per item ledger entry, key = entry_no)
#   product_type : 'Fines' for FGDRIFNDF1101, 'Lumps' for the other DRI items
#   quantity_mt  : -quantity / 1000  (sales are negative in the ledger, so this
#                  is positive tonnes sold; returns/corrections come out negative,
#                  so SUM(quantity_mt) = net tonnes sold)
# =============================================================================

# --- 1. SAFE CREDENTIAL LOADING ---
def get_env_var(name, default=None):
    value = os.getenv(name, default)
    if value is None:
        # Log which one is missing for easier debugging in Airflow logs
        print(f"CRITICAL: Environment variable {name} is MISSING!")
        return ""  # Return empty string to prevent NullPointerException in Java
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

run_id = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
unique_app_name = f"dri-sales-customer-wise-{run_id}"

# --- 3. SESSION & CONFIG ---
spark = SparkSession.builder \
    .appName(unique_app_name) \
    .getOrCreate()

# Business operates in WAT (UTC+1). posting_date in the bronze table is already
# WAT (the nav-raw-data job converts UTC -> WAT only when real times exist),
# so NO second timezone shift here.
WAT_TZ = "Africa/Lagos"
spark.conf.set("spark.sql.session.timeZone", WAT_TZ)

source_table = "nav_raw_data.raw_data"
table_name = "nav_gold.dri_sales_customer_wise"

FINES_ITEMS = ["FGDRIFNDF1101"]
LUMPS_ITEMS = ["FGDRIGRDG1101", "FGDRIGRDG1102", "FGDRIGRDG1104"]
DRI_ITEMS = FINES_ITEMS + LUMPS_ITEMS

print(f"Connecting to {db_host} as user: {db_user} on database: {db_name}")

# --- SCHEMA + TABLE-EXISTENCE CHECK ---
spark.sql("CREATE SCHEMA IF NOT EXISTS nav_gold")

table_exists = False
try:
    # Force an action so a stale/broken table surfaces here, not inside MERGE.
    spark.read.table(table_name).take(1)
    table_exists = True
    print(f"✅ Target table {table_name} found and readable — will run incremental MERGE.")
except Exception as e:
    table_exists = False
    print(f"⚠️  Target table {table_name} not found/readable ({type(e).__name__}) — will run full initial load.")

# 1. Get the Watermark
if table_exists:
    try:
        last_entry_no = spark.sql(f"SELECT MAX(entry_no) FROM {table_name}").collect()[0][0] or 0
    except Exception as e:
        print(f"⚠️  Could not read watermark from existing table ({e}). Defaulting to full load.")
        last_entry_no = 0
        table_exists = False
else:
    last_entry_no = 0

print(f"Watermark (last_entry_no): {last_entry_no}")

# 2. Fetch incremental DRI sales from bronze (Fact)
sales_updates_df = (
    spark.read.table(source_table)
    .filter(
        (col("entry_no") > last_entry_no)
        & (col("entry_type_desc") == "Sale")
        & col("item_no").isin(DRI_ITEMS)
    )
    .select(
        "entry_no",
        "posting_date",
        "document_date",
        "item_no",
        "item_description",
        "uom",
        "quantity",
        "invoiced_quantity",
        "source_no",
        "document_no",
        "external_doc_no",
        "document_type",
        "document_line_no",
        "location_code",
        "vehicle_no",
        "trip_no",
        "user_id",
        "department_name",
        "department_process_center",
    )
)

# 3. Fetch Dimension Data (Customer)
customer_query = """
(
    SELECT
        [No_]                    AS customer_no,
        [Name]                   AS customer_name,
        [Search Name]            AS customer_search_name,
        [City]                   AS customer_city,
        [Address]                AS customer_address,
        [Country_Region Code]    AS customer_country,
        [Salesperson Code]       AS salesperson_code,
        [Customer Posting Group] AS customer_posting_group
    FROM [dbo].[ANRML$Customer]
) t
"""
customer_df = spark.read.jdbc(url=jdbc_url, table=customer_query, properties=jdbc_props)

# 4. Join Fact and Dimension + derived columns
incremental_final_df = (
    sales_updates_df
    .join(broadcast(customer_df), sales_updates_df["source_no"] == customer_df["customer_no"], "inner")
    .drop("source_no")
    .withColumn(
        "product_type",
        when(col("item_no").isin(FINES_ITEMS), "Fines").otherwise("Lumps")
    )
    .withColumn("quantity_mt", F.round(-col("quantity") / 1000, 3))
    .withColumn("sale_date", F.to_date(col("posting_date")))
    .withColumn("sale_year", F.year(col("sale_date")))
    .withColumn("sale_month", F.date_format(col("sale_date"), "yyyy-MM"))
    .withColumn("sale_month_start", F.trunc(col("sale_date"), "month"))
    .withColumn("loaded_at", F.current_timestamp())
)

# 5. Write to Delta — full overwrite on first load, MERGE on subsequent runs
row_count = incremental_final_df.count()

if row_count == 0:
    print("No new DRI sales entries found. Nothing to write.")
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
print("DRI sales load completed successfully.")
spark.stop()
