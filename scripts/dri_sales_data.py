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

# --- 3. SESSION & LOGIC ---
run_id = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
unique_app_name = f"dri-sales-data-{run_id}"

spark = SparkSession.builder \
    .appName(unique_app_name) \
    .getOrCreate()

# --- 4. CUSTOMER Table loaded ---
customer_df = spark.read.jdbc(
    url=jdbc_url,
    table="""(
        SELECT *
        FROM [dbo].[ANRML$Customer]
    ) t""",
    properties=jdbc_props
)
# customer_df.show()

# nav table
nav_table = "nav_raw_data.raw_data"

raw_sales_data = spark.sql(f"SELECT * from {nav_table}") \
                      .filter((F.col("department_name") == "DRI") & (F.col("entry_type_desc") == "Sale"))

sales_data_wat = raw_sales_data \
    .withColumn("posting_date", F.col("posting_date").cast("timestamp")) \
    .withColumn("document_date", F.col("document_date").cast("timestamp"))

# Safe filtering for June 2026 using the corrected timezone columns
# sales_data_wat = sales_data_wat.filter(
#     (F.year(F.col("posting_date")) == 2026) & 
#     (F.month(F.col("posting_date")) == 6)
# )

# Filter for specific item numbers
allowed_items = ['FGDRIGRDG1104', 'FGDRIFNDF1101', 'FGDRIGRDG1101', 'FGDRIGRDG1102']
sales_data_wat = sales_data_wat.filter(F.col("item_no").isin(allowed_items))

# Format date safely for final reporting display
sales_display = sales_data_wat.withColumn(
    "posting_date", 
    F.date_format(F.col("posting_date"), "dd/MM/yyyy")
)

# Join tables
joined_data = sales_display.join(
    customer_df, 
    sales_display["source_no"] == customer_df["No_"], 
    "inner"
)

# -----------------------------------------------------------------------------
# 2. Transform quantity (Absolute value -> Divide by 1000 -> Round to whole number)
# -----------------------------------------------------------------------------
final_spark_df = joined_data.withColumn("quantity", F.round((F.col("quantity") / 1000)))

# -----------------------------------------------------------------------------
# 3. Select ONLY your requested columns
# -----------------------------------------------------------------------------
required_columns = [
    "item_no", "entry_no", "posting_date", "source_no", "document_no", 
    "location_code", "quantity", "document_date", "external_doc_no", 
    "dimension_set_id", "user_id", "uom", "item_description", 
    "department_process_center", "department_name", "Name", "Search Name", "Address"
]

dri_sales_data_df = final_spark_df.select(required_columns) \
    .withColumnRenamed("Search Name", "search_name") \
    .withColumnRenamed("Name", "name") \
    .withColumnRenamed("Address", "address")

target_s3_path = "s3a://dri-sales-data/bronze/dri_sales"
target_table = "dri_db.dri_sales_db"

print(f"Saving processed DRI Sales data to: {target_s3_path}")

spark.sql("CREATE SCHEMA IF NOT EXISTS dri_db")

(
    dri_sales_data_df.write
    .format("delta")
    .mode("overwrite")
    .option("path", target_s3_path)
    .option("overwriteSchema", "true") 
    .saveAsTable(target_table)
)