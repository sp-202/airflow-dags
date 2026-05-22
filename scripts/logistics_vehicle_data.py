# =============================================================================
# Logistics Vehicle Master Pipeline — Full Overwrite (Airflow-triggered every 3hrs)
# Reads the vehicle master table and overwrites the Delta table each run.
# Source: [dbo].[ANRML$Vehicle Master]
#
# CHANGELOG:
#   - Initial version: reads all columns from ANRML$Vehicle Master,
#     renames to snake_case, and overwrites logistics.vehicle_master.
#   - last_data_processed_timestamp: records exactly when this pipeline
#     run processed the data (UTC, millisecond precision).
# =============================================================================

import os
import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# -----------------------------------------------------------------------------
# 1. SAFE CREDENTIAL LOADING
# -----------------------------------------------------------------------------
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

# -----------------------------------------------------------------------------
# 2. JDBC CONFIGURATION
# -----------------------------------------------------------------------------
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
    "fetchsize": "10000",
}

# -----------------------------------------------------------------------------
# 3. SPARK SESSION
# -----------------------------------------------------------------------------
run_id = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

spark = SparkSession.builder \
    .appName(f"logistics-vehicle-master-overwrite-{run_id}") \
    .getOrCreate()

spark.conf.set("spark.sql.repl.eagerEval.enabled", True)
spark.conf.set("spark.sql.repl.eagerEval.maxNumRows", 10)
spark.conf.set("spark.sql.session.timeZone", "Africa/Lagos")  # WAT = UTC+1

print(f"Connecting to {db_host} as user: {db_user} on database: {db_name}")

# -----------------------------------------------------------------------------
# 4. TARGET TABLE CONFIG
# -----------------------------------------------------------------------------
TABLE_NAME = "logistics.vehicle_master"

# -----------------------------------------------------------------------------
# 5. CAPTURE PIPELINE RUN TIMESTAMP (UTC, millisecond precision)
#    Used as the audit column last_data_processed_timestamp.
# -----------------------------------------------------------------------------
pipeline_now_iso = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
pipeline_now_ms  = int(datetime.datetime.utcnow().timestamp() * 1000)

print(f"Pipeline capture time (UTC) : {pipeline_now_iso}")
print(f"Pipeline capture time (ms)  : {pipeline_now_ms}")

# -----------------------------------------------------------------------------
# 6. READ VEHICLE MASTER FROM SQL SERVER
# -----------------------------------------------------------------------------
raw_vehicles = spark.read.jdbc(
    url=jdbc_url,
    table="[dbo].[ANRML$Vehicle Master]",
    properties=jdbc_props,
)

total_count = raw_vehicles.count()
print(f"Total vehicle master rows fetched from SQL Server: {total_count:,}")

if total_count == 0:
    print("No vehicle master records found. Skipping overwrite to protect existing data.")
    spark.stop()
    exit(0)

# -----------------------------------------------------------------------------
# 7. SELECT & RENAME COLUMNS (source schema → snake_case)
#    All 21 columns from the schema are mapped here.
#    `timestamp` (binary) is kept as-is; all others cast where appropriate.
# -----------------------------------------------------------------------------
vehicles = raw_vehicles.select(

    # Binary system column — kept as raw bytes, not cast
    F.col("timestamp").alias("timestamp"),

    # Identifiers
    F.col("Vehicle No").alias("vehicle_no"),
    F.col("Actual Vehicle No").alias("actual_vehicle_no"),
    F.col("Vehicle Model No").alias("vehicle_model_no"),
    F.col("Vehicle Identification No").alias("vehicle_identification_no"),

    # Make / body
    F.col("Make Code").alias("make_code"),
    F.col("Make Name").alias("make_name"),
    F.col("Body Type").alias("body_type"),
    F.col("Engine Type").cast("integer").alias("engine_type"),
    F.col("No_ Of Axcel").cast("integer").alias("no_of_axcel"),

    # Status & availability
    F.col("Gate Entry Status").cast("integer").alias("gate_entry_status"),
    F.col("FA Code").alias("fa_code"),
    F.col("Blocked").cast("short").alias("blocked"),
    F.col("Blocked By").alias("blocked_by"),
    F.col("Blocked Time").cast("timestamp").alias("blocked_time"),
    F.col("Reason of Availability").alias("reason_of_availability"),

    # Tracking / location
    F.col("Truck Speed").alias("truck_speed"),
    F.col("Address").alias("address"),
    F.col("Last Nova Updated Details").cast("timestamp").alias("last_nova_updated_details"),

    # Last trip linkage
    F.col("Last Trip No_").alias("last_trip_no"),
    F.col("Last Trip Clossing").cast("timestamp").alias("last_trip_clossing"),  # source typo preserved

)

# -----------------------------------------------------------------------------
# 8. APPEND AUDIT COLUMN
# -----------------------------------------------------------------------------
vehicles = vehicles.withColumn(
    "last_data_processed_timestamp",
    F.lit(pipeline_now_iso).cast("string")
)

# -----------------------------------------------------------------------------
# 9. FULL OVERWRITE INTO DELTA TABLE
# -----------------------------------------------------------------------------
spark.sql("CREATE SCHEMA IF NOT EXISTS logistics")

print(f"Overwriting {TABLE_NAME} with {total_count:,} vehicle master rows...")

(
    vehicles.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TABLE_NAME)
)

print(f"✅ Overwrite complete — {total_count:,} rows written to {TABLE_NAME}")

# -----------------------------------------------------------------------------
# 10. QUICK VALIDATION
# -----------------------------------------------------------------------------
print("\n--- Blocked Vehicle Summary ---")
vehicles.groupBy("blocked").count().orderBy("blocked").show()

print("\n--- Gate Entry Status Distribution ---")
vehicles.groupBy("gate_entry_status").count().orderBy("gate_entry_status").show()

print("\n--- Sample: Currently Blocked Vehicles ---")
vehicles.filter(F.col("blocked") == 1).select(
    "vehicle_no", "actual_vehicle_no", "make_name",
    "blocked_by", "blocked_time", "reason_of_availability",
    "last_data_processed_timestamp"
).show(20, truncate=False)

# -----------------------------------------------------------------------------
# 11. DONE
# -----------------------------------------------------------------------------
print(f"\nRun ID                        : {run_id}")
print(f"Table                         : {TABLE_NAME}")
print(f"Pipeline capture time (UTC)   : {pipeline_now_iso}")
print(f"Pipeline capture time (ms)    : {pipeline_now_ms}")
print("Vehicle master load completed successfully.")

spark.stop()