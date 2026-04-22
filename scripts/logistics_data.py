# =============================================================================
# Logistics TAT Pipeline — Incremental Load (Airflow-triggered)
# Tracks new/updated trips via trip_no watermark and merges into Delta table.
# =============================================================================

import os
import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

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
    .appName(f"logistics-tat-incremental-{run_id}") \
    .getOrCreate()

spark.conf.set("spark.sql.repl.eagerEval.enabled", True)
spark.conf.set("spark.sql.repl.eagerEval.maxNumRows", 10)

print(f"Connecting to {db_host} as user: {db_user} on database: {db_name}")

# -----------------------------------------------------------------------------
# 4. TARGET TABLE CONFIG
# -----------------------------------------------------------------------------
TABLE_NAME  = "logistics.raw_data"
MERGE_KEY   = "trip_no"   # unique identifier for upsert

# -----------------------------------------------------------------------------
# 5. WATERMARK — get the latest trip_closing_time already in the Delta table
#    We use trip_closing_time (not trip_no string) as the watermark so we also
#    catch trips that were closed/updated after the last run.
# -----------------------------------------------------------------------------
spark.sql("CREATE SCHEMA IF NOT EXISTS logistics")

try:
    last_closing_time_raw = spark.sql(
        f"SELECT MAX(trip_closing_time) FROM {TABLE_NAME}"
    ).collect()[0][0]
    if last_closing_time_raw is None:
        last_closing_time = "1900-01-01 00:00:00"
    else:
        # Always format as yyyy-MM-dd HH:mm:ss — required by CONVERT(..., 120)
        # str(Timestamp) can include microseconds e.g. "2024-05-28 10:57:00.123456"
        # which SQL Server style 120 rejects — truncate to seconds only.
        last_closing_time = last_closing_time_raw.strftime("%Y-%m-%d %H:%M:%S")
    print(f"Watermark — last trip_closing_time in Delta: {last_closing_time}")
except Exception:
    last_closing_time = "1900-01-01 00:00:00"
    print("Target table not found — performing full initial load.")

# -----------------------------------------------------------------------------
# 6. READ INCREMENTAL TRIPS FROM SQL SERVER
#    Only fetch trips closed after the watermark to minimise JDBC payload.
# -----------------------------------------------------------------------------
incremental_trips_query = f"""
(
    SELECT *
    FROM [dbo].[ANRML$RGP_NRGP Header]
    WHERE [Trip Close] = 1
      AND [Type]       = 1
      AND [Trip Closing Time] > CONVERT(DATETIME, '{last_closing_time}', 120)
) t
"""
# CONVERT style 120 = ODBC canonical: yyyy-mm-dd hh:mi:ss
# Forces SQL Server to parse the watermark string safely as DATETIME
# without relying on server locale/regional date format settings

raw_trips = spark.read.jdbc(
    url=jdbc_url,
    table=incremental_trips_query,
    properties=jdbc_props,
)

incremental_count = raw_trips.count()
print(f"New / updated trips fetched from SQL Server: {incremental_count}")

if incremental_count == 0:
    print("No new trips found. Nothing to merge.")
    spark.stop()
    exit(0)

# -----------------------------------------------------------------------------
# 7. READ ROUTE MASTER (always fresh — small table)
# -----------------------------------------------------------------------------
route_window = Window.partitionBy("Route ID").orderBy(F.col("Starting Date").desc())

route_master = (
    spark.read.jdbc(
        url=jdbc_url,
        table="[dbo].[ANRML$Route Ledger]",
        properties=jdbc_props,
    )
    .withColumn("_rn", F.row_number().over(route_window))
    .filter(F.col("_rn") == 1)
    .drop("_rn", "timestamp")
    .select(
        F.col("Route ID").alias("route_id_def"),
        F.col("TAT Kilometer").alias("defined_tat_hrs"),
        F.col("Diesel Quantity").alias("defined_diesel_quantity"),
        F.col("Trip Allowance").alias("defined_trip_allowance"),
        F.col("Management Fees").alias("defined_management_fees"),
        F.col("Diesel Issue Default Location").alias("defined_diesel_location"),
    )
)

# -----------------------------------------------------------------------------
# 8. SELECT & RENAME TRIP COLUMNS
# -----------------------------------------------------------------------------
trips = raw_trips.select(
    F.col("Trip No").alias("trip_no"),
    F.col("Vehicle No_").alias("vehicle_no"),
    F.col("Driver Name").alias("driver_name"),
    F.col("Driver No_").alias("driver_no"),
    F.col("Route ID").alias("route_id"),
    F.col("Party Name").alias("party_name"),
    F.col("City").alias("city"),
    F.col("Company").alias("company"),

    # Dates & times
    F.col("Trip Start Date").cast("timestamp").alias("trip_start_date"),
    F.col("Trip Start Time").cast("timestamp").alias("trip_start_time"),
    F.col("Trip Closing Time").cast("timestamp").alias("trip_closing_time"),
    F.col("Return Date").cast("timestamp").alias("return_date"),
    F.col("Posting Date").cast("timestamp").alias("posting_date"),

    # Item / location
    F.col("Item No").alias("item_no"),
    F.col("Logistics Yard in Time").cast("timestamp").alias("logistics_yard_in_time"),
    F.col("Loading Point in Time").cast("timestamp").alias("loading_point_in_time"),
    F.col("Loading Point out Time").cast("timestamp").alias("loading_point_out_time"),
    F.col("Unloading Point in Time").cast("timestamp").alias("unloading_point_in_time"),
    F.col("Unloading Point out Time").cast("timestamp").alias("unloading_point_out_time"),

    # TAT / on-time
    F.col("TAT Kilometer").alias("tat_kilometer"),
    F.col("Ontime").alias("ontime"),
    F.col("Total Stoppage Time").alias("total_stoppage_time"),
    F.col("Halt Start Date Time").cast("timestamp").alias("halt_start_date_time"),
    F.col("Last Halt Date Time").alias("last_halt_date_time"),

    # Odometer
    F.col("Kilometer Out").alias("kilometer_out"),
    F.col("Kilometer IN").alias("kilometer_in"),

    # Fuel
    F.col("Diesel").alias("diesel"),
    F.col("Diesel Consumed").alias("diesel_consumed"),
    F.col("Disel Balance").alias("disel_balance"),
    F.col("Fuel Available in Ltr").alias("fuel_available_in_ltr"),
    F.col("Diesel Issue Location").alias("diesel_issue_location"),

    # Weights
    F.col("Gross Weight").alias("gross_weight"),
    F.col("Net Weight").alias("net_weight"),
    F.col("Tare Weight").alias("tare_weight"),

    # Flags / codes
    F.col("Trip Type").alias("trip_type"),
    F.col("Type of Movement").alias("type_of_movement"),
    F.col("Logistic Operation").alias("logistic_operation"),
    F.col("Trip Close").alias("trip_close"),
    F.col("Trip Clossing Type").alias("trip_clossing_type"),
    F.col("Status").alias("status"),
)

# -----------------------------------------------------------------------------
# 9. COMPUTE DERIVED METRICS
# -----------------------------------------------------------------------------
def hrs_diff(start_col, end_col):
    return (F.unix_timestamp(F.col(end_col)) - F.unix_timestamp(F.col(start_col))) / 3600.0

trips = (
    trips
    .withColumn("total_kms",
        (F.col("kilometer_in") - F.col("kilometer_out")).cast("decimal(38,19)"))
    .withColumn("fuel_efficiency_km_l",
        F.when(F.col("diesel_consumed") > 0,
            (F.col("kilometer_in") - F.col("kilometer_out")) / F.col("diesel_consumed")
        ).otherwise(None).cast("decimal(35,2)"))
    .withColumn("yard_to_loading_hrs",     hrs_diff("trip_start_time",        "loading_point_in_time"))
    .withColumn("loading_time_hrs",        hrs_diff("loading_point_in_time",  "loading_point_out_time"))
    .withColumn("transit_time_hrs",        hrs_diff("loading_point_out_time", "unloading_point_in_time"))
    .withColumn("unloading_time_hrs",      hrs_diff("unloading_point_in_time","unloading_point_out_time"))
    .withColumn("return_time_hrs",         hrs_diff("unloading_point_out_time","logistics_yard_in_time"))
    .withColumn("total_trip_duration_hrs", hrs_diff("trip_start_time",        "logistics_yard_in_time"))
    .withColumn("journey_time_hrs",        hrs_diff("loading_point_out_time", "unloading_point_out_time"))
    .withColumn("idle_time_hrs",
        F.col("total_trip_duration_hrs") - F.col("journey_time_hrs"))
    # Round all hour columns
    .withColumn("yard_to_loading_hrs",     F.round(F.col("yard_to_loading_hrs"),     2))
    .withColumn("loading_time_hrs",        F.round(F.col("loading_time_hrs"),        2))
    .withColumn("transit_time_hrs",        F.round(F.col("transit_time_hrs"),        2))
    .withColumn("unloading_time_hrs",      F.round(F.col("unloading_time_hrs"),      2))
    .withColumn("return_time_hrs",         F.round(F.col("return_time_hrs"),         2))
    .withColumn("total_trip_duration_hrs", F.round(F.col("total_trip_duration_hrs"), 2))
    .withColumn("journey_time_hrs",        F.round(F.col("journey_time_hrs"),        2))
    .withColumn("idle_time_hrs",           F.round(F.col("idle_time_hrs"),           2))
)

# -----------------------------------------------------------------------------
# 10. JOIN WITH ROUTE MASTER
# -----------------------------------------------------------------------------
incremental_final_df = (
    trips.join(route_master, trips["route_id"] == route_master["route_id_def"], how="left")
    .drop("route_id_def")
    .select([
        # Identifiers
        "trip_no", "vehicle_no", "driver_name", "driver_no",
        "route_id", "party_name", "city", "company",
        # Dates
        "trip_start_date", "trip_start_time", "trip_closing_time",
        "return_date", "posting_date", "item_no",
        # Timestamps
        "logistics_yard_in_time", "loading_point_in_time", "loading_point_out_time",
        "unloading_point_in_time", "unloading_point_out_time",
        # TAT / on-time
        "tat_kilometer", "ontime", "total_stoppage_time",
        "halt_start_date_time", "last_halt_date_time",
        # Odometer & fuel
        "kilometer_out", "kilometer_in",
        "diesel", "diesel_consumed", "disel_balance",
        "fuel_available_in_ltr", "diesel_issue_location",
        # Weights
        "gross_weight", "net_weight", "tare_weight",
        # Flags
        "trip_type", "type_of_movement", "logistic_operation",
        "trip_close", "trip_clossing_type", "status",
        # Computed metrics
        "total_kms", "fuel_efficiency_km_l",
        "yard_to_loading_hrs", "loading_time_hrs", "transit_time_hrs",
        "unloading_time_hrs", "return_time_hrs", "total_trip_duration_hrs",
        "journey_time_hrs", "idle_time_hrs",
        # Route definitions
        "defined_tat_hrs", "defined_diesel_quantity",
        "defined_trip_allowance", "defined_management_fees",
        "defined_diesel_location",
    ])
)

# -----------------------------------------------------------------------------
# 11. MERGE INTO DELTA TABLE (upsert on trip_no)
#     - First run: table doesn't exist → full write
#     - Subsequent runs: MERGE updates changed trips, inserts new ones
# -----------------------------------------------------------------------------
try:
    spark.read.table(TABLE_NAME)
    table_exists = True
except Exception:
    table_exists = False

if not table_exists:
    # ── Initial full load ────────────────────────────────────────────────────
    print("Performing initial full load...")
    (
        incremental_final_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(TABLE_NAME)
    )
    print(f"✅ Initial load complete — {incremental_final_df.count():,} rows written to {TABLE_NAME}")

else:
    # ── Incremental merge ────────────────────────────────────────────────────
    incremental_final_df.createOrReplaceTempView("incremental_batch")

    spark.sql(f"""
        MERGE INTO {TABLE_NAME} AS target
        USING incremental_batch AS updates
        ON target.trip_no = updates.trip_no
        WHEN MATCHED THEN
            UPDATE SET *
        WHEN NOT MATCHED THEN
            INSERT *
    """)
    print(f"✅ Merge complete — {incremental_count:,} rows upserted into {TABLE_NAME}")

# -----------------------------------------------------------------------------
# 12. DONE
# -----------------------------------------------------------------------------
print(f"Run ID       : {run_id}")
print(f"Watermark was: {last_closing_time}")
print(f"Table        : {TABLE_NAME}")
print("Incremental load completed successfully.")

spark.stop()