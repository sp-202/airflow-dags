# =============================================================================
# Logistics Active Trips Pipeline — Full Overwrite (Airflow-triggered every 3hrs)
# Fetches active/open trips and overwrites the Delta table each run.
# Filters: Trip Close=0, Logistic Operation=1, Type=1, Status=1
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
    .appName(f"logistics-active-trips-overwrite-{run_id}") \
    .getOrCreate()

spark.conf.set("spark.sql.repl.eagerEval.enabled", True)
spark.conf.set("spark.sql.repl.eagerEval.maxNumRows", 10)

print(f"Connecting to {db_host} as user: {db_user} on database: {db_name}")

# -----------------------------------------------------------------------------
# 4. TARGET TABLE CONFIG
# -----------------------------------------------------------------------------
TABLE_NAME = "logistics.active_trips"

# -----------------------------------------------------------------------------
# 5. READ ACTIVE TRIPS FROM SQL SERVER
#    Filters pushed down to SQL Server to minimise JDBC payload:
#      - Trip Close      = 0  → trip is still open/active
#      - Logistic Operation = 1
#      - Type            = 1
#      - Status          = 1
# -----------------------------------------------------------------------------
active_trips_query = """
(
    SELECT *
    FROM [dbo].[ANRML$RGP_NRGP Header]
    WHERE [Trip Close]        = 0
      AND [Logistic Operation] = 1
      AND [Type]               = 1
      AND [Status]             = 1
) t
"""

raw_trips = spark.read.jdbc(
    url=jdbc_url,
    table=active_trips_query,
    properties=jdbc_props,
)

total_count = raw_trips.count()
print(f"Total active trips fetched from SQL Server: {total_count}")

if total_count == 0:
    print("No active trips found. Skipping overwrite to protect existing data.")
    spark.stop()
    exit(0)

# -----------------------------------------------------------------------------
# 6. READ ROUTE MASTER (always fresh — small table)
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
# 7. SELECT & RENAME TRIP COLUMNS
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
# 8. COMPUTE DERIVED METRICS
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
    .withColumn("yard_to_loading_hrs",     hrs_diff("trip_start_time",         "loading_point_in_time"))
    .withColumn("loading_time_hrs",        hrs_diff("loading_point_in_time",   "loading_point_out_time"))
    .withColumn("transit_time_hrs",        hrs_diff("loading_point_out_time",  "unloading_point_in_time"))
    .withColumn("unloading_time_hrs",      hrs_diff("unloading_point_in_time", "unloading_point_out_time"))
    .withColumn("return_time_hrs",         hrs_diff("unloading_point_out_time","logistics_yard_in_time"))
    .withColumn("total_trip_duration_hrs", hrs_diff("trip_start_time",         "logistics_yard_in_time"))
    .withColumn("journey_time_hrs",        hrs_diff("loading_point_out_time",  "unloading_point_out_time"))
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
# 9. JOIN WITH ROUTE MASTER
# -----------------------------------------------------------------------------
final_df = (
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
# 10. FULL OVERWRITE INTO DELTA TABLE
#     Every 3hrs run replaces the entire table with fresh active trips.
#     No watermark needed — always reflects current snapshot of open trips.
# -----------------------------------------------------------------------------
spark.sql("CREATE SCHEMA IF NOT EXISTS logistics")

print(f"Overwriting {TABLE_NAME} with {total_count:,} active trips...")

(
    final_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TABLE_NAME)
)

print(f"✅ Overwrite complete — {total_count:,} rows written to {TABLE_NAME}")

# -----------------------------------------------------------------------------
# 11. DONE
# -----------------------------------------------------------------------------
print(f"Run ID : {run_id}")
print(f"Table  : {TABLE_NAME}")
print("Active trips load completed successfully.")

spark.stop()