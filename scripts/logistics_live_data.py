# =============================================================================
# Logistics Active Trips Pipeline — Full Overwrite (Airflow-triggered every 3hrs)
# Fetches active/open trips and overwrites the Delta table each run.
# Filters: Trip Close=0, Logistic Operation=1, Type=1, Status=1
#
# CHANGELOG:
#   - ontime: NAV backend always stores 0 (FlowField, not computed in SQL).
#             Recalculated here using unix timestamps (millisecond precision,
#             timezone-safe) as: elapsed_ms > defined_tat_ms → 0, else → 1.
#             Pipeline capture time is used as "now" so every run is consistent.
#   - last_data_processed_timestamp: new column recording exactly when this
#             pipeline run processed the row (UTC, millisecond precision).
#   - item_description: joined from ANRML$Item table via item_no to bring in
#             the human-readable item name alongside item_no.
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
spark.conf.set("spark.sql.session.timeZone", "Africa/Lagos")  # WAT = UTC+1

print(f"Connecting to {db_host} as user: {db_user} on database: {db_name}")

# -----------------------------------------------------------------------------
# 4. TARGET TABLE CONFIG
# -----------------------------------------------------------------------------
TABLE_NAME = "logistics.active_trips"

# -----------------------------------------------------------------------------
# 5. CAPTURE PIPELINE RUN TIMESTAMP (UTC, millisecond precision)
#    This is the single "now" reference used for:
#      - ontime calculation   (elapsed ms vs TAT ms)
#      - hrs_elapsed_so_far   (how long trip has been running)
#      - last_data_processed_timestamp (audit column)
#
#    Using unix epoch milliseconds avoids ALL local timezone issues —
#    both trip_start_time (from SQL Server) and pipeline_now_ms are in
#    the same absolute scale regardless of server locale.
# -----------------------------------------------------------------------------
pipeline_now_ms   = int(datetime.datetime.utcnow().timestamp() * 1000)   # e.g. 1745750400000
pipeline_now_ts   = F.lit(pipeline_now_ms).cast("long")                  # broadcast as Spark literal
pipeline_now_iso  = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")

print(f"Pipeline capture time (UTC) : {pipeline_now_iso}")
print(f"Pipeline capture time (ms)  : {pipeline_now_ms}")

# -----------------------------------------------------------------------------
# 6. READ ITEM MASTER — maps item_no → item_description
#    FIX: was using undefined `props`; corrected to `jdbc_props`.
#    Only 3 columns fetched to keep the payload minimal (small table).
# -----------------------------------------------------------------------------
item_df_query = """
(
    SELECT
        [Base Unit of Measure] AS uom,
        [No_]                  AS item_no,
        [Description]          AS item_description
    FROM [dbo].[ANRML$Item]
) t
"""

item_df = (
    spark.read.jdbc(
        url=jdbc_url,
        table=item_df_query,
        properties=jdbc_props,          # ← fixed: was `props`
    )
    .select("item_no", "item_description", "uom")   # only what we need
)

print(f"Item master rows loaded: {item_df.count():,}")

# -----------------------------------------------------------------------------
# 7. READ ACTIVE TRIPS FROM SQL SERVER
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
# 8. READ ROUTE MASTER (always fresh — small table)
#    Takes the LATEST definition per route (most recent Starting Date).
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
        # NOTE: field is misnamed in NAV — "TAT Kilometer" actually stores TAT hours
        F.col("TAT Kilometer").alias("defined_tat_hrs"),
        F.col("Diesel Quantity").alias("defined_diesel_quantity"),
        F.col("Trip Allowance").alias("defined_trip_allowance"),
        F.col("Management Fees").alias("defined_management_fees"),
        F.col("Diesel Issue Default Location").alias("defined_diesel_location"),
    )
)

# -----------------------------------------------------------------------------
# 9. SELECT & RENAME TRIP COLUMNS
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

    # Current Location
    F.trim(F.col("Address")).alias("current_location"),

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
    F.col("Ontime").alias("ontime_nav"),
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

    # misc
    F.col("TAT Breach Reason").alias("tat_breach_reason"),
    F.col("Way Bill No_").alias("way_bill_no"),
    F.col("Heavy Equipment Code").alias("heavy_equipment_code"),
    F.col("Linked Trip ID").alias("linked_trip_id"),
    F.col("Lineked MRN No_").alias("linked_mrn_no"),      # note typo in source — "Lineked"
    F.col("Linked Sales Shipment No_").alias("linked_sales_shipment_no"),
    F.col("No_").alias("no_"),
    F.col("Holding Reason").alias("holding_reason")
)

# -----------------------------------------------------------------------------
# 10. JOIN WITH ITEM MASTER — enrich item_no with item_description & uom
#     Left join so trips with an unrecognised / missing item_no are kept.
#     item_df column "item_no" is renamed before join to avoid ambiguity,
#     then dropped after the join.
# -----------------------------------------------------------------------------
item_lookup = item_df.select(
    F.col("item_no").alias("_item_no_key"),
    F.col("item_description"),
    F.col("uom"),
)

trips = (
    trips
    .join(item_lookup, trips["item_no"] == item_lookup["_item_no_key"], how="left")
    .drop("_item_no_key")
)

print("Item description joined to trips.")

# -----------------------------------------------------------------------------
# 11. COMPUTE DERIVED METRICS
# -----------------------------------------------------------------------------
def hrs_diff(start_col, end_col):
    """Returns fractional hours between two timestamp columns."""
    return (F.unix_timestamp(F.col(end_col)) - F.unix_timestamp(F.col(start_col))) / 3600.0

trips = (
    trips

    # --- Distance & fuel ---
    .withColumn("total_kms",
        (F.col("kilometer_in") - F.col("kilometer_out")).cast("decimal(38,19)"))
    .withColumn("fuel_efficiency_km_l",
        F.when(F.col("diesel_consumed") > 0,
            (F.col("kilometer_in") - F.col("kilometer_out")) / F.col("diesel_consumed")
        ).otherwise(None).cast("decimal(35,2)"))

    # --- Time legs (based on stored timestamps) ---
    .withColumn("yard_to_loading_hrs",     hrs_diff("trip_start_time",         "loading_point_in_time"))
    .withColumn("loading_time_hrs",        hrs_diff("loading_point_in_time",   "loading_point_out_time"))
    .withColumn("transit_time_hrs",        hrs_diff("loading_point_out_time",  "unloading_point_in_time"))
    .withColumn("unloading_time_hrs",      hrs_diff("unloading_point_in_time", "unloading_point_out_time"))
    .withColumn("return_time_hrs",         hrs_diff("unloading_point_out_time","logistics_yard_in_time"))
    .withColumn("total_trip_duration_hrs", hrs_diff("trip_start_time",         "logistics_yard_in_time"))

    # ✅ CORRECTED — journey_time = yard_to_loading + transit + return
    .withColumn("journey_time_hrs",
        F.col("yard_to_loading_hrs")
        + F.col("transit_time_hrs")
        + F.col("return_time_hrs"))

    # ✅ CORRECTED — idle_time = loading + unloading
    .withColumn("idle_time_hrs",
        F.col("loading_time_hrs") + F.col("unloading_time_hrs"))

    # --- Round all hour columns to 2 dp ---
    .withColumn("yard_to_loading_hrs",     F.round(F.col("yard_to_loading_hrs"),     2))
    .withColumn("loading_time_hrs",        F.round(F.col("loading_time_hrs"),        2))
    .withColumn("transit_time_hrs",        F.round(F.col("transit_time_hrs"),        2))
    .withColumn("unloading_time_hrs",      F.round(F.col("unloading_time_hrs"),      2))
    .withColumn("return_time_hrs",         F.round(F.col("return_time_hrs"),         2))
    .withColumn("total_trip_duration_hrs", F.round(F.col("total_trip_duration_hrs"), 2))
    .withColumn("journey_time_hrs",        F.round(F.col("journey_time_hrs"),        2))
    .withColumn("idle_time_hrs",           F.round(F.col("idle_time_hrs"),           2))

    # -------------------------------------------------------------------------
    # LIVE ELAPSED TIME (millisecond-precision, timezone-safe)
    # -------------------------------------------------------------------------
    .withColumn("trip_start_ms",
        (F.unix_timestamp(F.col("trip_start_time")) * 1000).cast("long"))

    .withColumn("elapsed_ms",
        F.when(
            F.col("trip_start_ms").isNotNull() & (F.col("trip_start_ms") > 0),
            pipeline_now_ts - F.col("trip_start_ms")
        ).otherwise(F.lit(None).cast("long"))
    )

    .withColumn("hrs_elapsed_so_far",
        F.when(
            F.col("elapsed_ms").isNotNull(),
            F.round(F.col("elapsed_ms") / 3_600_000.0, 2)
        ).otherwise(F.lit(None).cast("double"))
    )
)

# -----------------------------------------------------------------------------
# 12. JOIN WITH ROUTE MASTER (brings in defined_tat_hrs per route)
# -----------------------------------------------------------------------------
trips = (
    trips
    .join(route_master, trips["route_id"] == route_master["route_id_def"], how="left")
    .drop("route_id_def")
)

# -----------------------------------------------------------------------------
# 13. COMPUTE ONTIME  (replaces unreliable NAV FlowField — always 0 in SQL)
# -----------------------------------------------------------------------------
trips = (
    trips

    .withColumn("defined_tat_ms",
        F.when(
            F.col("defined_tat_hrs").isNotNull() &
            (F.col("defined_tat_hrs").cast("double") > 0),
            (F.col("defined_tat_hrs").cast("double") * 3_600_000).cast("long")
        ).otherwise(F.lit(None).cast("long"))
    )

    .withColumn("ontime",
        F.when(
            F.col("elapsed_ms").isNotNull() & F.col("defined_tat_ms").isNotNull(),
            F.when(F.col("elapsed_ms") <= F.col("defined_tat_ms"), F.lit(1))
             .otherwise(F.lit(0))
        ).otherwise(F.lit(None).cast("integer"))
    )

    .withColumn("tat_breach_hrs",
        F.when(
            F.col("elapsed_ms").isNotNull() & F.col("defined_tat_ms").isNotNull(),
            F.round(
                (F.col("elapsed_ms") - F.col("defined_tat_ms")) / 3_600_000.0, 2
            )
        ).otherwise(F.lit(None).cast("double"))
    )

    .withColumn("tat_risk_status",
        F.when(F.col("defined_tat_ms").isNull(),  F.lit("NO TAT DEFINED"))
        .when(F.col("elapsed_ms").isNull(),        F.lit("NO START TIME"))
        .when(F.col("elapsed_ms") > F.col("defined_tat_ms"),
              F.lit("BREACHED"))
        .when(F.col("elapsed_ms") > (F.col("defined_tat_ms") * 0.75).cast("long"),
              F.lit("AT RISK"))
        .otherwise(F.lit("ON TRACK"))
    )

    .withColumn("last_data_processed_timestamp",
        F.lit(pipeline_now_iso).cast("string")
    )
)

# -----------------------------------------------------------------------------
# 14. FINAL COLUMN SELECTION & ORDERING
#     item_description and uom are placed right after item_no for readability.
# -----------------------------------------------------------------------------
final_df = trips.select([
    # --- Identifiers ---
    "trip_no", "vehicle_no", "driver_name", "driver_no",
    "route_id", "party_name", "city", "company",

    # --- Dates ---
    "trip_start_date", "trip_start_time", "trip_closing_time",
    "return_date", "posting_date",

    # --- Item (enriched) ---
    "item_no",
    "item_description",     # ✅ human-readable name from ANRML$Item
    "uom",                  # ✅ base unit of measure from ANRML$Item

    # --- Gate timestamps ---
    "logistics_yard_in_time", "loading_point_in_time", "loading_point_out_time",
    "unloading_point_in_time", "unloading_point_out_time",

    # --- Current Location ---
    "current_location",

    # --- TAT / ontime ---
    "tat_kilometer",
    "ontime_nav",
    "ontime",
    "tat_breach_hrs",
    "tat_risk_status",
    "hrs_elapsed_so_far",
    "total_stoppage_time",
    "halt_start_date_time", "last_halt_date_time",

    # --- Odometer & fuel ---
    "kilometer_out", "kilometer_in",
    "diesel", "diesel_consumed", "disel_balance",
    "fuel_available_in_ltr", "diesel_issue_location",

    # --- Weights ---
    "gross_weight", "net_weight", "tare_weight",

    # --- Flags ---
    "trip_type", "type_of_movement", "logistic_operation",
    "trip_close", "trip_clossing_type", "status",

    # --- Computed trip metrics ---
    "total_kms", "fuel_efficiency_km_l",
    "yard_to_loading_hrs", "loading_time_hrs", "transit_time_hrs",
    "unloading_time_hrs", "return_time_hrs", "total_trip_duration_hrs",
    "journey_time_hrs", "idle_time_hrs",

    # --- Route definitions ---
    "defined_tat_hrs", "defined_tat_ms", "defined_diesel_quantity",
    "defined_trip_allowance", "defined_management_fees", "defined_diesel_location",

    # misc
    "tat_breach_reason",
    "way_bill_no", "heavy_equipment_code",
    "linked_trip_id", "linked_mrn_no",
    "linked_sales_shipment_no",
    "no_", "holding_reason",

    # --- Audit ---
    "last_data_processed_timestamp",
    
])

# -----------------------------------------------------------------------------
# 15. FULL OVERWRITE INTO DELTA TABLE
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
# 16. QUICK VALIDATION
# -----------------------------------------------------------------------------
print("\n--- Ontime Distribution (this run) ---")
final_df.groupBy("ontime", "tat_risk_status").count().orderBy("ontime").show()

print("\n--- Sample: trips AT RISK or BREACHED ---")
final_df.filter(
    F.col("tat_risk_status").isin("AT RISK", "BREACHED")
).select(
    "trip_no", "vehicle_no", "route_id",
    "hrs_elapsed_so_far", "defined_tat_hrs",
    "tat_breach_hrs", "tat_risk_status",
    "last_data_processed_timestamp"
).orderBy(F.col("tat_breach_hrs").desc()).show(20, truncate=False)

# -----------------------------------------------------------------------------
# 17. DONE
# -----------------------------------------------------------------------------
print(f"\nRun ID                        : {run_id}")
print(f"Table                         : {TABLE_NAME}")
print(f"Pipeline capture time (UTC)   : {pipeline_now_iso}")
print(f"Pipeline capture time (ms)    : {pipeline_now_ms}")
print("Active trips load completed successfully.")

spark.stop()