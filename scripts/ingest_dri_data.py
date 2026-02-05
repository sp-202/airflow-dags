import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, abs as spark_abs

# ------------------------------------------------------------------------------
# 1. CREDENTIALS RECONSTRUCTION (From K8s Secrets)
# ------------------------------------------------------------------------------
db_host = os.getenv("DB_HOST")
db_port = os.getenv("DB_PORT", "1433")
db_name = os.getenv("DB_NAME")
db_user = os.getenv("DB_USER")
db_pass = os.getenv("DB_PASS")

# Build JDBC URL with modern security parameters for Spark 4.0
jdbc_url = (
    f"jdbc:sqlserver://{db_host}:{db_port};"
    f"databaseName={db_name};"
    "encrypt=true;"
    "trustServerCertificate=true;"
)

jdbc_props = {
    "user": db_user,
    "password": db_pass,
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    "fetchsize": "10000" # Optimized for bulk reads
}

# ------------------------------------------------------------------------------
# 2. SPARK SESSION INITIALIZATION
# ------------------------------------------------------------------------------
# Configured via the YAML's configMap, so we just getOrCreate here
spark = SparkSession.builder \
    .appName(f"DRI_Ingestion_{db_name}") \
    .enableHiveSupport() \
    .getOrCreate()

# ------------------------------------------------------------------------------
# 3. MSSQL DATA INGESTION (JDBC)
# ------------------------------------------------------------------------------

# Ledger Query: Filter at source (SQL Server) to minimize network traffic
ledger_query = """
    (
        SELECT 
            [Item No_] AS item_no,
            [Entry No_] AS entry_no,
            [Posting Date] AS posting_date,
            [Quantity] AS quantity,
            [Entry Type] AS entry_type,
            [Document Type] AS document_type,
            [Global Dimension 1 Code] AS department,
            [Global Dimension 2 Code] AS process_center_code
        FROM [dbo].[ANRML$Item Ledger Entry]
        WHERE [Global Dimension 1 Code] = 'DRI'
    ) AS ledger_table
"""

# Item Query: Basic master data
item_query = """
    (
        SELECT 
            [No_] AS item_no,
            [Description] AS item_description,
            [Base Unit of Measure] AS uom
        FROM [dbo].[ANRML$Item]
    ) AS item_table
"""

print("Reading data from MSSQL...")
ledger_df = spark.read.jdbc(url=jdbc_url, table=ledger_query, properties=jdbc_props)
item_df = spark.read.jdbc(url=jdbc_url, table=item_query, properties=jdbc_props)

# ------------------------------------------------------------------------------
# 4. TRANSFORMATION LOGIC
# ------------------------------------------------------------------------------

# Map Numeric Entry Types to readable Descriptions
ledger_df = ledger_df.withColumn(
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

# Join Ledger with Item Master
final_df = ledger_df.join(item_df, on="item_no", how="left")

# Filter for Production Output and Sales, Normalize Quantity to Metric Tons
dri_output = final_df.filter(col("entry_type_desc").isin("Output", "Sale")) \
    .withColumn("quantity", spark_abs(col("quantity")) / 1000) \
    .withColumn(
        "product_name",
        when(
            col("item_description").isin(
                "DRI Lump(A-Grade- Fe(M)-80%)",
                "DRI Lump(B-Grade-<80%)",
                "Iron-ore Lumps"
            ),
            "DRI Lumps"
        )
        .when(col("item_description") == "Iron-Ore Fines", "DRI Fines")
        .otherwise(col("item_description"))
    )

# ------------------------------------------------------------------------------
# 5. DELTA WRITE (Production Hardened)
# ------------------------------------------------------------------------------
s3_delta_path = "s3a://dri-data/bronze/dri_output"

print(f"Saving data to Delta path: {s3_delta_path}...")

# overwriteSchema: true -> Fixes the 'item_no' type mismatch by updating Hive
# mergeSchema: true -> Allows for future column additions automatically
(
    dri_output.write
    .format("delta")
    .mode("overwrite")
    .option("path", s3_delta_path)
    .option("overwriteSchema", "true") 
    .option("mergeSchema", "true")
    .saveAsTable("dri_db.dri_prod")
)

print("Ingestion Job Completed Successfully.")