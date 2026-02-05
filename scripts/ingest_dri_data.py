from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, abs as spark_abs

# ------------------------------------------------------------------------------
# Spark session
# ------------------------------------------------------------------------------
spark = SparkSession.builder.appName("DRI_Output_Processing").getOrCreate()

# ------------------------------------------------------------------------------
# JDBC configuration
# ------------------------------------------------------------------------------
jdbc_url = (
    "jdbc:sqlserver://172.30.1.64:1433;"
    "databaseName=ANRML;"
    "encrypt=true;"
    "trustServerCertificate=true"
)

jdbc_props = {
    "user": "datawarehouse",
    "password": "d@t@w@reh0use",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
}

# ------------------------------------------------------------------------------
# Fetch all base tables (for validation / debugging)
# ------------------------------------------------------------------------------
tables_df = spark.read.jdbc(
    url=jdbc_url,
    table="""
        (
            SELECT
                TABLE_SCHEMA,
                TABLE_NAME
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_TYPE = 'BASE TABLE'
        ) t
    """,
    properties=jdbc_props
)

tables_df.show(truncate=False)

# ------------------------------------------------------------------------------
# Item Ledger Entry (DRI only)
# ------------------------------------------------------------------------------
ledger_df = spark.read.jdbc(
    url=jdbc_url,
    table="""
        (
            SELECT 
                [Item No_]        AS item_no,
                [Entry No_]       AS entry_no,
                [Posting Date]    AS posting_date,
                [Quantity]        AS quantity,
                [Entry Type]      AS entry_type,
                [Document Type]   AS document_type,
                [Global Dimension 1 Code] AS department,
                [Global Dimension 2 Code] AS process_center_code
            FROM [dbo].[ANRML$Item Ledger Entry]
            WHERE [Global Dimension 1 Code] = 'DRI'
        ) t
    """,
    properties=jdbc_props
)

# Entry type mapping
ledger_df = ledger_df.withColumn(
    "entry_type_desc",
    when(col("entry_type") == 0, "Purchase")
    .when(col("entry_type") == 1, "Sale")
    .when(col("entry_type") == 2, "Positive Adjmt")
    .when(col("entry_type") == 3, "Negative Adjmt")
    .when(col("entry_type") == 4, "Transfer")
    .when(col("entry_type") == 5, "Consumption")
    .when(col("entry_type") == 6, "Output")
    .when(col("entry_type") == 7, "")
    .when(col("entry_type") == 8, "Assembly Consumption")
    .when(col("entry_type") == 9, "Assembly Output")
    .otherwise("Unknown")
)

ledger_df.show()

# ------------------------------------------------------------------------------
# Item master
# ------------------------------------------------------------------------------
item_df = spark.read.jdbc(
    url=jdbc_url,
    table="""
        (
            SELECT 
                [Base Unit of Measure] AS uom,
                [No_]                 AS item_no,
                [Description]         AS item_description
            FROM [dbo].[ANRML$Item]
        ) t
    """,
    properties=jdbc_props
)

item_df.show(truncate=False)

# ------------------------------------------------------------------------------
# Join ledger with item master
# ------------------------------------------------------------------------------
final_df = ledger_df.join(item_df, on="item_no", how="left")
final_df.show(truncate=False)

# ------------------------------------------------------------------------------
# Filter DRI Output & Sale
# ------------------------------------------------------------------------------
dri_output = final_df.filter(
    col("entry_type_desc").isin("Output", "Sale")
)

# ------------------------------------------------------------------------------
# Quantity normalization & product mapping
# ------------------------------------------------------------------------------
dri_output = (
    dri_output
    # make quantity positive and convert to MT
    .withColumn("quantity", spark_abs(col("quantity")) / 1000)
    # normalize product name
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
)

dri_output.show(truncate=False)

# ------------------------------------------------------------------------------
# Delta write to S3
# ------------------------------------------------------------------------------
s3_delta_path = "s3a://dri-data/bronze/dri_output"
s3_checkpoint_path = "s3a://dri-data/checkpoint/dri_output"

(
    dri_output
    .write
    .mode("overwrite")
    .format("delta")
    .option("path", s3_delta_path)
    .option("mergeSchema", "true")
    .option("checkpointLocation", s3_checkpoint_path)
    .saveAsTable("dri_db.dri_prod")
)
