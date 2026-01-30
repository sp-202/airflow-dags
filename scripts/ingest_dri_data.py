from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, abs as spark_abs

def main():
    spark = SparkSession.builder \
        .appName("DRI-Ingestion") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    jdbc_url = "jdbc:sqlserver://172.30.1.42:1433;databaseName=ANRML_01-12-2025_AH;encrypt=true;trustServerCertificate=true"
    props = {
        "user": "sa",
        "password": "Welcome1",
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    }

    # 1. Read Ledger Entries
    ledger_df = spark.read.jdbc(url=jdbc_url, properties=props, table="""
        (SELECT [Item No_] AS item_no, [Quantity], [Entry Type], [Global Dimension 1 Code] AS department 
         FROM [dbo].[ANRML$Item Ledger Entry] WHERE [Global Dimension 1 Code] = 'DRI') t
    """)

    # 2. Read Item Master
    item_df = spark.read.jdbc(url=jdbc_url, properties=props, table="(SELECT [No_] AS item_no, [Description] FROM [dbo].[ANRML$Item]) t")

    # 3. Transform
    final_df = ledger_df.join(item_df, on="item_no", how="left") \
        .withColumn("quantity", spark_abs(col("quantity")) / 1000) \
        .withColumn("product_name", 
            when(col("Description").contains("Lump"), "DRI Lumps")
            .when(col("Description").contains("Fines"), "DRI Fines")
            .otherwise(col("Description")))

    # 4. Write to S3 (MinIO)
    final_df.write.format("delta").mode("overwrite").save("s3a://dri-data/bronze/dri_output")

if __name__ == "__main__":
    main()