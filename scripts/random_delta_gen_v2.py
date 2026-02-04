from pyspark.sql import SparkSession
from pyspark.sql.functions import rand, randn, expr

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("RandomDeltaGen") \
    .enableHiveSupport() \
    .getOrCreate()

# 1. Generate DataFrame
df = spark.range(0, 1000) \
    .withColumn("random_value", rand(seed=42)) \
    .withColumn("normal_dist", randn(seed=42)) \
    .withColumn("category", expr("case when random_value > 0.5 then 'A' else 'B' end"))

# 3. Define Table Name and S3 Location
table_name = "default.random_delta_table"  # Format: database.table_name
# Note: Ensure the S3 bucket exists. The endpoint is configured in SparkConf.
s3_path = "s3a://test-bucket/delta-test" # Changed s3:// to s3a:// for Hadoop S3A file system


# 4. Write to Delta and register in HMS
print(f"Writing to {s3_path} and registering table {table_name}")

# Step A: Write the Delta files to MinIO
# This handles the data and the Delta Transaction Log (_delta_log)
df.write.format("delta") \
    .mode("overwrite") \
    .save(s3_path)

# Step B: Register the table in Hive Metastore using SQL
# This tells Hive: "There is a Delta table here, please use the schema found in the files"
spark.sql(f"DROP TABLE IF EXISTS {table_name}") # Optional: Ensures a clean metadata state
spark.sql(f"CREATE TABLE {table_name} USING DELTA LOCATION '{s3_path}'")

print(f"Table '{table_name}' registered in HMS at {s3_path}")

spark.stop()