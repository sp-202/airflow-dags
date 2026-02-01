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
# Using 'path' inside saveAsTable links the HMS metadata to your S3 location
print(f"Writing to {s3_path} and registering table {table_name}")

df.write.format("delta") \
    .mode("overwrite") \
    .option("path", s3_path) \
    .saveAsTable(table_name)

print(f"Table '{table_name}' registered in HMS at {s3_path}")

spark.stop()
