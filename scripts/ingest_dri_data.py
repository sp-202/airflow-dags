from pyspark.sql import SparkSession
from pyspark.sql.functions import rand, randn
import time

def generate_random_data():
    # Initialize Spark Session
    # No hardcoded configs here - relies on spark-submit defaults or k8s injection
    spark = SparkSession.builder \
        .appName("RandomDeltaGen") \
        .getOrCreate()

    print("Generating 10 million rows of random data...")
    
    # Generate 10 Million rows using range and random functions
    # Using repartition to ensure parallelism
    df = spark.range(0, 10000000).withColumn("v1", rand()).withColumn("v2", randn())
    
    start_time = time.time()
    
    # Define S3 path (Delta Lake)
    output_path = "s3a://test-bucket/random_data"
    
    print(f"Writing data to {output_path} in Delta format...")
    
    # Write to Delta Lake
    df.write.format("delta").mode("overwrite").save(output_path)
    
    end_time = time.time()
    print(f"Write complete in {end_time - start_time:.2f} seconds.")
    
    spark.stop()

if __name__ == "__main__":
    generate_random_data()