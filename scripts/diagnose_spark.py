from pyspark.sql import SparkSession
import sys

spark = SparkSession.builder.getOrCreate()
conf = spark._jsc.hadoopConfiguration()
iterator = conf.iterator()

culprits = []
while iterator.hasNext():
    prop = iterator.next()
    val = str(prop.getValue())
    if val.endswith("s") or val.endswith("m") or val.endswith("h") or val.endswith("d"):
        # Check if it looks like a duration, e.g., "60s"
        if any(char.isdigit() for char in val):
            culprits.append((prop.getKey(), val))

print("--- Culprit Hadoop Properties ---")
for k, v in culprits:
    print(f"{k} = {v}")
print("--- End ---")

spark.stop()
