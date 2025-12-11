from pyspark.sql import SparkSession
import argparse

spark = (
    SparkSession.builder
    .appName("spark-minio-test")
    .getOrCreate()
)

parser = argparse.ArgumentParser()
parser.add_argument("--name", required=False, default="test_run")
args = parser.parse_args()

print("======================================")
print(" SPARK JOB TEST STARTED ")
print(f" Name argument: {args.name}")
print("======================================")

df = spark.createDataFrame(
    [(1, "Alice"), (2, "Bob"), (3, "Charlie")],
    ["id", "name"]
)

df.show()

spark.stop()
