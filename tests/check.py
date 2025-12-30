import sys
from pyspark.sql import SparkSession

if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit(1)

    path = sys.argv[1]

    spark = SparkSession.builder \
        .appName("Data Validation") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    try:
        df = spark.read.format("delta").load(path)
        row_count = df.count()
        
        print("-" * 50)
        print("VALIDATION SUCCESS")
        print(f"Path: {path}")
        print(f"Total rows: {row_count}")
        print("-" * 50)
        df.show(5)
        
    except Exception as e:
        print(f"Validation failed: {e}")
        sys.exit(1)