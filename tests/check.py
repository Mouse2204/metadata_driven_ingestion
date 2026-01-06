import sys
import os
# Thêm đường dẫn app để import được src
sys.path.append('/app')

from src.utils.spark import get_spark_session

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python check.py <s3_path>")
        sys.exit(1)
    
    path = sys.argv[1]
    spark = get_spark_session("DataCheckJob")
    
    print(f"--- Reading Delta Table from: {path} ---")
    try:
        df = spark.read.format("delta").load(path)
        print(f"Total Records: {df.count()}")
        print("Sample Data:")
        df.select("pageid", "title", "ingested_at").show(5, truncate=False)
    except Exception as e:
        print(f"Error: {e}")
    finally:
        spark.stop()