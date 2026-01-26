from pyspark.sql import SparkSession
from src.utils.spark import get_spark_session

spark = get_spark_session("CheckGithub")
path = "s3a://raw-data/github_events"

try:
    df = spark.read.format("delta").load(path)
    print(f"Tổng số bản ghi GitHub: {df.count()}")
    
    df.select("id", "type", "actor.display_login", "repo.name", "_ingested_at") \
      .orderBy("_ingested_at", ascending=False) \
      .show(10, truncate=False)
except Exception as e:
    print(f"Lỗi khi đọc dữ liệu: {e}")
finally:
    spark.stop()