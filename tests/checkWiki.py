from src.utils.spark import get_spark_session
import pyspark.sql.functions as F

spark = get_spark_session("CheckData")
path = "s3a://raw-data/wikipedia_search"

print("--- Đang đọc dữ liệu từ Delta Lake ---")
df = spark.read.format("delta").load(path)

df.printSchema()

print(f"Tổng số bản ghi: {df.count()}")
df.select("pageid", "title", "_ingested_at", "_source_name").show(10, truncate=False)

spark.stop()