from src.utils.spark import get_spark_session
import pyspark.sql.functions as F

spark = get_spark_session("CheckRedditLatest")
path = "s3a://raw-data/reddit_posts" 

df = spark.read.format("delta").load(path)

print(f"Tổng số bản ghi Reddit hiện có: {df.count()}")

print("--- 10 bản ghi mới nhất vừa được nạp ---")
df.select(
    F.col("data.title").alias("title"),
    F.col("data.author").alias("author"),
    "_ingested_at",
    "_ingestion_id"
).orderBy(F.col("_ingested_at").desc()) \
 .show(10, truncate=False)

spark.stop()