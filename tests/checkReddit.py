from src.utils.spark import get_spark_session
import pyspark.sql.functions as F

spark = get_spark_session("CheckReddit")
path = "s3a://raw-data/reddit_posts" #

df = spark.read.format("delta").load(path)

print(f"Tổng số bản ghi Reddit: {df.count()}")
# Reddit data lồng trong cột 'data', chúng ta sẽ lấy vài trường tiêu biểu
df.select(
    F.col("data.title").alias("title"),
    F.col("data.author").alias("author"),
    "_ingested_at"
).show(10, truncate=False)

spark.stop()