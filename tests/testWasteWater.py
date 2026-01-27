from src.utils.spark import get_spark_session
from pyspark.sql import functions as F

spark = get_spark_session("CheckWasteWater")

target_path = "s3a://raw-data/waste_water_events_v2"
df = spark.read.format("delta").load(target_path)

print(f"Tổng số bản ghi đã nạp: {df.count()}")

df.select(
    "id", 
    "plate", 
    "eventType", 
    "dateTime", 
    "pumpStation.name", 
    "_ingest_date"
).show(10, truncate=False)

df.groupBy("eventType").count().show()