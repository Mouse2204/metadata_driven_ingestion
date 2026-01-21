from pyspark.sql import functions as F
from src.utils.spark import get_spark_session 
spark = get_spark_session("PerformanceEvaluation")
audit_path = "s3a://delta-lake/ingestion_audit_logs"

print("\n" + "="*50)
print("BÁO CÁO HIỆU NĂNG NẠP DỮ LIỆU (AUDIT LOGS)")
print("="*50)

try:
    df = spark.read.format("delta").load(audit_path)
    
    performance_df = df.withColumn("speed_rec_per_sec", F.round(F.col("source_count") / F.col("duration"), 2)) \
                       .select(
                           "ts", 
                           "job_name", 
                           "status", 
                           "source_count", 
                           "duration", 
                           "speed_rec_per_sec"
                       ) \
                       .orderBy(F.col("ts").desc())
    
    performance_df.show(truncate=False)

except Exception as e:
    print(f"Chưa có dữ liệu Audit Log hoặc lỗi truy cập: {e}")
finally:
    spark.stop()