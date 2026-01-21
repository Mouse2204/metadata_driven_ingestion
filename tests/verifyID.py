from pyspark.sql import functions as F
from src.utils.spark import get_spark_session

def verify_data_integrity():
    spark = get_spark_session("DataIntegrityCheck")
    path = "s3a://raw-data/reddit_posts"
    pk_col = "data_id"

    print("\n" + "="*60)
    print(f"KIỂM TRA TÍNH TOÀN VẸN DỮ LIỆU: {path}")
    print("="*60)

    try:
        df = spark.read.format("delta").load(path)
        total_count = df.count()
        
        duplicates = df.groupBy(pk_col).count().filter("count > 1")
        duplicate_count = duplicates.count()

        dup_rate = (duplicate_count / total_count) * 100 if total_count > 0 else 0

        print(f"[*] Tổng số bản ghi trong kho: {total_count}")
        print(f"[*] Số lượng khóa chính bị trùng: {duplicate_count}")
        print(f"[*] Tỷ lệ trùng lặp: {dup_rate:.2f}%")

        if duplicate_count == 0:
            print("\n[SUCCESS] Chúc mừng! Cơ chế MERGE hoạt động chính xác 100%.")
        else:
            print("\n[WARNING] Phát hiện dữ liệu trùng lặp. Cần kiểm tra lại logic MERGE.")
            print("Danh sách các ID bị trùng:")
            duplicates.show(10, truncate=False)

    except Exception as e:
        print(f"[ERROR] Không thể truy cập dữ liệu: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    verify_data_integrity()