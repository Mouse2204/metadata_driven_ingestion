Bản Demo thứ 3:
||Metadata Ingestion Framework||

#Tính năng nổi bật:
- Kiến trúc Modular: Thêm nguồn dữ liệu mới chỉ bằng cách thêm một file trong thư mục connectors/ mà không cần sửa đổi mã nguồn cốt lõi
- Factory and Registry Pattern: Tự động phát hiện và đăng ký các Connector(Hiện tại là Mongo, File CSV, Database PostgreSQL) thông qua Decorators, loại bỏ hoàn toàn các khối lệnh if/else.
- Metadata Driven: Toàn bộ quy trình nạp dữ liệu (Nguồn, đích, định dạng, phân vùng) được điều khiển thông qua file cấu hình JSON.
- Đa dạng kết nối: Hỗ trợ nạp dữ liệu từ SQL(PostgreSQL), NoSQL(MongoDB), và FIles (SFTP/CSV/Excel).
- Chống trùng lặp dữ liệu (Idempotency): Tích hợp logic Delta Lake Merge (Upsert) và dropDuplicates tại nguồn để đảm bảo dữ liệu trong kho luôn duy nhất ngay cả khi nạp lại nhiều lần.
- Tự động hóa thông minh: Tích hợp Airflow DAGs tự động sinh theo cấu hình, hỗ trợ chạy lệch giờ (Staggered Scheduling) để tối ưu hóa tài nguyên RAM.
- Xử lý Offline: Sử dụng bộ thư viện JAR nội bộ giúp hệ thống khởi động Spark cực nhanh và không phụ thuộc vào Internet.

#Kiến trúc hệ thống:
1.JobRunner(main.py): Đóng vai trò điều phối, khởi tạo Spark và thực thi Job.
2.ConnectorFactory: Sử dụng Registry Pattern để quản lý danh sách các Connector khả dụng và khởi tạo chúng dựa trên source_type từ config
3.BaseConnector: Lớp cơ sở trừu tượng định nghĩa Interface chung cho tất cả các nguồn dữ liệu
4.Connectors: Các mô-đun cụ thể chịu trách nhiệm kết nối và trích xuất dữ liệu.

data-ingestion-framework/
├── configs/               # Chứa các file metadata JSON (postgres.json, mongo.json,...)
├── src/
│   ├── connectors/        # Chứa các mô-đun kết nối (Modular Connectors)
│   │   ├── base.py        # Interface chung cho tất cả connectors
│   │   ├── factory.py     # Bộ điều phối và tự động đăng ký (Dynamic Registry)
│   │   ├── jdbc.py        # Connector cho RDBMS (Postgres,...)
│   │   ├── mongo.py       # Connector cho MongoDB
│   │   └── file.py        # Connector cho SFTP (CSV, Excel,...)
│   ├── utils/             # Các tiện ích (Spark khởi tạo, Storage init,...)
│   └── main.py            # Điểm khởi đầu của ứng dụng (Entrypoint)
├── deps/                  # Chứa các thư viện JAR cho Spark (.jar)
├──                        # Script tạo dữ liệu giả và kiểm tra (check.py)
└── docker-compose.yml     # Thiết lập hạ tầng (Spark, MinIO, Postgres, Mongo, SFTP)

#Airflow - Tự động hóa với Airflow:
- Hệ thống tự động quét thư mục configs/ để sinh ra các DAG tương ứng.
- Lịch trình: Các Job được cấu hình chạy lệch giờ (phút 00, 20, 40) để tránh quá tải RAM.
- Giới hạn tài nguyên: Mỗi Job Spark được giới hạn --driver-memory 512m để đảm bảo tính ổn định.
- Validation: Sau mỗi lần nạp, task validate_data_count tự động kiểm tra số lượng bản ghi và hiển thị dữ liệu mẫu trong Log.

#Quy trình hệ thống:
1. Khởi tạo:
- Load Metadata: Ứng dụng đọc file cấu hình JSON từ tham số config
- Spark SessionL Khởi tạo SparkSession tập trung với cấu hình JARs và S3(Bridge to MinIO)
- Storage Setup: Tự động kiểm tra và tạo các Buckets cần thiết trên MinIO(raw, delta, processed).
2. Factory và Discovery:
- Auto Discovery: ConnectorFactory tự động quét thư mục connectors/ để nhận diện các mô-đun khả dụng
- Self-Registry: Các Connector sử dụng Decorators @register để tự đưa mình vào "Sổ đăng ký"
- Connector Selection: Dựa trên source_type trong file JSON, Factory trả về đúng đối tượng cần thiết mà không dùng If/else
3. Trích xuất - Extraction:
- Hệ thống gọi phương thức .read() chuẩn hóa connector.
- SFPT/File: Tải file về vùng đệm /tmp, Spark thực hiện đọc dữ liệu (Lazy Evaluation)
- DB/NoSQL: Thiết lập kết nối JDBC hoặc Mongo Driver để kéo dữ liệu vào Spark DataFrame.
4. Lưu trữ:
- Dữ liệu được chuẩn hóa thành DataFrame
- Write to Delta: Ghi dữ liệu xuống MinIO định dạng Delta(Parquet + ACID) với các tùy chọn mode(append/overwrite) và partitionBy được định nghĩa trong Metadata

#Hướng dẫn sử dụng:
1.Khởi động: docker compose up -d
2.Build: docker compose build ingestion_app
-----------------------------------------------------
3.mkdir -p deps && cd deps

# MongoDB Spark Connector & Drivers (Spark 3.5+ Compatibility)
wget https://repo1.maven.org/maven2/org/mongodb/spark/mongo-spark-connector_2.12/10.4.0/mongo-spark-connector_2.12-10.4.0.jar
wget https://repo1.maven.org/maven2/org/mongodb/bson/4.11.1/bson-4.11.1.jar
wget https://repo1.maven.org/maven2/org/mongodb/mongodb-driver-core/4.11.1/mongodb-driver-core-4.11.1.jar
wget https://repo1.maven.org/maven2/org/mongodb/mongodb-driver-sync/4.11.1/mongodb-driver-sync-4.11.1.jar

# PostgreSQL Driver
wget https://jdbc.postgresql.org/download/postgresql-42.7.2.jar

# Delta Lake Core
wget https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.0.0/delta-spark_2.12-3.0.0.jar
wget https://repo1.maven.org/maven2/io/delta/delta-storage/3.0.0/delta-storage-3.0.0.jar

# S3A / Hadoop AWS
wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar
wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar
-----------------------------------------------------
4.Nạp dữ liệu từ MongoDB: docker compose run --rm ingestion_app python -m src.main --config configs/mongo_logs.json
5.Nạp dữ liệu từ SFTP: docker compose run --rm ingestion_app python -m src.main --config configs/sftp.json
6.Nạp dữ liệu từ PostgreSQL: docker compose run --rm ingestion_app python -m src.main --config configs/postgres.json

#Cách thêm một nguồn dữ liệu mới - Mở rộng(Modular Extension):
- Để thêm một nguồn dữ liệu mới(Ví dụ như Google Cloud Storage), bạn chỉ cần thực hiện 1 bước duy nhất mà không cần sửa code cũ:
Bước 1: Tạo file src/connectors/ggCS.py
Bước 2: Read source
@ConnectorFactory.register("gcs") 
class GcsConnector(BaseConnector): 
    def read(self):  
        return self.spark.read.format("parquet").load(self.source_config.get("path"))
=> Hệ thống sẽ tự động phát hiện và sẵn sàng thực thi khi bạn cung cấp file config có “source_type”:”gcs”.