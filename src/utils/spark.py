import os
from pyspark.sql import SparkSession

def get_spark_session(job_name: str):
    jars = [
        "/app/deps/postgresql-42.7.2.jar",
        "/app/deps/delta-spark_2.12-3.1.0.jar",
        "/app/deps/delta-storage-3.1.0.jar",
        "/app/deps/hadoop-aws-3.3.4.jar",
        "/app/deps/aws-java-sdk-bundle-1.12.262.jar",
        "/app/deps/mongo-spark-connector_2.12-10.4.0.jar",
        "/app/deps/bson-4.11.1.jar",
        "/app/deps/mongodb-driver-core-4.11.1.jar",
        "/app/deps/mongodb-driver-sync-4.11.1.jar"
    ]

    jars_str = ",".join(jars)
    classpath_str = ":".join(jars)
    
    return SparkSession.builder \
        .appName(job_name) \
        .config("spark.jars", jars_str) \
        .config("spark.driver.extraClassPath", classpath_str) \
        .config("spark.executor.extraClassPath", classpath_str) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("S3_ENDPOINT", "http://minio:9000")) \
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")) \
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60") \
        .config("spark.hadoop.fs.s3a.metadata.ttl", "86400000") \
        .config("spark.hadoop.fs.s3a.connection.ttl", "86400000") \
        .config("spark.hadoop.fs.s3a.metadatastore.authoritative.ttl", "86400000") \
        .config("spark.hadoop.fs.s3a.metadatastore.impl", "org.apache.hadoop.fs.s3a.s3guard.NullMetadataStore") \
        .master("local[1]") \
        .getOrCreate()