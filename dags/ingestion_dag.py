import os
import json
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

CONFIG_PATH = "/opt/airflow/configs"

def create_dag(dag_id, config_filename):
    default_args = {
        "owner": "mouse",
        "start_date": datetime(2025, 1, 1),
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    }
    schedule_mapping = {
        "mongo_logs.json": "0 * * * *",
        "postgres.json": "20 * * * *",
        "sftp.json": "40 * * * *",
    }

    schedule = schedule_mapping.get(config_filename, "@daily")

    dag = DAG(
        dag_id=dag_id,
        default_args=default_args,
        schedule_interval=schedule,
        catchup=False,
        tags=["metadata-driven"]
    )

    with dag:
        path_mapping = {
            "mongo_logs.json": "mongo_orders",
            "sftp.json": "sftp_csv_data",
            "postgres.json": "users_delta_table"
        }
        folder_name = path_mapping.get(config_filename, config_filename.replace(".json", ""))
        target_path = f"s3a://raw-data/{folder_name}"

        ingest_task = BashOperator(
            task_id="run_spark_ingestion",
            bash_command=(
                f"docker exec my_ingestion_app python -m src.main "
                f"--config configs/{config_filename}"
            )
        )

        validate_task = BashOperator(
            task_id="validate_data_count",
            bash_command=(
                f"docker exec my_ingestion_app spark-submit "
                f"--driver-memory 512m "
                f"--executor-memory 512m " 
                f"--jars /app/deps/delta-spark_2.12-3.0.0.jar,"
                f"/app/deps/delta-storage-3.0.0.jar,"
                f"/app/deps/hadoop-aws-3.3.4.jar,"
                f"/app/deps/aws-java-sdk-bundle-1.12.262.jar "
                f"--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension "
                f"--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog "
                f"--conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 "
                f"--conf spark.hadoop.fs.s3a.access.key=minioadmin "
                f"--conf spark.hadoop.fs.s3a.secret.key=minioadmin "
                f"--conf spark.hadoop.fs.s3a.path.style.access=true "
                f"--conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem "
                f"/app/tests/check.py {target_path}"
            )
        )

        ingest_task >> validate_task
        
    return dag

if os.path.exists(CONFIG_PATH):
    for file in os.listdir(CONFIG_PATH):
        if file.endswith(".json"):
            dag_name = f"ingestion_{file.split('.')[0]}"
            globals()[dag_name] = create_dag(dag_name, file)