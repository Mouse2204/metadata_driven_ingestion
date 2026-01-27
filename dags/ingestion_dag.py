import os
import json
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime, timedelta

CONFIG_DIR = "/opt/airflow/configs"

def create_dag(dag_id, schedule, default_args, config_file, catchup=False):
    dag = DAG(
        dag_id=dag_id,
        schedule_interval=schedule,
        default_args=default_args,
        catchup=catchup,
        start_date=default_args.get('start_date', datetime(2025, 1, 1)),
        tags=['BecaData', 'Ingestion']
    )
    with dag:
        DockerOperator(
            task_id=f"run_{dag_id}",
            image="data-ingestion-framework-ingestion_app",
            api_version='auto',
            auto_remove=True,
            mount_tmp_dir=False,
            command=f"python -m src.main --config configs/{config_file}",
            network_mode="data-ingestion-framework_default",
            environment={
                "DB_PASSWORD": "{{ var.value.db_password }}",
                "REDDIT_TOKEN": "{{ var.value.reddit_token }}",
                "GITHUB_TOKEN": "{{ var.value.github_token }}",
                "WASTE_WATER_TOKEN": "{{ var.value.waste_water_token }}",
                "AWS_ACCESS_KEY_ID": "minioadmin",
                "AWS_SECRET_ACCESS_KEY": "minioadmin"
            }
        )
    return dag

if os.path.exists(CONFIG_DIR):
    for file_name in os.listdir(CONFIG_DIR):
        if file_name.endswith(".json"):
            with open(os.path.join(CONFIG_DIR, file_name), "r") as f:
                config = json.load(f)
                dag_params = config.get("dag_params", {})
                dag_id = f"ingestion_{config.get('job_name')}"
                default_args = {
                    "owner": dag_params.get("owner", "data_engine"),
                    "retries": dag_params.get("retries", 1),
                    "retry_delay": timedelta(minutes=dag_params.get("retry_delay_min", 5)),
                    "start_date": datetime.strptime(dag_params.get("start_date", "2025-01-01"), "%Y-%m-%d")
                }
                globals()[dag_id] = create_dag(dag_id, dag_params.get("schedule_interval"), default_args, file_name, dag_params.get("catchup", False))