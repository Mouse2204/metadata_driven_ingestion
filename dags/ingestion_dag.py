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
        tags=['MIND', 'Ingestion']
    )

    with dag:
        run_ingestion = DockerOperator(
            task_id=f"run_{dag_id}",
            image="data-ingestion-framework-ingestion_app",
            api_version='auto',
            auto_remove=True,
            command=f"python -m src.main --config configs/{config_file}",
            docker_url="unix://var/run/docker.sock",
            network_mode="data-ingestion-framework_default",
            environment={
                "DB_PASSWORD": "{{ var.value.db_password }}",
                "AWS_ACCESS_KEY_ID": "minioadmin",
                "AWS_SECRET_ACCESS_KEY": "minioadmin"
            }
        )
    return dag

if os.path.exists(CONFIG_DIR):
    for file_name in os.listdir(CONFIG_DIR):
        if file_name.endswith(".json"):
            file_path = os.path.join(CONFIG_DIR, file_name)
            
            with open(file_path, "r") as f:
                config = json.load(f)
                
                job_name = config.get("job_name")
                dag_params = config.get("dag_params", {})
                
                dag_id = f"ingestion_{job_name}"
                
                schedule = dag_params.get("schedule_interval")
                catchup = dag_params.get("catchup", False)
                
                default_args = {
                    "owner": dag_params.get("owner", "data_engine"),
                    "retries": dag_params.get("retries", 1),
                    "retry_delay": timedelta(minutes=dag_params.get("retry_delay_min", 5)),
                    "start_date": datetime.strptime(dag_params.get("start_date", "2025-01-01"), "%Y-%m-%d")
                }
                globals()[dag_id] = create_dag(dag_id, schedule, default_args, file_name, catchup)