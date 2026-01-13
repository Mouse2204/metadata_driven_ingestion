import json
import subprocess
import time
import os

SCENARIOS = [
    {
        "name": "1M_Full_Ingestion",
        "ingestion_type": "full",
        "last_val": "0"
    },
    {
        "name": "1M_Incremental_Ingestion_40_Percent",
        "ingestion_type": "incremental",
        "last_val": "599999"
    }
]

RESULTS = []

def run_mind_job(scenario):
    config_name = f"mind_{scenario['name']}.json"
    config_path = f"configs/{config_name}"
    
    config_data = {
        "job_name": scenario['name'],
        "source_type": "sftp",
        "ingest_method": "paramiko",
        "ingestion_type": scenario['ingestion_type'],
        "incremental_column": "id",
        "last_ingestion_value": scenario['last_val'],
        "source_config": {
            "host": "sftp_server",
            "port": 22,
            "username": "user",
            "password": "pass",
            "file_path": "upload/bench_1000000.csv"
        },
        "target": {
            "format": "delta",
            "path": f"s3a://raw-data/{scenario['name']}",
            "primary_key": "id"
        }
    }
    
    with open(config_path, "w") as f:
        json.dump(config_data, f, indent=2)

    print(f"\n>>> Đang thực thi: {scenario['name']}...")
    start_time = time.time()
    
    cmd = ["docker", "compose", "run", "--rm", "ingestion_app", "python", "-m", "src.main", "--config", f"configs/{config_name}"]
    process = subprocess.run(cmd, capture_output=True, text=True)
    
    duration = time.time() - start_time
    
    if process.returncode == 0:
        print(f"--- Hoàn thành: {duration:.2f} giây ---")
        return duration
    else:
        print(f"--- Lỗi khi thực thi: {process.stderr}")
        return None

if __name__ == "__main__":
    for sc in SCENARIOS:
        exec_time = run_mind_job(sc)
        if exec_time:
            RESULTS.append({"Scenario": sc['name'], "Time": exec_time})

    if len(RESULTS) == 2:
        full_time = RESULTS[0]['Time']
        inc_time = RESULTS[1]['Time']
        saved_time = full_time - inc_time
        percentage = (saved_time / full_time) * 100

        print("\n" + "="*60)
        print("KẾT QUẢ SO SÁNH HIỆU NĂNG MIND (FULL VS INCREMENTAL)")
        print("="*60)
        print(f"{'Kịch bản':<40} | {'Thời gian (s)':<15}")
        print("-" * 60)
        for res in RESULTS:
            print(f"{res['Scenario']:<40} | {res['Time']:<15.2f}")
        print("-" * 60)
        print(f"Thời gian tiết kiệm được: {saved_time:.2f} giây")
        print(f"Tỷ lệ cải thiện hiệu năng: {percentage:.2f}%")
        print("="*60)