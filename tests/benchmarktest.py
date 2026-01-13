import json
import subprocess
import time
import os

SCENARIOS = [
    {"size": 1000, "method": "paramiko"},
    {"size": 1000, "method": "addFile"},
    {"size": 100000, "method": "paramiko"},
    {"size": 100000, "method": "addFile"},
    {"size": 1000000, "method": "paramiko"},
    {"size": 1000000, "method": "addFile"}
]

RESULTS = []

def run_test(size, method):
    config_name = f"bench_{size}_{method}.json"
    config_path = f"configs/{config_name}"
    
    file_path = f"upload/bench_{size}.csv" if method == "paramiko" else f"sftp_data/bench_{size}.csv"
    
    config_data = {
        "job_name": f"bench_{size}_{method}",
        "source_type": "sftp",
        "ingest_method": method,
        "ingestion_type": "full",
        "source_config": {
            "host": "sftp_server", "port": 22, "username": "user", "password": "pass",
            "file_path": file_path
        },
        "target": {
            "format": "delta", "path": f"s3a://raw-data/bench_{size}_{method}", "primary_key": "id"
        }
    }
    
    with open(config_path, "w") as f:
        json.dump(config_data, f, indent=2)

    print(f"\n>>> Đang chạy: Quy mô={size}, Phương thức={method}...")
    start_time = time.time()
    
    cmd = ["docker", "compose", "run", "--rm", "ingestion_app", "python", "-m", "src.main", "--config", f"configs/{config_name}"]
    process = subprocess.run(cmd, capture_output=True, text=True)
    
    duration = time.time() - start_time
    
    if process.returncode == 0:
        print(f"--- Thành công: {duration:.2f} giây ---")
        return duration
    else:
        print(f"--- Thất bại ---")
        print(f"Lỗi: {process.stderr}")
        return None

if __name__ == "__main__":
    for sc in SCENARIOS:
        duration = run_test(sc["size"], sc["method"])
        if duration:
            RESULTS.append({"Rows": sc["size"], "Method": sc["method"], "Time": round(duration, 2)})

    print("\n" + "="*50)
    print("BÁO CÁO BENCHMARK HIỆU NĂNG (MIND PATTERN)")
    print("="*50)
    print(f"{'Quy mô (Rows)':<15} | {'Phương thức':<15} | {'Thời gian (s)':<15}")
    print("-" * 50)
    for res in RESULTS:
        print(f"{res['Rows']:<15} | {res['Method']:<15} | {res['Time']:<15}")