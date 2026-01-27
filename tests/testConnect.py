import subprocess
import platform

def network_diagnostic(host):
    system_pf = platform.system().lower()
    # Chọn lệnh tương ứng với hệ điều hành
    cmd = ["tracert", "-d", host] if system_pf == "windows" else ["traceroute", "-n", host]
    
    print(f"--- Đang bắt đầu Trace Route tới {host} ---")
    try:
        # Chạy lệnh và in trực tiếp kết quả
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        for line in process.stdout:
            print(line, end="")
        process.wait()
    except Exception as e:
        print(f"Lỗi khi thực thi: {e}")

if __name__ == "__main__":
    # Địa chỉ IP đang gặp lỗi Timeout
    TARGET_IP = "10.10.101.251" 
    network_diagnostic(TARGET_IP)