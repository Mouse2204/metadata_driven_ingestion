import pandas as pd
import numpy as np
import os

def generate_data(rows, filename):
    data = {
        "id": np.arange(rows),
        "product": [f"Product_{i%50}" for i in range(rows)],
        "price": np.random.uniform(10, 500, size=rows),
        "category": [f"Category_{i%10}" for i in range(rows)],
        "timestamp": pd.date_range(start="2025-01-01", periods=rows, freq="S").strftime("%Y-%m-%d %H:%M:%S")
    }
    df = pd.DataFrame(data)
    os.makedirs("sftp_data", exist_ok=True)
    df.to_csv(f"sftp_data/{filename}", index=False)
    print(f"-> Đã tạo {filename}: {rows} dòng.")

if __name__ == "__main__":
    for size in [1000, 100000, 1000000]:
        generate_data(size, f"bench_{size}.csv")