import psycopg2
from pymongo import MongoClient
import os
import time
import random
from datetime import datetime

# Cấu hình kết nối
PG_CONFIG = "host=localhost port=5432 dbname=postgres user=postgres password=mysecretpassword"
MONGO_URI = "mongodb://admin:password123@localhost:27017/"

def seed_step():
    now = datetime.now()
    timestamp_str = now.strftime("%Y-%m-%d %H:%M:%S")

    # 1. SEED POSTGRES (Thêm User mới)
    try:
        conn = psycopg2.connect(PG_CONFIG)
        cur = conn.cursor()
        cur.execute("CREATE TABLE IF NOT EXISTS users (id SERIAL PRIMARY KEY, name TEXT, email TEXT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);")
        name = f"User_{random.randint(1000, 9999)}"
        cur.execute("INSERT INTO users (name, email) VALUES (%s, %s);", (name, f"{name.lower()}@example.com"))
        conn.commit()
        print(f"[{timestamp_str}] Postgres: Inserted {name}")
        cur.close()
        conn.close()
    except Exception as e:
        print(f"Postgres Error: {e}")

    # 2. SEED MONGODB (Thêm Order mới)
    try:
        client = MongoClient(MONGO_URI)
        db = client["sales_db"]
        col = db["orders"]
        order = {
            "order_id": str(random.randint(10000, 99999)),
            "amount": round(random.uniform(10.0, 1000.0), 2),
            "status": random.choice(["delivered", "pending", "shipped"]),
            "updated_at": now
        }
        col.insert_one(order)
        print(f"[{timestamp_str}] Mongo: Inserted {order['order_id']}")
        client.close()
    except Exception as e:
        print(f"Mongo Error: {e}")

    # 3. SEED SFTP (Ghi đè file với dữ liệu mới hoặc Append)
    try:
        file_path = "sftp_data/data_sample.csv"
        file_exists = os.path.isfile(file_path)
        with open(file_path, "a") as f:
            if not file_exists:
                f.write("id,product,price,timestamp\n")
            f.write(f"{random.randint(1, 1000)},Product_{random.randint(1, 50)},{random.randint(10, 500)},{timestamp_str}\n")
        print(f"[{timestamp_str}] SFTP: Updated data_sample.csv")
    except Exception as e:
        print(f"SFTP Error: {e}")

if __name__ == "__main__":
    print("Data Generator started. Press Ctrl+C to stop.")
    while True:
        seed_step()
        print("--- Waiting 5 seconds ---")
        time.sleep(5)