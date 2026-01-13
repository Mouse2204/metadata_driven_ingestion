import sys
import argparse
from src.connectors.factory import ConnectorFactory
from src.utils.config_loader import load_config
from src.utils.MinIOBucket import init_minio_buckets
from src.utils.spark import get_spark_session
from src.utils.logging import get_logger
logger = get_logger("MainApp")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    args = parser.parse_args()

    try:
        init_minio_buckets()
    except Exception as e:
        print(f"Warning: Storage init failed: {e}")

    config = load_config(args.config)
    job_name = config.get("job_name", "IngestionJob")
    spark = get_spark_session(job_name)

    try:
        connector = ConnectorFactory.get_connector(spark, config)
        df = connector.read()
        
        target_conf = config.get("target", {})
        primary_key = target_conf.get("primary_key")
        
        connector.write(df, primary_key=primary_key)
        
        print("-> Job Finished Successfully!")
    except Exception as e:
        print(f"Job Failed: {e}")
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()