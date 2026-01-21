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
    except:
        pass
    config = load_config(args.config)
    spark = get_spark_session(config.get("job_name", "IngestionJob"))
    connector = None
    try:
        connector = ConnectorFactory.get_connector(spark, config)
        connector.execute() 
    except Exception as e:
        sys.exit(1)
    finally:
        if connector:
            connector.cleanup()
        if spark:
            spark.stop()

if __name__ == "__main__":
    main()