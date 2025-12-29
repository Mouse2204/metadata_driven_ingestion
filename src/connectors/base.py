from abc import ABC, abstractmethod
from pyspark.sql import DataFrame, SparkSession

class BaseConnector(ABC):
    def __init__(self, spark: SparkSession, config: dict):
        self.spark = spark
        self.config = config
        self.source_config = config.get("source_config", {})
        self.target_config = config.get("target_config", {})

    @abstractmethod
    def read(self) -> DataFrame:
        pass

    def write(self, df: DataFrame):
        target = self.target_config
        fmt = target.get("format", "console")
        mode = target.get("mode", "append")
        path = target.get("path")
        
        print(f"-> Writing data to: {path} (Format: {fmt}, Mode: {mode})...")
        
        if fmt == "console":
            df.show(5, truncate=False)
            return

        writer = df.write.format(fmt).mode(mode)
        
        if "partition_cols" in target:
            writer = writer.partitionBy(target["partition_cols"])

        writer.save(path)