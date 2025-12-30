from abc import ABC, abstractmethod
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from delta.tables import DeltaTable
import datetime

class BaseConnector(ABC):
    def __init__(self, spark: SparkSession, config: dict):
        self.spark = spark
        self.config = config
        self.source_config = config.get("source_config", {})
        self.target_config = config.get("target", {})

    @abstractmethod
    def read(self) -> DataFrame:
        pass

    def apply_transformations(self, df: DataFrame) -> DataFrame:
        rules = self.target_config.get("transformations", [])
        if not rules:
            return df

        print(f"-> Applying {len(rules)} transformation rules from metadata...")
        for rule in rules:
            rule_type = rule.get("type")
            columns = rule.get("columns", [])

            if rule_type == "fill_na":
                existing_cols = [c for c in columns if c in df.columns]
                if existing_cols:
                    df = df.fillna(rule.get("value", 0), subset=existing_cols)
            
            elif rule_type == "upper_case":
                for col in columns:
                    if col in df.columns:
                        df = df.withColumn(col, F.upper(F.col(col)))
            
            elif rule_type == "cast_type":
                target_type = rule.get("data_type", "string")
                for col in columns:
                    if col in df.columns:
                        df = df.withColumn(col, F.col(col).cast(target_type))
            
            elif rule_type == "add_ingestion_timestamp":
                df = df.withColumn("ingested_at", F.current_timestamp())

        return df

    def write(self, df: DataFrame, primary_key: str = None):
        target = self.target_config
        fmt = target.get("format", "delta").lower()
        path = target.get("path")
        
        try:
            if fmt == "console":
                df.show(5, truncate=False)
                return
            if primary_key:
                if primary_key in df.columns:
                    print(f"-> Deduplicating source data on key: {primary_key}")
                    df = df.dropDuplicates([primary_key])
                else:
                    print(f"-> Warning: Primary key '{primary_key}' not found. Skipping deduplication.")

            df = self.apply_transformations(df)

            print(f"-> Writing data to: {path} (Format: {fmt})...")

            if fmt == "delta" and primary_key:
                if DeltaTable.isDeltaTable(self.spark, path):
                    print(f"-> Performing Upsert (Merge) on key: {primary_key}")
                    dt = DeltaTable.forPath(self.spark, path)
                    dt.alias("t").merge(
                        df.alias("s"),
                        f"t.{primary_key} = s.{primary_key}"
                    ).whenMatchedUpdateAll() \
                     .whenNotMatchedInsertAll() \
                     .execute()
                else:
                    print("-> Initial Write (Overwrite)...")
                    df.write.format("delta").mode("overwrite").save(path)
            else:
                mode = target.get("mode", "append")
                df.write.format(fmt).mode(mode).save(path)
            
            print(f"Ingestion Success at {datetime.datetime.now()}")

        except Exception as e:
            error_msg = f"Ingestion Failed: {str(e)}"
            print(error_msg)
            raise e