from abc import ABC, abstractmethod
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from delta.tables import DeltaTable
from src.utils.logging import get_logger
import datetime
import uuid

class BaseConnector(ABC):
    def __init__(self, spark: SparkSession, config: dict):
        self.spark = spark
        self.config = config
        self.job_name = config.get("job_name", "unknown")
        self.source_config = config.get("source_config", {})
        self.target_config = config.get("target", {})
        self.logger = get_logger(self.__class__.__name__)
        self.ingestion_id = str(uuid.uuid4())

    @abstractmethod
    def read(self) -> DataFrame:
        pass

    def get_incremental_filter(self):
        ingest_type = self.config.get("ingestion_type", "full").lower()
        col = self.config.get("incremental_column")
        last_val = self.config.get("last_ingestion_value")
        if ingest_type == "incremental" and col and last_val:
            return f"{col} > '{last_val}'"
        return None

    def _enrich_metadata(self, df: DataFrame) -> DataFrame:
        return df.withColumn("_ingestion_id", F.lit(self.ingestion_id)) \
                 .withColumn("_ingestion_type", F.lit(self.config.get("ingestion_type", "full").upper())) \
                 .withColumn("_source_name", F.lit(self.config.get("source_type"))) \
                 .withColumn("_ingestion_status", F.lit("SUCCESS")) \
                 .withColumn("_ingested_at", F.current_timestamp())

    def apply_transformations(self, df: DataFrame) -> DataFrame:
        inc_filter = self.get_incremental_filter()
        if inc_filter:
            df = df.filter(inc_filter)
        
        df = self._enrich_metadata(df)
        rules = self.target_config.get("transformations", [])
        for rule in rules:
            rt = rule.get("type")
            cols = rule.get("columns", [])
            if rt == "fill_na":
                df = df.fillna(rule.get("value", 0), subset=[c for c in cols if c in df.columns])
            elif rt == "upper_case":
                for c in cols:
                    if c in df.columns: df = df.withColumn(c, F.upper(F.col(c)))
            elif rt == "cast_type":
                tt = rule.get("data_type", "string")
                for c in cols:
                    if c in df.columns: df = df.withColumn(c, F.col(c).cast(tt))
            elif rt == "add_ingestion_timestamp":
                df = df.withColumn("ingested_at", F.current_timestamp())
        return df

    def validate(self, source_df: DataFrame, target_path: str) -> dict:
        sc = source_df.count()
        try:
            tdf = self.spark.read.format("delta").load(target_path)
            tc = tdf.filter(F.col("_ingestion_id") == self.ingestion_id).count()
        except:
            tc = 0
        return {"sc": sc, "tc": tc, "status": "SUCCESS" if sc == tc else "FAILED"}

    def write(self, df: DataFrame, primary_key: str = None):
        target = self.target_config
        fmt = target.get("format", "delta").lower()
        path = target.get("path")
        pk = primary_key or target.get("primary_key")
        
        if pk and pk in df.columns:
            df = df.dropDuplicates([pk])
        
        df = self.apply_transformations(df)
        
        if fmt == "delta" and pk:
            if DeltaTable.isDeltaTable(self.spark, path):
                dt = DeltaTable.forPath(self.spark, path)
                self.spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
                dt.alias("t").merge(df.alias("s"), f"t.{pk} = s.{pk}") \
                  .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
            else:
                df.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(path)
        else:
            df.write.format(fmt).mode(target.get("mode", "append")).save(path)

    def _save_audit_log(self, status, sc=0, tc=0, start=None, err=""):
        audit_path = "s3a://delta-lake/ingestion_audit_logs"
        log = self.spark.createDataFrame([{
            "ingestion_id": self.ingestion_id,
            "job_name": self.job_name,
            "status": status,
            "source_count": sc,
            "target_count": tc,
            "duration": (datetime.datetime.now() - start).total_seconds(),
            "error": err,
            "ts": datetime.datetime.now()
        }])
        log.write.format("delta").mode("append").option("mergeSchema", "true").save(audit_path)

    def execute(self):
        start = datetime.datetime.now()
        try:
            df = self.read()
            self.write(df)
            res = self.validate(df, self.target_config.get("path"))
            self._save_audit_log("SUCCESS", res["sc"], res["tc"], start)
        except Exception as e:
            self.logger.error(f"Job Failed: {str(e)}")
            self._save_audit_log("FAILED", 0, 0, start, str(e))
            raise e