import requests
import json
import time
import os
import certifi
import boto3
from botocore.client import Config
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType, StructType
from src.connectors.base import BaseConnector
from src.connectors.factory import ConnectorFactory

@ConnectorFactory.register("api")
class ApiConnector(BaseConnector):
    def _get_session(self):
        session = requests.Session()
        source = self.source_config
        headers = source.get("headers", {}).copy()
        
        auth_config = source.get("auth", {})
        auth_type = auth_config.get("type", "").lower()
        token = os.getenv(auth_config.get("token_env_var", ""), auth_config.get("token"))

        if token:
            if auth_type == "bearer":
                headers["Authorization"] = f"Bearer {token}"
            elif auth_type == "token":
                headers["Authorization"] = f"token {token}"
            elif auth_type == "apikey":
                headers["X-API-Key"] = token

        session.headers.update(headers)
        session.verify = certifi.where() if source.get("verify_ssl", True) else False
        
        retries = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
        session.mount('https://', HTTPAdapter(max_retries=retries))
        return session

    def _get_nested_value(self, data, path):
        if not path: return data
        for key in path.split('.'):
            if isinstance(data, dict):
                data = data.get(key)
            else:
                return None
        return data

    def _fetch_page(self, session, url, params, s3_client, s3_bucket, s3_prefix, page_idx):
        try:
            response = session.get(url, params=params, timeout=60)
            response.raise_for_status()
            json_data = response.json()
            records = self._get_nested_value(json_data, self.source_config.get("data_path", ""))
            
            if records:
                key = f"{s3_prefix}/page_{page_idx}.json"
                content = json.dumps(records if isinstance(records, list) else [records])
                s3_client.put_object(Bucket=s3_bucket, Key=key, Body=content)
                return True
            return False
        except Exception as e:
            self.logger.error(f"Lỗi nạp trang {page_idx}: {str(e)}")
            return e 

    def read(self):
        source = self.source_config
        url = source.get("url")
        pagination = source.get("pagination", {})
        strategy = pagination.get("strategy", "single").lower()
        max_pages = pagination.get("max_pages", 1)
        
        s3_bucket = "raw-data"
        s3_prefix = f"staging/{self.job_name}/{self.ingestion_id}"
        s3_client = boto3.client(
            's3',
            endpoint_url=os.getenv("S3_ENDPOINT", "http://minio:9000"),
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"),
            config=Config(signature_version='s3v4')
        )

        session = self._get_session()
        self.logger.info(f"-> [GĐ 2] Nạp dữ liệu lên S3 Staging: s3://{s3_bucket}/{s3_prefix}")

        if strategy == "page":
            futures = []
            with ThreadPoolExecutor(max_workers=10) as executor:
                for i in range(max_pages):
                    params = source.get("params", {}).copy()
                    params[pagination.get("page_param", "page")] = pagination.get("start_page", 1) + i
                    futures.append(executor.submit(self._fetch_page, session, url, params, s3_client, s3_bucket, s3_prefix, i))
            
            for future in as_completed(futures):
                result = future.result()
                if isinstance(result, Exception) or result is False:
                    self.logger.warning(f"Có lỗi xảy ra ở một số trang: {result}")
        else:
            current_cursor = None
            for i in range(max_pages):
                params = source.get("params", {}).copy()
                if strategy == "cursor" and current_cursor:
                    params[pagination.get("cursor_param")] = current_cursor
                
                try:
                    response = session.get(url, params=params, timeout=60)
                    response.raise_for_status() 
                    json_data = response.json()
                    records = self._get_nested_value(json_data, source.get("data_path", ""))
                    if not records: break
                    
                    key = f"{s3_prefix}/page_{i}.json"
                    content = json.dumps(records if isinstance(records, list) else [records])
                    s3_client.put_object(Bucket=s3_bucket, Key=key, Body=content)
                    
                    current_cursor = self._get_nested_value(json_data, pagination.get("cursor_path", ""))
                    if not current_cursor: break
                except Exception as e:
                    raise RuntimeError(f"Hệ thống dừng nạp do lỗi: {str(e)}")

        df = self.spark.read.option("mergeSchema", "true").json(f"s3a://{s3_bucket}/{s3_prefix}")
        
        if df.count() > 0:
            self.logger.info(f"Đã nạp thành công {df.count()} bản ghi.")
            for col_name in ["images", "eventLogs"]:
                if col_name in df.columns:
                    df = df.withColumn(col_name, F.col(col_name).cast("array<string>"))
            
            if "vehicle" in df.columns:
                df = df.withColumn("vehicle", 
                    F.when(F.col("vehicle").isNotNull(), 
                           F.struct(F.col("vehicle.*"))).otherwise(None))
        else:
            raise RuntimeError("Dữ liệu nạp từ API rỗng.")
        
        pk = self.target_config.get("primary_key")
        if pk and "." in pk:
            new_pk_name = pk.replace(".", "_")
            df = df.withColumn(new_pk_name, F.col(pk))
            self.target_config["primary_key"] = new_pk_name
            
        return df

    def cleanup(self):
        s3_bucket = "raw-data"
        s3_prefix = f"staging/{self.job_name}/{self.ingestion_id}"
        s3_client = boto3.client(
            's3',
            endpoint_url=os.getenv("S3_ENDPOINT", "http://minio:9000"),
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
        )
        objs = s3_client.list_objects_v2(Bucket=s3_bucket, Prefix=s3_prefix)
        if 'Contents' in objs:
            keys = [{'Key': obj['Key']} for obj in objs['Contents']]
            s3_client.delete_objects(Bucket=s3_bucket, Delete={'Objects': keys})