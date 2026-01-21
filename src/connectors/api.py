import requests
import json
import time
import os
import shutil
import certifi
import urllib3
from concurrent.futures import ThreadPoolExecutor
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from pyspark.sql import functions as F
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
        verify_ssl = source.get("verify_ssl", True)
        session.verify = certifi.where() if verify_ssl else False
        if not verify_ssl:
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
            
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

    def _fetch_page(self, session, url, params, staging_path, page_idx):
        try:
            response = session.get(url, params=params, timeout=60)
            response.raise_for_status()
            json_data = response.json()
            records = self._get_nested_value(json_data, self.source_config.get("data_path", ""))
            
            if records:
                batch_file = os.path.join(staging_path, f"page_{page_idx}.json")
                with open(batch_file, "w") as f:
                    json.dump(records if isinstance(records, list) else [records], f)
                return True
        except Exception as e:
            self.logger.error(f"Lỗi nạp trang {page_idx}: {e}")
        return False

    def read(self):
        source = self.source_config
        url = source.get("url")
        pagination = source.get("pagination", {})
        strategy = pagination.get("strategy", "single").lower()
        max_pages = pagination.get("max_pages", 1)
        
        staging_path = f"/tmp/ingestion_{self.job_name}_{self.ingestion_id}"
        os.makedirs(staging_path, exist_ok=True)
        session = self._get_session()

        self.logger.info(f"-> Bắt đầu nạp dữ liệu (Strategy: {strategy})")

        if strategy == "page":
            with ThreadPoolExecutor(max_workers=5) as executor:
                for i in range(max_pages):
                    params = source.get("params", {}).copy()
                    params[pagination.get("page_param", "page")] = pagination.get("start_page", 1) + i
                    executor.submit(self._fetch_page, session, url, params, staging_path, i)
        else:
            current_cursor = None
            for i in range(max_pages):
                params = source.get("params", {}).copy()
                if strategy == "cursor" and current_cursor:
                    params[pagination.get("cursor_param")] = current_cursor
                
                response = session.get(url, params=params, timeout=60)
                response.raise_for_status()
                json_data = response.json()
                
                records = self._get_nested_value(json_data, source.get("data_path", ""))
                if not records: break
                
                with open(os.path.join(staging_path, f"page_{i}.json"), "w") as f:
                    json.dump(records if isinstance(records, list) else [records], f)
                
                current_cursor = self._get_nested_value(json_data, pagination.get("cursor_path", ""))
                if not current_cursor: break
                if pagination.get("sleep_time", 0) > 0: time.sleep(pagination.get("sleep_time"))

        if not os.path.exists(staging_path) or not os.listdir(staging_path):
            return self.spark.createDataFrame([], schema="string")
            
        df = self.spark.read.json(staging_path)
        
        pk = self.target_config.get("primary_key")
        if pk and "." in pk:
            new_pk_name = pk.replace(".", "_")
            df = df.withColumn(new_pk_name, F.col(pk))
            self.target_config["primary_key"] = new_pk_name
            self.logger.info(f"-> Đã phẳng hóa khóa chính: {pk} -> {new_pk_name}")
            
        return df

    def cleanup(self):
        staging_path = f"/tmp/ingestion_{self.job_name}_{self.ingestion_id}"
        if os.path.exists(staging_path):
            shutil.rmtree(staging_path)
            self.logger.info(f"-> Đã dọn dẹp thư mục tạm: {staging_path}")