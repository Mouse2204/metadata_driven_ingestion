import requests
import json
import time
from pyspark.sql import DataFrame
from src.connectors.base import BaseConnector
from src.connectors.factory import ConnectorFactory

@ConnectorFactory.register("api")
class ApiConnector(BaseConnector):
    def read(self) -> DataFrame:
        source = self.source_config
        url = source.get("url")
        method = source.get("method", "GET").upper()
        headers = source.get("headers", {})
        params = source.get("params", {}).copy()
        data_path = source.get("data_path", "")
        pagination = source.get("pagination", {})
        cursor_param = pagination.get("cursor_param", "cursor")
        cursor_path = pagination.get("cursor_path")
        max_pages = pagination.get("max_pages", 10)
        sleep_time = pagination.get("sleep_time", 0)
        
        all_records = []
        page_count = 0
        current_cursor = None

        print(f"-> Starting Cursor Pagination: {url}")

        while page_count < max_pages:
            page_count += 1
            if current_cursor:
                params[cursor_param] = current_cursor

            print(f"   - Fetching page {page_count} (Cursor: {current_cursor})...")
            
            try:
                response = requests.get(url, headers=headers, params=params, timeout=30)
                response.raise_for_status()
                json_data = response.json()
            except Exception as e:
                print(f"   - API Request failed: {e}")
                break

            records = self._get_nested_value(json_data, data_path)
            if not records:
                break
            
            all_records.extend(records if isinstance(records, list) else [records])

            next_cursor = self._get_nested_value(json_data, cursor_path)
            if not next_cursor or next_cursor == current_cursor:
                print("   - No more cursors found.")
                break
            
            current_cursor = next_cursor
            if sleep_time > 0:
                time.sleep(sleep_time)

        if not all_records:
            return self.spark.createDataFrame([], schema="string")

        print(f"-> Total records collected via Cursor: {len(all_records)}")
        rdd = self.spark.sparkContext.parallelize([json.dumps(r) for r in all_records])
        return self.spark.read.json(rdd)

    def _get_nested_value(self, data, path):
        if not path: return None
        for key in path.split('.'):
            if isinstance(data, dict):
                data = data.get(key)
            else:
                return None
        return data