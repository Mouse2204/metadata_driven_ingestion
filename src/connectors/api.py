import requests
import json
from pyspark.sql import DataFrame
from src.connectors.base import BaseConnector
from src.connectors.factory import ConnectorFactory

@ConnectorFactory.register("api")
class ApiConnector(BaseConnector):
    def read(self) -> DataFrame:
        url = self.source_config.get("url")
        method = self.source_config.get("method", "GET").upper()
        headers = self.source_config.get("headers", {})
        params = self.source_config.get("params", {})
        data_path = self.source_config.get("data_path", "")
        
        print(f"-> Calling API ({method}): {url}...")
        
        try:
            if method == "GET":
                response = requests.get(url, headers=headers, params=params)
            elif method == "POST":
                response = requests.post(url, headers=headers, json=params)
            else:
                raise ValueError(f"Unsupported method: {method}")
            
            response.raise_for_status()
            data = response.json()
            
            if data_path:
                keys = data_path.split('.')
                for key in keys:
                    if isinstance(data, dict):
                        data = data.get(key, [])
                    else:
                        break

            if isinstance(data, dict):
                data = [data]
            elif not isinstance(data, list):
                data = []

            if not data:
                return self.spark.createDataFrame([], schema="string") 

            print(f"-> Retrieved {len(data)} records. Converting to Spark DataFrame...")
            
            rdd = self.spark.sparkContext.parallelize([json.dumps(r) for r in data])
            df = self.spark.read.json(rdd)
            
            return df

        except Exception as e:
            raise Exception(f"API Connection failed: {e}")