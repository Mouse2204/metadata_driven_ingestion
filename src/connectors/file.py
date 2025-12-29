import paramiko
import os
import pandas as pd
from pyspark.sql import DataFrame
from src.connectors.base import BaseConnector
from src.connectors.factory import ConnectorFactory

@ConnectorFactory.register("sftp")
class SftpConnector(BaseConnector):
    def read(self) -> DataFrame:
        host = self.source_config.get("host")
        port = int(self.source_config.get("port", 22))
        username = self.source_config.get("username")
        password = self.source_config.get("password")
        remote_path = self.source_config.get("file_path")
        file_type = self.source_config.get("file_type", "csv").lower()
        
        local_temp_path = f"/tmp/{os.path.basename(remote_path)}"

        print(f"-> Downloading from SFTP ({host}): {remote_path} ...")
        try:
            transport = paramiko.Transport((host, port))
            transport.connect(username=username, password=password)
            sftp = paramiko.SFTPClient.from_transport(transport)
            sftp.get(remote_path, local_temp_path)
            sftp.close()
            transport.close()
        except Exception as e:
            raise Exception(f"SFTP Connection failed: {e}")
        if file_type == "csv":
           df = self.spark.read.csv(local_temp_path, header=True, inferSchema=True)
        elif file_type == "json":
           df = self.spark.read.json(local_temp_path)
        elif file_type == "excel":
            pdf = pd.read_excel(local_temp_path)
            pdf = pdf.fillna("") 
            df = self.spark.createDataFrame(pdf)
        else:
            raise ValueError(f"Unsupported file type: {file_type}")
                
        return df