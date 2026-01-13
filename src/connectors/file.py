import paramiko
import os
import time
from pyspark.sql import DataFrame
from pyspark import SparkFiles
from src.connectors.base import BaseConnector
from src.connectors.factory import ConnectorFactory

@ConnectorFactory.register("sftp")
class SftpConnector(BaseConnector):
    def read(self) -> DataFrame:
        method = self.config.get("ingest_method", "paramiko")
        remote_path = self.source_config.get("file_path")
        file_name = os.path.basename(remote_path)
        
        if method == "addFile":
            self.logger.info(f"-> Đang dùng SparkContext.addFile cho tệp: {remote_path}")
            self.spark.sparkContext.addFile(remote_path)
            local_path = SparkFiles.get(file_name)
        else:
            self.logger.info(f"-> Đang dùng Paramiko để tải tệp: {remote_path}")
            local_path = f"/tmp/{file_name}"
            self._download_via_paramiko(remote_path, local_path)

        df = self.spark.read.csv(local_path, header=True, inferSchema=True)
        return df

    def _download_via_paramiko(self, remote_path, local_path):
        host = self.source_config.get("host")
        port = int(self.source_config.get("port", 22))
        username = self.source_config.get("username")
        password = self.source_config.get("password")

        try:
            transport = paramiko.Transport((host, port))
            transport.connect(username=username, password=password)
            sftp = paramiko.SFTPClient.from_transport(transport)
            sftp.get(remote_path, local_path)
            
            sftp.close()
            transport.close()
            self.logger.info(f"-> Tải tệp thành công về: {local_path}")
        except Exception as e:
            self.logger.error(f"-> Lỗi kết nối SFTP: {str(e)}")
            raise Exception(f"SFTP Connection failed: {e}")