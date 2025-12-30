from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from src.connectors.base import BaseConnector
from src.connectors.factory import ConnectorFactory

@ConnectorFactory.register("mongo")
class MongoConnector(BaseConnector):
    def read(self) -> DataFrame:
        uri = self.source_config.get("connection_uri")
        database = self.source_config.get("database")
        collection = self.source_config.get("collection")
        
        print(f"-> Reading from MongoDB: {database}.{collection}...")

        df = self.spark.read.format("mongodb") \
            .option("connection.uri", uri) \
            .option("database", database) \
            .option("collection", collection) \
            .load()
        
        if df.isEmpty():
            print(f"-> Warning: Collection {collection} is empty. Schema might be incomplete.")
        else:
            print(f"-> Source columns detected: {df.columns}")

        return df