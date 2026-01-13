from src.connectors.base import BaseConnector
from src.connectors.factory import ConnectorFactory

@ConnectorFactory.register("mongo")
class MongoConnector(BaseConnector):
    def read(self):
        self.logger.info(f"-> Reading from MongoDB: {self.source_config.get('database')}.{self.source_config.get('collection')}")
        return self.spark.read.format("mongodb").options(
            **{
                "connection.uri": self.source_config.get("connection_uri"),
                "database": self.source_config.get("database"),
                "collection": self.source_config.get("collection")
            }
        ).load()