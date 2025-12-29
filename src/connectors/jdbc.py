from src.connectors.base import BaseConnector
from src.connectors.factory import ConnectorFactory
@ConnectorFactory.register("postgres")
class JdbcConnector(BaseConnector):
    def read(self):
        print(f"-> Reading from Postgres table: {self.source_config.get('dbtable')}...")

        reader = self.spark.read.format("jdbc") \
            .option("url", self.source_config.get("url")) \
            .option("dbtable", self.source_config.get("dbtable")) \
            .option("user", self.source_config.get("user")) \
            .option("password", self.source_config.get("password")) \
            .option("driver", "org.postgresql.Driver") 
        
        return reader.load()