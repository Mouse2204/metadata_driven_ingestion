from src.connectors.base import BaseConnector
from src.connectors.factory import ConnectorFactory

@ConnectorFactory.register("postgres")
class JdbcConnector(BaseConnector):
    def read(self):
        self.logger.info(f"-> Reading from Postgres via JDBC: {self.source_config.get('dbtable')}")
        return self.spark.read.format("jdbc").options(
            url=self.source_config.get("url"),
            dbtable=self.source_config.get("dbtable"),
            user=self.source_config.get("user"),
            password=self.source_config.get("password"),
            driver=self.source_config.get("driver", "org.postgresql.Driver"),
            partitionColumn=self.source_config.get("partition_column"),
            lowerBound=self.source_config.get("lower_bound"),
            upperBound=self.source_config.get("upper_bound"),
            numPartitions=self.source_config.get("num_partitions")
        ).load()