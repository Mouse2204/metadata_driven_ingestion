import importlib
import pkgutil
import sys
from pyspark.sql import SparkSession
from src.connectors.base import BaseConnector

class ConnectorFactory:
    _registry = {}
    _loaded = False

    @classmethod
    def _load_connectors(cls):
        """Tự động quét và import toàn bộ file trong thư mục connectors"""
        if cls._loaded:
            return
        
        import src.connectors as connectors_pkg
        path = connectors_pkg.__path__
        for loader, module_name, is_pkg in pkgutil.iter_modules(path):
            full_module_name = f'src.connectors.{module_name}'
            if full_module_name not in sys.modules:
                importlib.import_module(full_module_name)
        
        cls._loaded = True

    @classmethod
    def register(cls, source_type: str):
        def inner_wrapper(wrapped_class):
            cls._registry[source_type.lower()] = wrapped_class
            return wrapped_class
        return inner_wrapper

    @staticmethod
    def get_connector(spark: SparkSession, config: dict) -> BaseConnector:
        ConnectorFactory._load_connectors()
        
        source_type = config.get("source_type", "").lower()
        connector_class = ConnectorFactory._registry.get(source_type)

        if not connector_class:
            supported = list(ConnectorFactory._registry.keys())
            raise ValueError(f"Source type '{source_type}' not found. Supported: {supported}")

        print(f"-> Factory: Selected connector for '{source_type}'")
        return connector_class(spark, config)