"""
Connectors module for data source ingestion.

This module provides a framework for reading data from various sources
into Spark DataFrames. Each connector implements the BaseConnector interface
and is automatically registered with the ConnectorFactory.

Available connectors:
    - AutoLoaderConnector: Incremental ingestion from volumes, S3, ADLS, GCS, Kafka, Event Hubs
    - RestApiConnector: Read from REST APIs with auth, pagination, rate limiting
    - JdbcConnector: Read from relational databases (PostgreSQL, MySQL, SQL Server, etc.)
    - TableConnector: Read from Unity Catalog Delta tables (batch or streaming)
    - DataFrameConnector: Wrap existing Spark DataFrames
"""

from src.framework.connectors.base_connector import BaseConnector
from src.framework.connectors.connector_factory import ConnectorFactory
from src.framework.connectors.autoloader_connector import AutoLoaderConnector
from src.framework.connectors.rest_api_connector import RESTAPIDataSource
from src.framework.connectors.jdbc_connector import JdbcConnector
from src.framework.connectors.dataframe_connector import DataFrameConnector
from src.framework.connectors.pyspark_datasource_adapter import (
    BasePySparkDataSource,
    BaseDataSourceReader,
    BaseDataSourceStreamReader,
    SimpleInputPartition,
)
from src.framework.connectors.json_response_extractor import JSONResponseExtractor
from src.framework.connectors.partition_strategies import (
    RangeInputPartition,
    FileInputPartition,
    OffsetInputPartition,
    PageInputPartition,
    TablePartition,
    HashInputPartition,
)
# PySpark DataSource implementations (Spark 4.0+) for non-Databricks sources
from src.framework.connectors.rest_api_connector import RESTAPIDataSource

# Auto-register all connectors
ConnectorFactory.register("autoloader", AutoLoaderConnector)
ConnectorFactory.register("volume", AutoLoaderConnector)  # Backward compatibility
ConnectorFactory.register("volume_autoloader", AutoLoaderConnector)  # Backward compatibility
ConnectorFactory.register("s3", AutoLoaderConnector)
ConnectorFactory.register("adls", AutoLoaderConnector)
ConnectorFactory.register("gcs", AutoLoaderConnector)
ConnectorFactory.register("kafka", AutoLoaderConnector)
ConnectorFactory.register("eventhub", AutoLoaderConnector)
ConnectorFactory.register("rest_api", RESTAPIDataSource)
ConnectorFactory.register("http", RESTAPIDataSource)  # Alias
ConnectorFactory.register("https", RESTAPIDataSource)  # Alias

ConnectorFactory.register("jdbc", JdbcConnector)
ConnectorFactory.register("database", JdbcConnector)  # Alias
ConnectorFactory.register("dataframe", DataFrameConnector)

# Backward compatibility alias
VolumeConnector = AutoLoaderConnector

__all__ = [
    # Base classes
    "BaseConnector",
    "ConnectorFactory",
    # Legacy connectors
    "AutoLoaderConnector",
    "JdbcConnector",
    "DataFrameConnector",
    # PySpark DataSource API (Spark 4.0+)
    "BasePySparkDataSource",
    "BaseDataSourceReader",
    "BaseDataSourceStreamReader",
    "SimpleInputPartition",
    # JSON extraction utility
    "JSONResponseExtractor",
    # Partition strategies
    "RangeInputPartition",
    "FileInputPartition",
    "OffsetInputPartition",
    "PageInputPartition",
    "TablePartition",
    "HashInputPartition",
    # PySpark DataSource implementations (non-Databricks native)
    "RESTAPIDataSource"
]
