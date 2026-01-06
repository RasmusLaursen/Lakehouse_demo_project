"""
Auto Loader connector for reading data from various cloud storage sources.

This connector implements the BaseConnector interface using Databricks Auto Loader
(cloudFiles) for incremental data ingestion from multiple sources including:
- Databricks Unity Catalog volumes
- AWS S3
- Azure Data Lake Storage (ADLS)
- Google Cloud Storage (GCS)
- Kafka topics
- Azure Event Hubs
"""

from typing import Dict, Any, Optional
from pyspark.sql import SparkSession, DataFrame
from src.framework.connectors.base_connector import BaseConnector
from src.framework.helper import logging_helper, add_audit_columns

logger = logging_helper.get_logger(__name__)


class AutoLoaderConnector(BaseConnector):
    """
    Connector for reading data using Databricks Auto Loader (cloudFiles).
    
    Auto Loader provides incremental and efficient ingestion from:
    - Volumes: /Volumes/{catalog}/{schema}/{volume}
    - S3: s3://bucket/path
    - ADLS: abfss://container@account.dfs.core.windows.net/path
    - GCS: gs://bucket/path
    - Kafka: kafka broker configuration
    - Event Hubs: event hub connection configuration
    
    Supports both streaming (primary use case) and batch reading.
    """
    
    def validate_config(self, config: Dict[str, Any]) -> None:
        """
        Validate Auto Loader connector configuration.
        
        Required fields (choose one source type):
            For Volumes:
                - source_type: "volume"
                - catalog: Unity Catalog catalog name
                - schema: Unity Catalog schema name
                - volume: Volume name
                - format: File format (json, csv, parquet, etc.)
            
            For S3/ADLS/GCS:
                - source_type: "s3" | "adls" | "gcs"
                - path: Cloud storage path (s3://, abfss://, gs://)
                - format: File format
            
            For Kafka:
                - source_type: "kafka"
                - kafka_bootstrap_servers: Kafka broker addresses
                - topics: Topic name or list of topics
            
            For Event Hubs:
                - source_type: "eventhub"
                - eventhub_connection_string: Connection string
                - eventhub_name: Event Hub name
            
        Optional fields (all source types):
            - add_audit_columns: Whether to add audit columns (default: False)
            - options: Additional cloudFiles options (default: {})
            - schema_hints: Schema hints for Auto Loader
            - inference_schema: Schema location for inference
        
        Args:
            config: Configuration dictionary
            
        Raises:
            ValueError: If required fields are missing or invalid source_type
        """
        source_type = config.get("source_type", "volume").lower()
        
        if source_type == "volume":
            required_fields = ["catalog", "schema", "volume", "format"]
            missing = [f for f in required_fields if f not in config]
            if missing:
                raise ValueError(
                    f"AutoLoaderConnector (volume) missing required fields: {', '.join(missing)}"
                )
        elif source_type in ["s3", "adls", "gcs"]:
            required_fields = ["path", "format"]
            missing = [f for f in required_fields if f not in config]
            if missing:
                raise ValueError(
                    f"AutoLoaderConnector ({source_type}) missing required fields: {', '.join(missing)}"
                )
        elif source_type == "kafka":
            required_fields = ["kafka_bootstrap_servers", "topics"]
            missing = [f for f in required_fields if f not in config]
            if missing:
                raise ValueError(
                    f"AutoLoaderConnector (kafka) missing required fields: {', '.join(missing)}"
                )
        elif source_type == "eventhub":
            required_fields = ["eventhub_connection_string", "eventhub_name"]
            missing = [f for f in required_fields if f not in config]
            if missing:
                raise ValueError(
                    f"AutoLoaderConnector (eventhub) missing required fields: {', '.join(missing)}"
                )
        else:
            raise ValueError(
                f"Invalid source_type: '{source_type}'. "
                f"Valid types: volume, s3, adls, gcs, kafka, eventhub"
            )
        
        logger.info(f"AutoLoaderConnector ({source_type}) config validated")
    
    def read_stream(self, spark: SparkSession) -> DataFrame:
        """
        Read data as a streaming DataFrame using cloudFiles (Auto Loader).
        
        Auto Loader automatically detects and processes new data as it arrives,
        making it ideal for incremental data ingestion from various sources.
        
        Args:
            spark: Active SparkSession
            
        Returns:
            Streaming DataFrame from the configured source
        """
        source_type = self.config.get("source_type", "volume").lower()
        add_audit = self.config.get("add_audit_columns", False)
        options = self.config.get("options", {}).copy()
        
        if source_type == "volume":
            df = self._read_stream_volume(spark, options)
        elif source_type in ["s3", "adls", "gcs"]:
            df = self._read_stream_cloud_storage(spark, options, source_type)
        elif source_type == "kafka":
            df = self._read_stream_kafka(spark, options)
        elif source_type == "eventhub":
            df = self._read_stream_eventhub(spark, options)
        else:
            raise ValueError(f"Unsupported source_type for streaming: {source_type}")
        
        # Add audit columns if requested
        if add_audit:
            df = add_audit_columns(df=df)
            logger.info("Added audit columns to streaming DataFrame")
        
        return df
    
    def _read_stream_volume(self, spark: SparkSession, options: Dict[str, Any]) -> DataFrame:
        """Read stream from Databricks volume using Auto Loader."""
        catalog = self.config["catalog"]
        schema = self.config["schema"]
        volume = self.config["volume"]
        file_format = self.config["format"]
        subfolder = self.config.get("path", "")
        
        # Construct volume path
        volume_path = f"/Volumes/{catalog}/{schema}/{volume}/"
        if subfolder:
            volume_path += subfolder.strip("/") + "/"
        
        logger.info(f"Reading stream from volume: {volume_path} (format: {file_format})")
        
        return (
            spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", file_format)
            .options(**options)
            .load(volume_path)
        )
    
    def _read_stream_cloud_storage(
        self, 
        spark: SparkSession, 
        options: Dict[str, Any],
        source_type: str
    ) -> DataFrame:
        """Read stream from cloud storage (S3/ADLS/GCS) using Auto Loader."""
        path = self.config["path"]
        file_format = self.config["format"]
        
        logger.info(f"Reading stream from {source_type.upper()}: {path} (format: {file_format})")
        
        return (
            spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", file_format)
            .options(**options)
            .load(path)
        )
    
    def _read_stream_kafka(self, spark: SparkSession, options: Dict[str, Any]) -> DataFrame:
        """Read stream from Kafka using Spark Structured Streaming."""
        kafka_servers = self.config["kafka_bootstrap_servers"]
        topics = self.config["topics"]
        
        if isinstance(topics, list):
            topics = ",".join(topics)
        
        logger.info(f"Reading stream from Kafka: {kafka_servers}, topics: {topics}")
        
        # Kafka uses native Spark streaming, not cloudFiles
        kafka_options = {
            "kafka.bootstrap.servers": kafka_servers,
            "subscribe": topics,
            **options
        }
        
        return (
            spark.readStream
            .format("kafka")
            .options(**kafka_options)
            .load()
        )
    
    def _read_stream_eventhub(self, spark: SparkSession, options: Dict[str, Any]) -> DataFrame:
        """Read stream from Azure Event Hubs."""
        connection_string = self.config["eventhub_connection_string"]
        eventhub_name = self.config["eventhub_name"]
        
        logger.info(f"Reading stream from Event Hub: {eventhub_name}")
        
        eh_conf = {
            "eventhubs.connectionString": connection_string,
            **options
        }
        
        return (
            spark.readStream
            .format("eventhubs")
            .options(**eh_conf)
            .load()
        )
    
    def read_batch(self, spark: SparkSession) -> DataFrame:
        """
        Read data as a batch DataFrame.
        
        Reads all available data as a single batch operation.
        Useful for historical data loading or one-time imports.
        
        Note: Batch reading is primarily for file-based sources.
        For Kafka/Event Hubs, use streaming with triggers instead.
        
        Args:
            spark: Active SparkSession
            
        Returns:
            Batch DataFrame from the configured source
        """
        source_type = self.config.get("source_type", "volume").lower()
        add_audit = self.config.get("add_audit_columns", False)
        options = self.config.get("options", {})
        
        try:
            if source_type == "volume":
                df = self._read_batch_volume(spark, options)
            elif source_type in ["s3", "adls", "gcs"]:
                df = self._read_batch_cloud_storage(spark, options)
            elif source_type in ["kafka", "eventhub"]:
                raise NotImplementedError(
                    f"Batch reading not supported for {source_type}. "
                    "Use streaming with Trigger.Once or Trigger.AvailableNow instead."
                )
            else:
                raise ValueError(f"Unsupported source_type for batch: {source_type}")
            
            # Add audit columns if requested
            if add_audit:
                df = add_audit_columns(df=df)
                logger.info("Added audit columns to batch DataFrame")
            
            logger.info(f"Successfully read batch data from {source_type}")
            return df
            
        except Exception as e:
            logger.error(f"Failed to read batch from {source_type}: {str(e)}")
            raise
    
    def _read_batch_volume(self, spark: SparkSession, options: Dict[str, Any]) -> DataFrame:
        """Read batch from Databricks volume."""
        catalog = self.config["catalog"]
        schema = self.config["schema"]
        volume = self.config["volume"]
        file_format = self.config["format"]
        subfolder = self.config.get("path", "")
        
        # Construct volume path
        volume_path = f"/Volumes/{catalog}/{schema}/{volume}/"
        if subfolder:
            volume_path += subfolder.strip("/")
        
        logger.info(f"Reading batch from volume: {volume_path} (format: {file_format})")
        
        return (
            spark.read
            .format(file_format)
            .options(**options)
            .load(volume_path)
        )
    
    def _read_batch_cloud_storage(self, spark: SparkSession, options: Dict[str, Any]) -> DataFrame:
        """Read batch from cloud storage (S3/ADLS/GCS)."""
        path = self.config["path"]
        file_format = self.config["format"]
        
        logger.info(f"Reading batch from cloud storage: {path} (format: {file_format})")
        
        return (
            spark.read
            .format(file_format)
            .options(**options)
            .load(path)
        )
