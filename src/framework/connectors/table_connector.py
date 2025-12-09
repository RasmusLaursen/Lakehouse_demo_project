"""Connector for reading Unity Catalog tables.

This connector supports both batch and streaming reads from Delta tables
in Unity Catalog, with optional audit column addition.
"""
from typing import Dict, Any, Optional
from pyspark.sql import SparkSession, DataFrame
from src.framework.connectors.base_connector import BaseConnector
from src.framework.helper import logging_helper

logger = logging_helper.get_logger(__name__)


class TableConnector(BaseConnector):
    """Connector for reading Unity Catalog Delta tables.
    
    Supports:
    - Batch reads from Delta tables
    - Streaming reads (readStream.table)
    - Optional audit column addition
    - Fully qualified table paths (catalog.schema.table)
    
    Configuration:
        catalog (str): Unity Catalog catalog name
        schema (str): Unity Catalog schema name
        table (str): Table name
        streaming (bool): Whether to read as stream (default: False)
        add_audit_columns (bool): Add audit metadata columns (default: True)
    
    Example:
        >>> # Batch read
        >>> connector = TableConnector({
        ...     "catalog": "raw",
        ...     "schema": "lakehouse",
        ...     "table": "customer",
        ...     "streaming": False
        ... })
        >>> df = connector.read_batch(spark)
        
        >>> # Streaming read
        >>> connector = TableConnector({
        ...     "catalog": "raw",
        ...     "schema": "lakehouse",
        ...     "table": "customer",
        ...     "streaming": True
        ... })
        >>> stream_df = connector.read_stream(spark)
    """
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize connector with configuration.
        
        Args:
            config: Configuration dictionary with catalog, schema, table, and options
        """
        self.catalog = config.get("catalog")
        self.schema = config.get("schema")
        self.table = config.get("table")
        self.streaming = config.get("streaming", False)
        self.add_audit_columns = config.get("add_audit_columns", True)
        
        # Set table_path before validation (needed for error messages)
        if self.catalog and self.schema and self.table:
            self.table_path = f"{self.catalog}.{self.schema}.{self.table}"
        else:
            self.table_path = "incomplete_config"
        
        # Validate on initialization
        self.validate_config()
        
        logger.debug(
            f"Created TableConnector for {self.table_path} "
            f"(streaming={self.streaming}, audit={self.add_audit_columns})"
        )
    
    def validate_config(self) -> bool:
        """Validate that required table path components are provided.
        
        Returns:
            True if valid, raises ValueError otherwise
            
        Raises:
            ValueError: If catalog, schema, or table is missing
        """
        if not self.catalog:
            raise ValueError("TableConnector requires 'catalog' in config")
        if not self.schema:
            raise ValueError("TableConnector requires 'schema' in config")
        if not self.table:
            raise ValueError("TableConnector requires 'table' in config")
        
        logger.debug(f"Validated config for table: {self.table_path}")
        return True
    
    def read_stream(self, spark: SparkSession) -> DataFrame:
        """Read table as streaming or batch DataFrame.
        
        If streaming=True in config, uses readStream.table().
        Otherwise, falls back to batch read.
        
        Args:
            spark: Active SparkSession
            
        Returns:
            Streaming or batch DataFrame with optional audit columns
        """
        self.validate_config()
        
        if self.streaming:
            logger.info(f"Reading stream from table: {self.table_path}")
            df = spark.readStream.table(self.table_path)
        else:
            logger.info(f"Reading batch from table: {self.table_path} (via read_stream)")
            df = spark.read.table(self.table_path)
        
        if self.add_audit_columns:
            from src.framework.helper import common
            df = common.add_audit_columns(df=df)
            logger.debug("Added audit columns to DataFrame")
        
        return df
    
    def read_batch(self, spark: SparkSession) -> DataFrame:
        """Read table as batch DataFrame.
        
        Args:
            spark: Active SparkSession
            
        Returns:
            Batch DataFrame with optional audit columns
        """
        self.validate_config()
        
        logger.info(f"Reading batch from table: {self.table_path}")
        df = spark.read.table(self.table_path)
        
        if self.add_audit_columns:
            from src.framework.helper import common
            df = common.add_audit_columns(df=df)
            logger.debug("Added audit columns to DataFrame")
        
        return df
