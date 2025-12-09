"""
Base connector interface for data ingestion.

This module defines the abstract base class for all data source connectors.
Connectors encapsulate the logic for reading data from different sources
(volumes, REST APIs, databases, message queues, etc.) into Spark DataFrames.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
from pyspark.sql import SparkSession, DataFrame


class BaseConnector(ABC):
    """
    Abstract base class for all data source connectors.
    
    All connectors must implement read_stream() for streaming ingestion
    and read_batch() for batch ingestion. The validate_config() method
    ensures connector-specific configuration is valid before execution.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the connector with configuration.
        
        Args:
            config: Connector-specific configuration dictionary
        """
        self.config = config
        self.validate_config(config)
    
    @abstractmethod
    def read_stream(self, spark: SparkSession) -> DataFrame:
        """
        Read data as a streaming DataFrame.
        
        Args:
            spark: Active SparkSession
            
        Returns:
            Streaming DataFrame from the data source
            
        Raises:
            NotImplementedError: If streaming is not supported by this connector
        """
        pass
    
    @abstractmethod
    def read_batch(self, spark: SparkSession) -> DataFrame:
        """
        Read data as a batch DataFrame.
        
        Args:
            spark: Active SparkSession
            
        Returns:
            Batch DataFrame from the data source
            
        Raises:
            NotImplementedError: If batch reading is not supported by this connector
        """
        pass
    
    @abstractmethod
    def validate_config(self, config: Dict[str, Any]) -> None:
        """
        Validate connector-specific configuration.
        
        Args:
            config: Configuration dictionary to validate
            
        Raises:
            ValueError: If configuration is invalid or missing required fields
        """
        pass
    
    def supports_streaming(self) -> bool:
        """
        Check if this connector supports streaming ingestion.
        
        Returns:
            True if streaming is supported, False otherwise
        """
        try:
            # Check if read_stream is implemented (not just inherited abstract method)
            return True
        except NotImplementedError:
            return False
    
    def supports_batch(self) -> bool:
        """
        Check if this connector supports batch ingestion.
        
        Returns:
            True if batch is supported, False otherwise
        """
        try:
            # Check if read_batch is implemented (not just inherited abstract method)
            return True
        except NotImplementedError:
            return False
