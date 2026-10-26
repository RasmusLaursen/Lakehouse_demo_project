"""Connector for wrapping existing Spark DataFrames.

This connector is primarily used for backward compatibility with code that
passes DataFrames directly to DLT table creation.
"""
from typing import Dict, Any
from pyspark.sql import SparkSession, DataFrame
from src.framework.connectors.base_connector import BaseConnector
from src.framework.helper import logging_helper

logger = logging_helper.get_logger(__name__)


class DataFrameConnector(BaseConnector):
    """Connector that wraps an existing Spark DataFrame.
    
    This connector is useful for:
    1. Backward compatibility with legacy DataFrame-based code
    2. In-memory transformations that produce DataFrames
    3. Testing with mock DataFrames
    
    Example:
        >>> df = spark.createDataFrame([(1, "Alice"), (2, "Bob")], ["id", "name"])
        >>> connector = DataFrameConnector(df)
        >>> result = connector.read_stream(spark)
    """
    
    def __init__(self, dataframe: DataFrame):
        """Initialize connector with DataFrame.
        
        Args:
            dataframe: Spark DataFrame to wrap
        """
        self.dataframe = dataframe
        logger.debug(f"Created DataFrameConnector with schema: {dataframe.schema.simpleString()}")
    
    def validate_config(self) -> bool:
        """Validate that DataFrame is provided.
        
        Returns:
            True if DataFrame exists, raises ValueError otherwise
            
        Raises:
            ValueError: If dataframe is None
        """
        if self.dataframe is None:
            raise ValueError("DataFrameConnector requires a non-null DataFrame")
        return True
    
    def read_stream(self, spark: SparkSession) -> DataFrame:
        """Return the wrapped DataFrame.
        
        Note: This returns the DataFrame as-is. If you need a streaming DataFrame,
        ensure the source DataFrame is already a streaming DataFrame.
        
        Args:
            spark: Active SparkSession (not used, DataFrame already exists)
            
        Returns:
            The wrapped DataFrame
        """
        logger.debug("Reading DataFrame (pass-through)")
        return self.dataframe
    
    def read_batch(self, spark: SparkSession) -> DataFrame:
        """Return the wrapped DataFrame in batch mode.
        
        Args:
            spark: Active SparkSession (not used, DataFrame already exists)
            
        Returns:
            The wrapped DataFrame
        """
        logger.debug("Reading DataFrame in batch mode (pass-through)")
        return self.dataframe
