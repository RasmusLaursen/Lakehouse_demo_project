"""
PySpark DataSource adapter layer.

This module provides a bridge between the current BaseConnector interface
and PySpark's native DataSource API (Spark 4.0+), enabling connectors to
leverage Spark's built-in optimization, partitioning, and streaming capabilities.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, Iterator, Union, Sequence, List
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType
from pyspark.sql.datasource import (
    DataSource,
    DataSourceReader,
    DataSourceStreamReader,
    SimpleDataSourceStreamReader,
    InputPartition,
)
from src.framework.helper import logging_helper

logger = logging_helper.get_logger(__name__)


class BasePySparkDataSource(DataSource):
    """
    Base class bridging BaseConnector interface with PySpark DataSource API.
    
    This adapter allows existing connectors to leverage PySpark's native
    DataSource capabilities while maintaining backward compatibility.
    
    Subclasses should implement:
    - schema() - Define the data schema
    - create_reader() - Return a batch reader
    - create_stream_reader() - Return a stream reader (optional)
    """
    
    def __init__(self, options: Dict[str, str]) -> None:
        """
        Initialize data source with options.
        
        Args:
            options: Configuration options for the data source
        """
        super().__init__(options)
        self.config = dict(options)  # Convert to regular dict
        logger.info(f"Initialized {self.__class__.__name__} with options: {list(options.keys())}")
        
    @abstractmethod
    def schema(self) -> Union[StructType, str]:
        """
        Return the schema of the data source.
        
        Returns:
            StructType or DDL string representing the schema
        """
        pass
    
    @abstractmethod
    def create_reader(self, schema: StructType) -> "BaseDataSourceReader":
        """
        Create a batch reader for this data source.
        
        Args:
            schema: The schema to use for reading
            
        Returns:
            A DataSourceReader instance
        """
        pass
    
    def reader(self, schema: StructType) -> DataSourceReader:
        """
        Return a DataSourceReader for batch reads.
        
        Args:
            schema: The schema to use for reading
            
        Returns:
            A DataSourceReader instance
        """
        return self.create_reader(schema)
    
    def create_stream_reader(self, schema: StructType):
        """
        Create a streaming reader for this data source.
        
        Args:
            schema: The schema to use for reading
            
        Returns:
            A DataSourceStreamReader instance
            
        Raises:
            NotImplementedError: If streaming is not supported
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} does not support streaming reads. "
            "Override create_stream_reader() to enable streaming."
        )

    def create_simple_stream_reader(self, schema: StructType) -> "BaseSimpleDataSourceStreamReader":
        """
        Create a streaming reader for this data source.
        
        Args:
            schema: The schema to use for reading
            
        Returns:
            A DataSourceStreamReader instance
            
        Raises:
            NotImplementedError: If streaming is not supported
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} does not support streaming reads. "
            "Override create_stream_reader() to enable streaming."
        )    
    
    def streamReader(self, schema: StructType) -> DataSourceStreamReader:
        """
        Return a DataSourceStreamReader for streaming reads.
        
        Args:
            schema: The schema to use for reading
            
        Returns:
            A DataSourceStreamReader instance
        """
        return self.create_stream_reader(schema)


class BaseDataSourceReader(DataSourceReader, ABC):
    """
    Base implementation of DataSourceReader with partition support.
    
    Subclasses should implement:
    - create_partitions() - Generate input partitions
    - read_partition(partition) - Read data for a partition
    """
    
    def __init__(self, config: Dict[str, Any], schema: StructType):
        """
        Initialize the reader.
        
        Args:
            config: Configuration dictionary
            schema: Schema for the data
        """
        self.config = config
        self.schema_struct = schema
        logger.debug(f"Initialized {self.__class__.__name__} reader")
    
    @abstractmethod
    def create_partitions(self) -> Sequence[InputPartition]:
        """
        Create input partitions for parallel reading.
        
        Returns:
            Sequence of InputPartition objects
        """
        pass
    
    def partitions(self) -> Sequence[InputPartition]:
        """
        Return partitions for parallel execution.
        
        Returns:
            Sequence of InputPartition objects
        """
        msg = f"*** BaseDataSourceReader.partitions() called"
        logger.warning(msg)
        print(msg, flush=True)
        import sys
        print(msg, file=sys.stderr, flush=True)
        partitions = self.create_partitions()
        msg2 = f"*** BaseDataSourceReader.partitions() returning {len(partitions)} partitions"
        logger.info(msg2)
        logger.warning(msg2)
        print(msg2, flush=True)
        print(msg2, file=sys.stderr, flush=True)
        return partitions
    
    @abstractmethod
    def read_partition(self, partition: InputPartition) -> Iterator[Row]:
        """
        Read data for a specific partition.
        
        Args:
            partition: The partition to read
            
        Returns:
            Iterator of Row objects
        """
        pass
    
    def read(self, partition: InputPartition) -> Iterator[Row]:
        """
        Read data for the given partition.
        
        Args:
            partition: The partition to read
            
        Returns:
            Iterator of Row objects
        """
        msg = f"*** BaseDataSourceReader.read() called: partition_type={type(partition).__name__}"
        logger.debug(msg)
        logger.warning(msg)  # Force it to warning level
        print(msg, flush=True)
        import sys
        print(msg, file=sys.stderr, flush=True)
        return self.read_partition(partition)


class BaseDataSourceStreamReader(DataSourceStreamReader, ABC):
    """
    Base implementation of DataSourceStreamReader with offset management.
    
    Subclasses should implement:
    - get_initial_offset() - Return starting offset
    - get_latest_offset() - Return current latest offset
    - create_stream_partitions(start, end) - Generate partitions for offset range
    - read_stream_partition(partition) - Read data for a partition
    """
    
    def __init__(self, config: Dict[str, Any], schema: StructType):
        """
        Initialize the stream reader.
        
        Args:
            config: Configuration dictionary
            schema: Schema for the data
        """
        self.config = config
        self.schema_struct = schema
        logger.debug(f"Initialized {self.__class__.__name__} stream reader")
    
    @abstractmethod
    def get_initial_offset(self) -> dict:
        """
        Return the initial offset for streaming.
        
        Returns:
            Dictionary representing the initial offset
        """
        pass
    
    def initialOffset(self) -> dict:
        """
        Return the initial offset.
        
        Returns:
            Dictionary representing the initial offset
        """
        offset = self.get_initial_offset()
        logger.info(f"Initial offset: {offset}")
        return offset
    
    @abstractmethod
    def get_latest_offset(self) -> dict:
        """
        Return the latest available offset.
        
        Returns:
            Dictionary representing the latest offset
        """
        pass
    
    def latestOffset(self) -> dict:
        """
        Return the latest offset.
        
        Returns:
            Dictionary representing the latest offset
        """
        offset = self.get_latest_offset()
        logger.debug(f"Latest offset: {offset}")
        return offset
    
    @abstractmethod
    def create_stream_partitions(self, start: dict, end: dict) -> Sequence[InputPartition]:
        """
        Create partitions for the given offset range.
        
        Args:
            start: Start offset
            end: End offset
            
        Returns:
            Sequence of InputPartition objects
        """
        pass
    
    def partitions(self, start: dict, end: dict) -> Sequence[InputPartition]:
        """
        Return partitions for the offset range.
        
        Args:
            start: Start offset
            end: End offset
            
        Returns:
            Sequence of InputPartition objects
        """
        partitions = self.create_stream_partitions(start, end)
        logger.info(f"Created {len(partitions)} stream partitions from {start} to {end}")
        return partitions
    
    @abstractmethod
    def read_stream_partition(self, partition: InputPartition) -> Iterator[Row]:
        """
        Read data for a specific partition.
        
        Args:
            partition: The partition to read
            
        Returns:
            Iterator of Row objects
        """
        pass
    
    def read(self, partition: InputPartition) -> Iterator[Row]:
        """
        Read data for the given partition.
        
        Args:
            partition: The partition to read
            
        Returns:
            Iterator of Row objects
        """
        logger.debug(f"Reading stream partition: {partition}")
        return self.read_stream_partition(partition)
    
    def commit(self, end: dict) -> None:
        """
        Commit processed offset.
        
        Args:
            end: The offset that has been processed
        """
        logger.debug(f"Committed offset: {end}")
    
    def stop(self) -> None:
        """
        Stop the stream reader and free resources.
        """
        logger.info(f"Stopped {self.__class__.__name__} stream reader")


class BaseSimpleDataSourceStreamReader(DataSourceStreamReader, ABC):
    """
    Simplified base implementation of DataSourceStreamReader for date/offset-based streaming.
    
    This class bridges the SimpleDataSourceStreamReader pattern with the full
    DataSourceStreamReader API that PySpark expects. It creates single partitions
    and provides hooks for offset-based data fetching.
    
    Subclasses should implement:
    - get_initial_offset() - Return starting offset
    - get_latest_offset() - Return latest available offset
    - read_data(start, end) - Fetch data between offsets
    - cleanup() - Optional cleanup when stream stops
    """
    
    def __init__(self, config: Dict[str, Any], schema: StructType):
        """
        Initialize the simple stream reader.
        
        Args:
            config: Configuration dictionary
            schema: Schema for the data
        """
        self.config = config
        self.schema_struct = schema
        logger.debug(f"Initialized {self.__class__.__name__} simple stream reader")
    
    @abstractmethod
    def get_initial_offset(self) -> dict:
        """
        Return the initial offset for streaming.
        
        Returns:
            Dictionary representing the initial offset
        """
        pass
    
    def initialOffset(self) -> dict:
        """
        Return the initial offset.
        
        Returns:
            Dictionary representing the initial offset
        """
        offset = self.get_initial_offset()
        logger.info(f"[{self.__class__.__name__}] Initial offset: {offset}")
        return offset
    
    @abstractmethod
    def get_latest_offset(self) -> dict:
        """
        Return the latest available offset.
        
        Subclasses must implement this to define how to compute the latest offset.
        For date-based offsets, this might return today's date.
        For sequence-based offsets, this might query the source for the max ID.
        
        Returns:
            Dictionary representing the latest offset
        """
        pass
    
    def latestOffset(self) -> dict:
        """
        Return the latest offset available in the data source.
        
        Called by PySpark streaming engine to determine the most recent data.
        
        Returns:
            Dictionary representing the latest offset
        """
        offset = self.get_latest_offset()
        logger.debug(f"[{self.__class__.__name__}] Latest offset: {offset}")
        return offset
    
    def partitions(self, start: dict, end: dict) -> Sequence[InputPartition]:
        """
        Return partitions for the offset range.
        
        For simple stream readers, this creates a single partition
        containing the offset range. Subclasses can override for custom partitioning.
        
        Args:
            start: Start offset
            end: End offset
            
        Returns:
            Sequence of InputPartition objects (single partition by default)
        """
        partition = SimpleInputPartition({"start": start, "end": end})
        logger.debug(f"[{self.__class__.__name__}] Created single partition from {start} to {end}")
        return [partition]
    
    @abstractmethod
    def read_data(self, partition: InputPartition) -> Iterator[Row]:
        """
        Read data for the given partition.
        
        Args:
            partition: SimpleInputPartition containing start and end offsets
            
        Returns:
            Iterator of Row objects
        """
        pass
    
    def read(self, partition: InputPartition) -> Iterator[Row]:
        """
        Read data for the given partition.
        
        Args:
            partition: The partition to read
            
        Returns:
            Iterator of Row objects
        """
        logger.debug(f"[{self.__class__.__name__}] Reading partition: {partition}")
        return self.read_data(partition)
    
    def commit(self, end: dict) -> None:
        """
        Commit processed offset.
        
        Args:
            end: The offset that has been processed
        """
        logger.debug(f"[{self.__class__.__name__}] Committed offset: {end}")
    
    def cleanup(self) -> None:
        """
        Perform custom cleanup when the stream stops.
        
        Subclasses can override this to clear caches, close connections, etc.
        Default implementation does nothing.
        """
        pass
    
    def stop(self) -> None:
        """
        Stop the stream reader and free resources.
        
        Called by PySpark when the streaming query is terminated.
        Calls cleanup() for subclass-specific cleanup logic.
        """
        logger.info(f"[{self.__class__.__name__}] Stopping stream reader")
        self.cleanup()


class SimpleInputPartition(InputPartition):
    """
    Simple InputPartition implementation with a single value.
    
    Used for connectors that don't need complex partitioning logic.
    """
    
    def __init__(self, value: Any):
        """
        Initialize partition with a value.
        
        Args:
            value: The partition value (must be picklable)
        """
        self.value = value
    
    def __repr__(self) -> str:
        return f"SimpleInputPartition({self.value})"
