"""
Partition strategies for parallel data ingestion.

This module provides reusable partition implementations for different
data source types, enabling efficient parallel execution.
"""

from dataclasses import dataclass
from typing import Any, List, Optional
from pyspark.sql.datasource import InputPartition


@dataclass
class RangeInputPartition(InputPartition):
    """
    Partition representing a numeric range.
    
    Useful for JDBC connectors with partition columns, offset-based
    pagination, or any range-based data splitting.
    """
    start: int
    end: int
    column: Optional[str] = None
    
    def __repr__(self) -> str:
        if self.column:
            return f"RangeInputPartition(column={self.column}, start={self.start}, end={self.end})"
        return f"RangeInputPartition(start={self.start}, end={self.end})"


@dataclass
class FileInputPartition(InputPartition):
    """
    Partition representing one or more files.
    
    Useful for volume/AutoLoader connectors reading from file systems.
    """
    file_paths: List[str]
    format: str = "parquet"
    
    def __repr__(self) -> str:
        file_count = len(self.file_paths)
        return f"FileInputPartition(files={file_count}, format={self.format})"


@dataclass
class OffsetInputPartition(InputPartition):
    """
    Partition representing an offset range for streaming sources.
    
    Useful for Kafka, REST API polling, or any offset-based streaming.
    """
    start_offset: dict
    end_offset: dict
    partition_id: Optional[str] = None
    
    def __repr__(self) -> str:
        if self.partition_id:
            return f"OffsetInputPartition(id={self.partition_id}, start={self.start_offset}, end={self.end_offset})"
        return f"OffsetInputPartition(start={self.start_offset}, end={self.end_offset})"


@dataclass
class PageInputPartition(InputPartition):
    """
    Partition representing a page for API pagination.
    
    Useful for REST API connectors with page-based or cursor-based pagination.
    """
    page_number: Optional[int] = None
    cursor: Optional[str] = None
    offset: Optional[int] = None
    limit: int = 5000
    
    def __repr__(self) -> str:
        if self.page_number is not None:
            return f"PageInputPartition(page={self.page_number}, limit={self.limit})"
        elif self.cursor:
            return f"PageInputPartition(cursor={self.cursor}, limit={self.limit})"
        else:
            return f"PageInputPartition(offset={self.offset}, limit={self.limit})"


@dataclass
class TablePartition(InputPartition):
    """
    Partition representing a table partition or filter predicate.
    
    Useful for Unity Catalog table connectors with partition pruning.
    """
    table_path: str
    partition_filter: Optional[str] = None
    
    def __repr__(self) -> str:
        if self.partition_filter:
            return f"TablePartition(table={self.table_path}, filter={self.partition_filter})"
        return f"TablePartition(table={self.table_path})"


@dataclass
class HashInputPartition(InputPartition):
    """
    Partition based on hash bucketing.
    
    Useful for distributing data evenly across partitions when
    natural partitioning isn't available.
    """
    bucket_id: int
    num_buckets: int
    hash_column: Optional[str] = None
    
    def __repr__(self) -> str:
        if self.hash_column:
            return f"HashInputPartition(bucket={self.bucket_id}/{self.num_buckets}, column={self.hash_column})"
        return f"HashInputPartition(bucket={self.bucket_id}/{self.num_buckets})"
