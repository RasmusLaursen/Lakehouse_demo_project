"""Configuration models for pipeline and table management.

This module contains Pydantic models for validating and managing configuration
across different layers of the lakehouse pipeline.
"""

from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field, asdict
from pydantic import BaseModel, field_validator


class TableConfig(BaseModel):
    """Configuration for a single table (CDC, SCD, etc.)
    
    Attributes:
        keys: Primary key columns for the table
        sequence_column: Column used to track changes (timestamps, sequence numbers)
        stored_as_scd_type: Type 1 (snapshot) or Type 2 (slowly changing dimension)
        backfill: Optional backfill date for incremental loads
        column_list: Explicit list of columns to include
        except_column_list: Columns to exclude from processing
        track_history_column_list: Columns where full history should be tracked
        track_history_except_column_list: Columns to exclude from history tracking
        apply_as_deletes: Configure deletion handling
        apply_as_truncates: Configure truncation handling
        ignore_null_updates: Whether to ignore NULL value updates
        data_quality: Whether to apply data quality checks
    """
    
    keys: List[str]
    sequence_column: str
    stored_as_scd_type: int
    backfill: Optional[str] = None
    column_list: Optional[List[str]] = None
    except_column_list: Optional[List[str]] = None
    track_history_column_list: Optional[List[str]] = None
    track_history_except_column_list: Optional[List[str]] = None
    apply_as_deletes: Optional[Dict[str, Any]] = None
    apply_as_truncates: Optional[Dict[str, Any]] = None
    ignore_null_updates: bool = False
    data_quality: bool = False
    
    class Config:
        """Pydantic configuration."""
        validate_assignment = True
    
    @field_validator('stored_as_scd_type')
    @classmethod
    def validate_scd_type(cls, v: int) -> int:
        """Validate that SCD type is 1 or 2."""
        if v not in (1, 2):
            raise ValueError('stored_as_scd_type must be 1 or 2')
        return v
    
    @field_validator('track_history_column_list', 'track_history_except_column_list')
    @classmethod
    def validate_track_history_lists(cls, v: Optional[List[str]]) -> Optional[List[str]]:
        """Validate track history list configurations."""
        if v is not None and not isinstance(v, list):
            raise ValueError('track_history columns must be a list')
        return v


class LayerConfig(BaseModel):
    """Configuration for an entire layer (raw, base, curated, etc.)
    
    Attributes:
        source_system_name: Name of the source system (lakehouse, review, etc.)
        load_type: Load type (batch, streaming, incremental)
        file_type: File format (parquet, delta, csv, etc.)
        objects: Dictionary of table configurations by table name
    """
    
    source_system_name: Optional[str] = None
    load_type: Optional[str] = None
    file_type: Optional[str] = None
    objects: Dict[str, TableConfig] = field(default_factory=dict)
    
    class Config:
        """Pydantic configuration."""
        validate_assignment = True


@dataclass(frozen=True)
class DefaultTblProperties:
    """Default Delta table properties for CDC and DLT.
    
    These are the recommended default properties for Delta tables
    supporting change data capture and Delta Live Tables.
    
    Attributes:
        delta_enableDeletionVectors: Enable deletion vectors
        delta_enableRowTracking: Enable row tracking
        delta_enableChangeDataFeed: Enable change data feed
        pipelines_changeDataCaptureMode: CDC mode (TRACK_CHANGES or DISABLED)
    """
    
    delta_enableDeletionVectors: str = "true"
    delta_enableRowTracking: str = "true"
    delta_enableChangeDataFeed: str = "true"
    pipelines_changeDataCaptureMode: str = "TRACK_CHANGES"
    
    def as_dict(self) -> Dict[str, str]:
        """Convert to dictionary format for table properties.
        
        Returns:
            Dictionary with table properties
        """
        return {
            "delta.enableDeletionVectors": self.delta_enableDeletionVectors,
            "delta.enableRowTracking": self.delta_enableRowTracking,
            "delta.enableChangeDataFeed": self.delta_enableChangeDataFeed,
            "pipelines.changeDataCaptureMode": self.pipelines_changeDataCaptureMode,
        }


@dataclass(frozen=True)
class InternalAuditColumns:
    """Standard audit columns added by the framework.
    
    These columns track metadata about data ingestion and processing.
    
    Attributes:
        metadata_ldp: Metadata column containing ingest timestamp and source system
        ingest_timestamp: Timestamp when data was ingested
        source_system: Name of the source system
    """
    
    metadata_ldp: str = "_metadata_ldp"
    ingest_timestamp: str = "_metadata_ldp.ingest_timestamp"
    source_system: str = "_metadata_ldp.SourceSystem"
    
    def as_dict(self) -> Dict[str, str]:
        """Convert to dictionary format.
        
        Returns:
            Dictionary with audit column names
        """
        return {
            "metadata_ldp": self.metadata_ldp,
            "ingest_timestamp": self.ingest_timestamp,
            "source_system": self.source_system,
        }


__all__ = [
    "TableConfig",
    "LayerConfig",
    "DefaultTblProperties",
    "InternalAuditColumns",
]
