"""AutoLoader connector configuration builder."""
from typing import Any, Optional
from src.framework.config.builders.base_config_builder import BaseConfigBuilder
from src.framework.helper import logging_helper

logger = logging_helper.get_logger(__name__)


class AutoLoaderConfigBuilder(BaseConfigBuilder):
    """Builder for AutoLoader connector configuration.
    
    AutoLoader connectors use Databricks AutoLoader for incremental data ingestion
    from cloud storage (S3, ADLS, GCS).
    Future: Will add cloud path management, credentials, checkpoint handling.
    """
    
    def __init__(self, base_config, centralized_config, model_name: str = None):
        """Initialize AutoLoaderConfigBuilder.
        
        Args:
            base_config: Base ConnectorConfig from server configuration
            centralized_config: CentralizedPipelineConfig with shared metadata
            model_name: Optional model/schema name for per-schema volume mapping
        """
        super().__init__(base_config, centralized_config)
        self.model_name = model_name
    
    def merge_shared_context(self) -> 'AutoLoaderConfigBuilder':
        """Add AutoLoader-specific context from CentralizedPipelineConfig.
        
        Maps landing layer location to AutoLoader volume source configuration:
        - catalog: landing_catalog
        - schema: landing_schema  
        - format: filetype (from config)
        - volume: per-schema volume mapping if model_name provided, else source_system_name
        
        For volume-based AutoLoader with per-schema volumes:
        - If model_name is provided, sets volume to model_name (each schema reads own volume)
        - Otherwise, defaults to source_system_name
        
        For other source types (s3, adls, gcs), path configuration would be added here.
        
        Returns:
            Self for method chaining
        """
        # Add landing layer paths for volume-based AutoLoader
        self.config.set("catalog", self.pipeline_config.landing_catalog)
        self.config.set("schema", self.pipeline_config.landing_schema)
        self.config.set("format", self.pipeline_config.filetype)
        
        # Set volume name (per-schema mapping if model_name provided)
        source_type = self.config.get("source_type", "volume")
        if source_type == "volume":
            # Only set if not already provided in schema-level overrides
            if "volume" not in self.config.to_dict():
                # Use model_name (schema name) if provided for per-schema volumes
                # Otherwise use source system name as fallback
                volume_name = self.model_name or self.pipeline_config.source_system_name
                self.config.set("volume", volume_name)
        
        logger.debug(f"Added AutoLoader context: catalog={self.pipeline_config.landing_catalog}, "
                    f"schema={self.pipeline_config.landing_schema}, format={self.pipeline_config.filetype}, "
                    f"volume={self.config.get('volume')}")
        return self
