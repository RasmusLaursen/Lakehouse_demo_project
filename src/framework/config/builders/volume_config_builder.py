"""Volume connector configuration builder."""
from typing import Any, Optional
from src.framework.config.builders.base_config_builder import BaseConfigBuilder
from src.framework.helper import logging_helper

logger = logging_helper.get_logger(__name__)


class VolumeConfigBuilder(BaseConfigBuilder):
    """Builder for volume connector configuration.
    
    Volume connectors read data from Databricks Volumes in Unity Catalog.
    Adds: catalog, schema, volume name, and format from centralized config.
    """
    
    def merge_shared_context(self) -> 'VolumeConfigBuilder':
        """Add volume-specific context from CentralizedPipelineConfig.
        
        Adds:
        - catalog: Landing catalog for volume storage
        - schema: Landing schema for volume organization
        - format: Data format (parquet, csv, json, etc.)
        - volume: Volume name based on model (optional, set by factory)
        
        Returns:
            Self for method chaining
        """
        context = {
            "catalog": self.pipeline_config.landing_catalog,
            "schema": self.pipeline_config.landing_schema,
            "format": self.pipeline_config.filetype,
        }
        
        logger.debug(f"Adding volume context: catalog={context.get('catalog')}, schema={context.get('schema')}")
        
        # Only set if not already configured
        context_to_add = {k: v for k, v in context.items() if k not in self.config.to_dict()}
        if context_to_add:
            self.config = self.config.merge(context_to_add)
            logger.info(f"Added {len(context_to_add)} volume context items")
        
        return self
