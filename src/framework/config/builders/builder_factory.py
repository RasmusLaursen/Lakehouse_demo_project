"""Factory for creating appropriate connector configuration builders."""
from typing import Type
from src.framework.config.connector_config import ConnectorConfig
from src.framework.config.centralized_config import CentralizedPipelineConfig
from src.framework.config.builders.base_config_builder import BaseConfigBuilder
from src.framework.config.builders.volume_config_builder import VolumeConfigBuilder
from src.framework.config.builders.rest_api_config_builder import RestApiConfigBuilder
from src.framework.config.builders.jdbc_config_builder import JdbcConfigBuilder
from src.framework.config.builders.autoloader_config_builder import AutoLoaderConfigBuilder
from src.framework.helper import logging_helper

logger = logging_helper.get_logger(__name__)

# Mapping of connector types to builder classes
BUILDER_MAPPING = {
    "volume": VolumeConfigBuilder,
    "autoloader": AutoLoaderConfigBuilder,
    "rest_api": RestApiConfigBuilder,
    "rest_api_ds": RestApiConfigBuilder,
    "rest_api_workflow_ds": RestApiConfigBuilder,
    "jdbc": JdbcConfigBuilder,
}


class ConnectorConfigBuilderFactory:
    """Factory for creating connector-specific configuration builders.
    
    Instantiates the appropriate builder class based on connector type.
    Single entry point for builder creation throughout the application.
    """
    
    @staticmethod
    def create_builder(
        connector_type: str,
        base_config: ConnectorConfig,
        centralized_config: CentralizedPipelineConfig,
        model_name: str = None,
    ) -> BaseConfigBuilder:
        """Create a configuration builder for the specified connector type.
        
        Args:
            connector_type: Type of connector (volume, rest_api, jdbc, autoloader, etc.)
            base_config: Base connector configuration
            centralized_config: Centralized pipeline configuration
            model_name: Optional model/schema name for per-schema configuration
            
        Returns:
            Instance of appropriate builder class (VolumeConfigBuilder, RestApiConfigBuilder, etc.)
            
        Raises:
            ValueError: If connector type is not recognized
            
        Example:
            >>> builder = ConnectorConfigBuilderFactory.create_builder(
            ...     "rest_api_workflow_ds",
            ...     base_config,
            ...     centralized_config,
            ...     "customer"
            ... )
            >>> config = builder.merge_schema_overrides(schema).build()
        """
        builder_class = BUILDER_MAPPING.get(connector_type)
        
        if not builder_class:
            logger.error(f"Unknown connector type: {connector_type}")
            raise ValueError(f"No builder found for connector type: {connector_type}")
        
        logger.info(f"Creating {builder_class.__name__} for connector type: {connector_type}")
        
        # AutoLoader builder accepts model_name parameter for per-schema volume mapping
        if connector_type == "autoloader":
            return builder_class(base_config, centralized_config, model_name)
        else:
            return builder_class(base_config, centralized_config)
    
    @staticmethod
    def register_builder(connector_type: str, builder_class: Type[BaseConfigBuilder]) -> None:
        """Register a custom builder for a connector type.
        
        Allows extension with new connector types without modifying factory code.
        
        Args:
            connector_type: Name of the connector type
            builder_class: Builder class that extends BaseConfigBuilder
            
        Example:
            >>> class CustomConnectorBuilder(BaseConfigBuilder):
            ...     def merge_shared_context(self):
            ...         # Custom implementation
            ...         return self
            >>> ConnectorConfigBuilderFactory.register_builder("custom", CustomConnectorBuilder)
        """
        if not issubclass(builder_class, BaseConfigBuilder):
            raise TypeError(f"Builder class must extend BaseConfigBuilder, got {builder_class}")
        
        BUILDER_MAPPING[connector_type] = builder_class
        logger.info(f"Registered builder {builder_class.__name__} for connector type: {connector_type}")
    
    @staticmethod
    def get_registered_types() -> list:
        """Get list of all registered connector types.
        
        Returns:
            List of connector type strings
        """
        return list(BUILDER_MAPPING.keys())
