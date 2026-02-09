"""Base configuration builder with common logic for all connector types."""
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional
from src.framework.config.connector_config import ConnectorConfig
from src.framework.config.centralized_config import CentralizedPipelineConfig
from src.framework.config.secret_resolver import SecretResolver
from src.framework.helper import logging_helper

logger = logging_helper.get_logger(__name__)


class BaseConfigBuilder(ABC):
    """Abstract base class for connector configuration builders.
    
    Defines template method pattern with common steps:
    1. Merge schema-level overrides
    2. Add connector-specific context (abstract - implemented by subclasses)
    3. Resolve secrets
    4. Export final configuration
    
    Subclasses implement merge_shared_context() with connector-specific logic.
    """
    
    SCHEMA_LEVEL_PROPERTIES = {
        "pagination_config", "params", "secret_keys", "mode",
        "timestamp_field", "timestamp_param", "initial_timestamp",
        "table_name", "is_root_call", "workflow_step", "depends_on",
        "provides_dependencies", "url_params_template", "body_params_template",
        "method", "dependency_mapping","dependency_table", "static_url_params", "static_body_params",
        "data_path", "field_mapping", "include_parent_context"
    }
    
    def __init__(self, 
                 base_config: ConnectorConfig,
                 centralized_config: CentralizedPipelineConfig):
        """Initialize BaseConfigBuilder.
        
        Args:
            base_config: Base ConnectorConfig from server configuration
            centralized_config: CentralizedPipelineConfig with shared metadata
        """
        self.config = base_config
        self.pipeline_config = centralized_config
        self.secret_resolver = SecretResolver()
        logger.debug(f"Initialized {self.__class__.__name__} for {base_config.connector_type}")
    
    def merge_schema_overrides(self, schema: Optional[Any]) -> 'BaseConfigBuilder':
        """Merge schema-level property overrides into configuration.
        
        Processes schema customProperties and applies schema-level configuration
        that may override or extend server-level configuration.
        This is common to all connector types.
        
        Args:
            schema: Schema object with optional customProperties
            
        Returns:
            Self for method chaining
        """
        if not schema or not hasattr(schema, 'customProperties') or not schema.customProperties:
            logger.debug("No schema customProperties to merge")
            return self
        
        overrides = {}
        for prop in schema.customProperties:
            prop_name = getattr(prop, 'property', getattr(prop, 'key', None))
            prop_value = getattr(prop, 'value', None)
            
            if prop_name not in self.SCHEMA_LEVEL_PROPERTIES:
                continue
            
            # Special handling for merged configs
            if prop_name == "params":
                existing = self.config.get("params", {})
                if isinstance(existing, dict) and isinstance(prop_value, dict):
                    overrides["params"] = {**existing, **prop_value}
                    logger.debug(f"Merged params: {list(prop_value.keys())}")
                else:
                    overrides["params"] = prop_value
            elif prop_name == "secret_keys" and isinstance(prop_value, list):
                existing = self.config.get("secret_keys", [])
                overrides["secret_keys"] = existing + prop_value
                logger.debug(f"Extended secret_keys: {prop_value}")
            elif prop_name == "connector_config" and isinstance(prop_value, dict):
                existing = self.config.get("connector_config", {})
                overrides["connector_config"] = {**existing, **prop_value}
                logger.debug(f"Merged connector_config: {list(prop_value.keys())}")
            else:
                overrides[prop_name] = prop_value
                logger.debug(f"Set {prop_name} from schema")
        
        if overrides:
            self.config = self.config.merge(overrides)
            logger.info(f"Applied {len(overrides)} schema-level overrides")
        
        return self
    
    @abstractmethod
    def merge_shared_context(self) -> 'BaseConfigBuilder':
        """Add connector-specific context from CentralizedPipelineConfig.
        
        Subclasses implement this to add context appropriate for their connector type.
        Volume adds catalog/schema/format.
        REST API adds raw_catalog/raw_schema.
        JDBC adds host/port/database.
        etc.
        
        Returns:
            Self for method chaining
        """
        pass
    
    def resolve_secrets(self) -> 'BaseConfigBuilder':
        """Resolve all declared secret references in configuration.
        
        Extracts secret_keys list and resolves each secret reference to its actual value.
        Fails fast if any secret cannot be resolved.
        This is common to all connector types.
        
        Returns:
            Self for method chaining
            
        Raises:
            ValueError: If any secret cannot be resolved
        """
        secret_keys = self.config.extract_secrets()
        logger.info(f"Resolving secrets for keys: {secret_keys}")
        if not secret_keys:
            return self
        
        config_dict = self.config.to_dict()
        logger.info(f"Resolving {len(secret_keys)} secrets")
        
        for key in secret_keys:
            if key not in config_dict:
                logger.warning(f"Secret key '{key}' not found in configuration")
                continue
            
            value = config_dict[key]
            if not value or not isinstance(value, str):
                logger.debug(f"Secret key '{key}' is not a string, skipping")
                continue
            
            try:
                resolved = self.secret_resolver.resolve(value)
                self.config.set(key, resolved)
                logger.info(f"Successfully resolved secret key '{key}' to value of length {len(str(resolved))}")
            except ValueError as e:
                logger.error(f"Failed to resolve secret key '{key}': {e}")
                raise ValueError(f"Failed to resolve secret key '{key}' in connector config: {e}")
        
        return self
    
    def build(self) -> Dict[str, Any]:
        """Export final configuration as dictionary.
        
        Returns:
            Complete connector configuration ready for ConnectorFactory
        """
        config_dict = self.config.to_dict()
        logger.info(f"Built final config with {len(config_dict)} keys: {list(config_dict.keys())}")
        return config_dict
