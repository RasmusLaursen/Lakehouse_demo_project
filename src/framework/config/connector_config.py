"""Connector-specific configuration wrapper."""
from typing import Any, Dict, Optional, List
from src.framework.helper import logging_helper

logger = logging_helper.get_logger(__name__)


class ConnectorConfig:
    """Wraps and manages connector-specific configuration.
    
    Provides clean API for getting, setting, and merging connector config
    while maintaining type safety and logging.
    """
    
    def __init__(self, connector_type: str, config_dict: Optional[Dict[str, Any]] = None):
        """Initialize ConnectorConfig.
        
        Args:
            connector_type: Type of connector (rest_api, volume, jdbc, etc.)
            config_dict: Initial configuration dictionary (optional)
        """
        self.connector_type = connector_type
        self._config = config_dict or {}
        logger.debug(f"Created ConnectorConfig for type '{connector_type}' with keys: {list(self._config.keys())}")
    
    def get(self, key: str, default: Any = None) -> Any:
        """Get configuration value.
        
        Args:
            key: Configuration key
            default: Default value if key not found
            
        Returns:
            Configuration value or default
        """
        return self._config.get(key, default)
    
    def set(self, key: str, value: Any) -> None:
        """Set configuration value.
        
        Args:
            key: Configuration key
            value: Configuration value
        """
        self._config[key] = value
        logger.debug(f"Set config key '{key}' = {type(value).__name__}")
    
    def merge(self, other: Dict[str, Any]) -> 'ConnectorConfig':
        """Merge additional configuration.
        
        Creates a new ConnectorConfig instance with merged configuration.
        Existing values are overridden by new values.
        
        Args:
            other: Dictionary of configuration to merge
            
        Returns:
            New ConnectorConfig instance with merged config
        """
        merged = {**self._config, **other}
        logger.debug(f"Merged config, new keys: {list(set(merged.keys()) - set(self._config.keys()))}")
        return ConnectorConfig(self.connector_type, merged)
    
    def extract_secrets(self) -> List[str]:
        """Extract and remove secret keys list.
        
        Returns:
            List of secret key names to resolve
        """
        secret_keys = self._config.pop("secret_keys", [])
        if secret_keys:
            logger.debug(f"Extracted {len(secret_keys)} secret keys: {secret_keys}")
        return secret_keys
    
    def to_dict(self) -> Dict[str, Any]:
        """Export configuration as dictionary.
        
        Returns:
            Copy of internal configuration dictionary
        """
        return self._config.copy()
    
    @property
    def source_type(self) -> str:
        """Get source type from configuration.
        
        Returns:
            Source type (default: 'volume')
        """
        return self._config.get("source_type", "volume")
    
    def __repr__(self) -> str:
        """String representation of ConnectorConfig."""
        return f"ConnectorConfig(type='{self.connector_type}', keys={list(self._config.keys())})"
    
    @staticmethod
    def from_server_config(server_config: Any) -> 'ConnectorConfig':
        """Create ConnectorConfig from data contract server configuration.
        
        Extracts connector_type, connector_config, and loadtype from server customProperties.
        
        Args:
            server_config: Data contract server configuration object
            
        Returns:
            ConnectorConfig instance
        """
        connector_type = "volume"  # Default
        config_dict = {}
        
        if not server_config:
            logger.debug("No server config provided, returning default ConnectorConfig")
            return ConnectorConfig(connector_type, config_dict)
        
        if hasattr(server_config, 'customProperties') and server_config.customProperties:
            for prop in server_config.customProperties:
                prop_name = getattr(prop, 'property', getattr(prop, 'key', None))
                prop_value = getattr(prop, 'value', None)
                
                if prop_name == "connector_type":
                    connector_type = prop_value
                    logger.info(f"Set connector type from server config: {prop_value}")
                elif prop_name == "connector_config" and isinstance(prop_value, dict):
                    config_dict.update(prop_value)
                    logger.debug(f"Merged connector_config from server: {list(prop_value.keys())}")
                elif prop_name == "loadtype":
                    config_dict["loadtype"] = prop_value
                    logger.debug(f"Set loadtype: {prop_value}")
                elif prop_name == "secret_keys": # and isinstance(prop_value, list):
                    config_dict.setdefault("secret_keys", []).extend(prop_value)
                    logger.info(f"Added secret_keys from server config: {prop_value}")
        
        logger.info(f"Created ConnectorConfig from server config: type={connector_type}, keys={list(config_dict.keys())}")
        return ConnectorConfig(connector_type, config_dict)
