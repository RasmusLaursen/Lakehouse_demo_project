"""
Connector factory for instantiating data source connectors.

This module provides a registry pattern for creating connector instances
based on connector type strings from data contracts.
"""

from typing import Dict, Any, Type
from src.framework.connectors.base_connector import BaseConnector


class ConnectorFactory:
    """
    Factory class for creating connector instances.
    
    Maintains a registry of connector types and instantiates
    the appropriate connector based on configuration.
    """
    
    # Registry of connector types to connector classes
    _connectors: Dict[str, Type[BaseConnector]] = {}
    
    @classmethod
    def register(cls, connector_type: str, connector_class: Type[BaseConnector]) -> None:
        """
        Register a connector class for a specific type.
        
        Args:
            connector_type: String identifier for the connector type (e.g., "volume", "rest_api")
            connector_class: The connector class to register
            
        Example:
            ConnectorFactory.register("volume", VolumeConnector)
        """
        cls._connectors[connector_type.lower()] = connector_class
    
    @classmethod
    def create(cls, connector_type: str, config: Dict[str, Any]) -> BaseConnector:
        """
        Create a connector instance based on type and configuration.
        
        Args:
            connector_type: String identifier for the connector type
            config: Connector-specific configuration dictionary
            
        Returns:
            Instance of the appropriate connector class
            
        Raises:
            ValueError: If connector_type is not registered
            
        Example:
            connector = ConnectorFactory.create("volume", {"path": "/mnt/data", "format": "json"})
        """
        connector_type_lower = connector_type.lower()
        
        if connector_type_lower not in cls._connectors:
            registered = ", ".join(cls._connectors.keys())
            raise ValueError(
                f"Unknown connector type: '{connector_type}'. "
                f"Registered connectors: {registered}"
            )
        
        connector_class = cls._connectors[connector_type_lower]
        return connector_class(config)
    
    @classmethod
    def get_registered_types(cls) -> list[str]:
        """
        Get list of all registered connector types.
        
        Returns:
            List of registered connector type strings
        """
        return list(cls._connectors.keys())
    
    @classmethod
    def is_registered(cls, connector_type: str) -> bool:
        """
        Check if a connector type is registered.
        
        Args:
            connector_type: String identifier for the connector type
            
        Returns:
            True if the connector type is registered, False otherwise
        """
        return connector_type.lower() in cls._connectors
