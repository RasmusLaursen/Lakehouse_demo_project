"""JDBC connector configuration builder."""
from typing import Any, Optional
from src.framework.config.builders.base_config_builder import BaseConfigBuilder
from src.framework.helper import logging_helper

logger = logging_helper.get_logger(__name__)


class JdbcConfigBuilder(BaseConfigBuilder):
    """Builder for JDBC connector configuration.
    
    JDBC connectors connect to relational databases via JDBC drivers.
    Future: Will add host, port, database, credentials management.
    """
    
    def merge_shared_context(self) -> 'JdbcConfigBuilder':
        """Add JDBC-specific context from CentralizedPipelineConfig.
        
        Future implementation will add:
        - host: Database server hostname
        - port: Database connection port
        - database: Default database name
        - credentials: Database credentials from secrets
        
        Returns:
            Self for method chaining
        """
        logger.debug("No JDBC-specific context to add (not yet implemented)")
        return self
