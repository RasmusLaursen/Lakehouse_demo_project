"""REST API connector configuration builder."""
from typing import Any, Optional
from src.framework.config.builders.base_config_builder import BaseConfigBuilder
from src.framework.helper import logging_helper

logger = logging_helper.get_logger(__name__)


class RestApiConfigBuilder(BaseConfigBuilder):
    """Builder for REST API connector configuration.
    
    REST API connectors fetch data from HTTP(S) endpoints.
    Adds:
    - raw_catalog/raw_schema: For Delta table reads in dependent calls
    - table_name: For endpoint construction
    - Spark schema: Built from data contract properties
    - OAuth2 tokens: Pre-loaded and cached
    """
    
    def merge_shared_context(self) -> 'RestApiConfigBuilder':
        """Add REST API-specific context from CentralizedPipelineConfig.
        
        For workflow datasources with dependent calls, adds raw_catalog/raw_schema
        to enable reading parent data from Delta tables.
        
        Returns:
            Self for method chaining
        """
        context = {}
        
        # Workflow datasources with dependent calls need raw catalog/schema for Delta reads
        if self.config.connector_type == "rest_api_workflow_ds":
            context = {
                "raw_catalog": self.pipeline_config.raw_catalog,
                "raw_schema": self.pipeline_config.raw_schema,
            }
            logger.debug(f"Adding REST API workflow context: raw_catalog={context.get('raw_catalog')}, raw_schema={context.get('raw_schema')}")
        
        # Only set if not already configured
        context_to_add = {k: v for k, v in context.items() if k not in self.config.to_dict()}
        if context_to_add:
            self.config = self.config.merge(context_to_add)
            logger.info(f"Added {len(context_to_add)} REST API context items")
        
        return self
    
    def build_spark_schema(self, model_name: str, schema: Optional[Any]) -> 'RestApiConfigBuilder':
        """Build Spark schema from data contract properties for REST API connectors.
        
        Converts data contract schema properties to Spark StructType for REST API connectors.
        This avoids needing to infer schema from API at connector creation time.
        
        For workflow datasources, caches the schema for later retrieval during dependent calls.
        
        Args:
            model_name: Name of the model/table
            schema: Data contract schema object
            
        Returns:
            Self for method chaining
        """
        if not schema or not hasattr(schema, 'properties') or not schema.properties:
            logger.warning(f"No schema properties available for {model_name}, will infer from API")
            return self
        
        try:
            from src.framework.helper.contracts import schema_properties_to_spark_schema
            spark_schema = schema_properties_to_spark_schema(schema)
            self.config.set("schema", spark_schema)
            logger.info(f"Built Spark schema for {model_name} with {len(spark_schema.fields)} fields: {[f.name for f in spark_schema.fields]}")
            
            # For workflow datasources, cache the schema for later retrieval
            if self.config.connector_type == "rest_api_workflow_ds":
                try:
                    from src.framework.connectors.rest_api_workflow_datasource import RestApiWorkflowDataSource
                    RestApiWorkflowDataSource.cache_schema(model_name, spark_schema)
                    logger.info(f"Cached schema for workflow datasource: {model_name}")
                except Exception as e:
                    logger.warning(f"Could not cache schema for workflow datasource: {e}")
        except Exception as e:
            logger.warning(f"Could not build schema from contract properties for {model_name}: {e}")
        
        return self
    
    def pre_load_oauth2_token(self, model_name: str) -> 'RestApiConfigBuilder':
        """Pre-load OAuth2 access token if configured.
        
        Exchanges refresh token for access token early to avoid repeated exchanges
        during schema inference and reader initialization.
        
        Args:
            model_name: Name of the model (for logging)
            
        Returns:
            Self for method chaining
        """
        auth_type = self.config.get("auth_type", "none").lower()
        if auth_type != "oauth2_refresh":
            return self
        
        refresh_token = self.config.get("auth_token")
        if not refresh_token:
            logger.warning(f"OAuth2 configured for {model_name} but no refresh token (auth_token) found")
            return self
        
        try:
            from src.framework.connectors.oauth2_token_manager import OAuth2TokenManager
            
            # Check if token is already cached
            cached_token = OAuth2TokenManager.get_cached_token(refresh_token)
            if cached_token:
                logger.info(f"OAuth2 token already cached for {model_name}, skipping exchange")
            else:
                # Exchange and cache token
                logger.info(f"Pre-loading OAuth2 access token for {model_name} connector")
                access_token = OAuth2TokenManager.exchange_token(
                    refresh_token=refresh_token,
                    token_endpoint=self.config.get("token_endpoint"),
                    token_method=self.config.get("token_method", "GET"),
                    token_response_path=self.config.get("token_response_path", "result")
                )
                logger.info(f"Successfully pre-loaded OAuth2 token for {model_name}")
        except Exception as e:
            logger.error(f"Failed to pre-load OAuth2 token for {model_name}: {e}")
            logger.warning(f"Connector will attempt token exchange on-demand during read operations")
        
        return self
