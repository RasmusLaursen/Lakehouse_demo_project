"""Pipeline configuration models."""
from dataclasses import dataclass
from typing import Dict, Optional, Any, List
from pyspark.sql import SparkSession
from src.framework.helper import databricks_helper, logging_helper

logger = logging_helper.get_logger(__name__)


@dataclass
class PipelineConfig:
    """Unified configuration for all pipeline layers (raw, base, curated).
    
    This dataclass provides type-safe access to all lakehouse configuration,
    eliminating the need for repeated configuration loading in each file.
    """
    
    # Source system context
    source_system_name: str
    environment: str
    
    # Catalogs
    landing_catalog: str
    raw_catalog: str
    base_catalog: str
    curated_catalog: str
    enriched_catalog: str
    
    # Schemas - layer-specific
    landing_schema: str
    raw_schema: str
    base_schema: str
    dimensions_schema: str
    facts_schema: str
    enriched_schema: str
    
    # Server config (for raw/base layers)
    filetype: str
    loadtype: str
    
    # Connector configuration (for extensible data sources)
    connector_type: str = "volume"  # Default to volume for backward compatibility
    connector_config: Optional[Dict[str, Any]] = None  # Connector-specific configuration
    
    @classmethod
    def from_spark(cls, spark: SparkSession, source_system_name: str = "lakehouse") -> 'PipelineConfig':
        """Create PipelineConfig from Spark configuration.
        
        Args:
            spark: Active SparkSession
            source_system_name: Name of the source system (e.g., 'lakehouse', 'review')
            
        Returns:
            PipelineConfig instance with all configuration loaded
            
        Example:
            >>> config = PipelineConfig.from_spark(spark, "lakehouse")
            >>> print(config.base_catalog)
            'dev_base'
            >>> print(config.dimensions_schema)
            'dimensions'
        """
        environment = spark.conf.get("environment", "dev")
        
        catalogs = databricks_helper.get_pipeline_configurations(spark, "catalogs")
        schemas = databricks_helper.get_pipeline_configurations(spark, "schemas")
        
        logger.debug(f"Catalogs configuration: {catalogs}")
        logger.debug(f"Schemas configuration: {schemas}")
        
        # Get source-system-specific base schema name
        base_schema_key = f"{source_system_name}_base_schema"
        base_schema = schemas.get(base_schema_key)
        
        if not base_schema:
            logger.warning(
                f"Base schema key '{base_schema_key}' not found in configuration. "
                f"Available schemas: {list(schemas.keys())}"
            )
            base_schema = f"{source_system_name}_base"  # Fallback
        
        return cls(
            source_system_name=source_system_name,
            environment=environment or "dev",
            
            # Catalogs
            landing_catalog=catalogs.get("landing_catalog", "landing"),
            raw_catalog=catalogs.get("raw_catalog", "raw"),
            base_catalog=catalogs.get("base_catalog", "base"),
            curated_catalog=catalogs.get("curated_catalog", "curated"),
            enriched_catalog=catalogs.get("enriched_catalog", "enriched"),
            
            # Schemas - source-system-specific for raw/base
            landing_schema=schemas.get(f"{source_system_name}_landing_schema", f"{source_system_name}_landing"),
            raw_schema=schemas.get(f"{source_system_name}_raw_schema", f"{source_system_name}_raw"),
            base_schema=base_schema,
            
            # Schemas - curated layer (shared across source systems)
            dimensions_schema=schemas.get("dimensions_schema", "dimensions"),
            facts_schema=schemas.get("facts_schema", "facts"),
            enriched_schema=schemas.get("enriched_schema", "enriched"),
            
            # Default server config - will be overridden by data contract
            filetype="parquet",
            loadtype="volume_autoloader",
            
            # Default connector config
            connector_type="volume",
            connector_config={}
        )
    
    def update_from_server_config(self, server_config: Any) -> None:
        """Update configuration from data contract server config.
        
        Args:
            server_config: Server configuration from data contract
        """
        if server_config:
            if hasattr(server_config, "format"):
                self.filetype = server_config.format
            
            if hasattr(server_config, "customProperties") and server_config.customProperties:
                # First pass: process connector_config and secret_keys separately
                connector_config_value = None
                secret_keys_value = None
                
                for prop in server_config.customProperties:
                    if hasattr(prop, 'property'):
                        prop_name = getattr(prop, 'property', getattr(prop, 'key', None))
                        prop_value = getattr(prop, 'value', None)
                        
                        if prop_name == "loadtype":
                            self.loadtype = prop_value
                        elif prop_name == "connector_type":
                            self.connector_type = prop_value
                            logger.info(f"Set connector type to: {prop_value}")
                        elif prop_name == "connector_config":
                            connector_config_value = prop_value
                        elif prop_name == "secret_keys":
                            secret_keys_value = prop_value
                
                # Second pass: merge connector_config and secret_keys to ensure both are preserved
                if connector_config_value and isinstance(connector_config_value, dict):
                    self.connector_config.update(connector_config_value)
                    logger.info(f"Merged connector config: {self.connector_config}")
                
                if secret_keys_value and isinstance(secret_keys_value, list):
                    self.connector_config.setdefault("secret_keys", []).extend(secret_keys_value)
                    logger.info(f"Added server-level secret_keys to connector config: {secret_keys_value}")
    
    # Helper methods for table paths
    
    def get_base_table_path(self, table_name: str) -> str:
        """Get fully qualified path to a base layer table.
        
        Args:
            table_name: Name of the table in base layer
            
        Returns:
            Fully qualified table path
            
        Example:
            >>> config.get_base_table_path("customer")
            'dev_base.lakehouse_base.customer'
        """
        return f"{self.base_catalog}.{self.base_schema}.{table_name}"
    
    def get_dimension_table_path(self, dimension_name: str) -> str:
        """Get fully qualified path to a dimension table.
        
        Args:
            dimension_name: Name of the dimension (e.g., 'dim_customer')
            
        Returns:
            Fully qualified table path
            
        Example:
            >>> config.get_dimension_table_path("dim_customer")
            'dev_curated.dimensions.dim_customer'
        """
        return f"{self.curated_catalog}.{self.dimensions_schema}.{dimension_name}"
    
    def get_fact_table_path(self, fact_name: str) -> str:
        """Get fully qualified path to a fact table.
        
        Args:
            fact_name: Name of the fact (e.g., 'fact_sales')
            
        Returns:
            Fully qualified table path
            
        Example:
            >>> config.get_fact_table_path("fact_sales")
            'dev_curated.facts.fact_sales'
        """
        return f"{self.curated_catalog}.{self.facts_schema}.{fact_name}"
    
    def get_connector(self, model_name: str, schema: Any = None):
        """Create a connector instance with config merged from data contract and context.
        
        This method intelligently merges connector configuration:
        - Base config from data contract server (connector_type, connector_config)
        - Schema-level overrides (pagination_config, etc.)
        - Context-specific parameters (catalog, schema, volume) ONLY for volume sources
        - Model-specific naming (volume name based on model_name)
        
        Args:
            model_name: Name of the model/table (used for volume naming)
            schema: Optional schema object for schema-level config overrides
            
        Returns:
            Configured connector instance ready to read data
            
        Example:
            >>> connector = config.get_connector("customer", schema)
            >>> df = connector.read_stream(spark)
        """
        from src.framework.connectors import ConnectorFactory
        from src.framework.helper import data_contract_helper
        
        # Start with connector config from data contract server - use only what's defined
        connector_config = (self.connector_config or {}).copy()
        
        # Merge schema-level properties (like pagination_config) if schema provided
        if schema and hasattr(schema, 'customProperties') and schema.customProperties:
            for prop in schema.customProperties:
                prop_name = getattr(prop, 'property', getattr(prop, 'key', None))
                prop_value = getattr(prop, 'value', None)
                
                if prop_name == "pagination_config":
                    # Merge schema-level pagination_config with server-level
                    connector_config["pagination_config"] = prop_value
                    logger.info(f"Applied schema-level pagination_config for {model_name}: {prop_value}")
                elif prop_name == "params":
                    # Merge schema-level params with server-level (schema-level takes precedence)
                    server_params = connector_config.get("params", {})
                    if isinstance(server_params, dict) and isinstance(prop_value, dict):
                        merged_params = {**server_params, **prop_value}
                        connector_config["params"] = merged_params
                        logger.info(f"Merged schema-level params for {model_name}: {merged_params}")
                    else:
                        connector_config["params"] = prop_value
                        logger.info(f"Applied schema-level params for {model_name}: {prop_value}")
                elif prop_name == "secret_keys":
                    # Schema-level secret_keys to be resolved
                    if isinstance(prop_value, list):
                        connector_config.setdefault("secret_keys", []).extend(prop_value)
                        logger.info(f"Applied schema-level secret_keys for {model_name}: {prop_value}")
                elif prop_name in ["mode", "timestamp_field", "timestamp_param", "initial_timestamp", "table_name"]:
                    # Pass streaming-related config and table_name directly to connector
                    connector_config[prop_name] = prop_value
                    logger.info(f"Applied schema-level {prop_name} for {model_name}: {prop_value}")
        
        # For volume-based connectors ONLY, merge in catalog/schema/volume
        # Other connector types (REST API, JDBC, etc.) should use only their defined config
        source_type = connector_config.get("source_type", "volume")
        if source_type == "volume":
            connector_config.setdefault("catalog", self.landing_catalog)
            connector_config.setdefault("schema", self.landing_schema)
            connector_config.setdefault("volume", f"{model_name}_contract")
            connector_config.setdefault("format", self.filetype)
        
        # For REST API connectors, add table_name for endpoint construction
        if self.connector_type in ["rest_api", "rest_api_ds"]:
            connector_config.setdefault("table_name", model_name)
            
        # For REST API connectors with schema, build schema from contract properties
            # This avoids needing to infer schema from API at connector creation time
            if schema and hasattr(schema, 'properties') and schema.properties:
                try:
                    from src.framework.helper.data_contract_helper import schema_properties_to_spark_schema
                    spark_schema = schema_properties_to_spark_schema(schema)
                    connector_config["schema"] = spark_schema
                    logger.info(f"✓ Added Spark schema to REST API connector config for {model_name} with {len(spark_schema.fields)} fields: {[f.name for f in spark_schema.fields]}")
                except Exception as e:
                    logger.warning(f"✗ Could not build schema from contract properties for {model_name}: {e}. Will infer from API.")
            else:
                logger.warning(f"✗ REST API connector for {model_name} has no schema properties. Schema will be inferred from API response.")
        
        # Extract and resolve secrets from connector config
        # Secret keys should be declared in the connector_config via secret_keys property
        secret_keys = connector_config.pop("secret_keys", [])
        logger.info("testsd: " + str(secret_keys))
        if secret_keys:
            logger.info(f"Resolving {len(secret_keys)} secrets for {model_name} connector")
            try:
                self._resolve_connector_secrets(connector_config, secret_keys)
                logger.info(f"Successfully resolved all secrets for {model_name}")
            except ValueError as e:
                logger.error(f"Secret resolution failed for {model_name}: {e}")
                raise
        
        logger.info(f"Creating {self.connector_type} connector for {model_name} with config keys: {list(connector_config.keys())}")
        
        return ConnectorFactory.create(self.connector_type, connector_config)
    
    def _resolve_secret_reference(self, reference: str) -> str:
        """Resolve a single secret reference to its actual value.
        
        Supports formats:
        - {{spark.config-key}}: Spark configuration value (for DLT pipelines)
        - {{secrets/scope/key}}: Databricks secret reference
        - secret://scope/key: Databricks secret reference
        - Plain string: Returns as-is (not a secret reference)
        
        Args:
            reference: Secret reference or plain value
            
        Returns:
            Resolved secret value
            
        Raises:
            ValueError: If secret reference cannot be resolved
        """
        if not reference or not isinstance(reference, str):
            return reference
        
        # Handle "{{spark.config-key}}" format (Spark config for DLT pipelines)
        if reference.startswith("{{spark.") and reference.endswith("}}"):
            config_key = reference[8:-2]  # Remove {{spark. and }}
            logger.info(f"Resolving {{{{spark.{config_key}}}}} from Spark config")
            try:
                spark = databricks_helper.get_spark()
                if spark:
                    # Try with and without spark. prefix
                    value = spark.conf.get(f"spark.{config_key}", None)
                    if not value:
                        value = spark.conf.get(config_key, None)
                    
                    if value:
                        logger.info(f"Successfully resolved {{{{spark.{config_key}}}}} from Spark config (length: {len(str(value))})")
                        return value
                    else:
                        logger.warning(f"Spark config key 'spark.{config_key}' or '{config_key}' not found")
                else:
                    logger.warning(f"Could not get Spark session to resolve {{{{spark.{config_key}}}}}")
            except Exception as e:
                logger.warning(f"Exception resolving {{{{spark.{config_key}}}}}: {e}")
            
            raise ValueError(f"Could not resolve {{{{spark.{config_key}}}}} - key not found in Spark config")
        
        # Handle "secret://scope/key" format
        if reference.startswith("secret://"):
            path = reference.replace("secret://", "")
            if "/" in path:
                scope, key = path.split("/", 1)
                logger.debug(f"Resolving secret: scope='{scope}', key='{key}'")
                return self._get_dbutils_secret(scope, key)
        
        # Handle "{{secrets/scope/key}}" format
        if reference.startswith("{{secrets/") and reference.endswith("}}"):
            path = reference[10:-2]  # Remove {{secrets/ and }}
            if "/" in path:
                scope, key = path.split("/", 1)
                logger.debug(f"Resolving secret: scope='{scope}', key='{key}'")
                return self._get_dbutils_secret(scope, key)
        
        # Not a secret reference, return as-is
        return reference
    
    def _get_dbutils_secret(self, scope: str, key: str) -> str:
        """Get a secret from Databricks secrets using dbutils.
        
        Args:
            scope: Secret scope name
            key: Secret key name
            
        Returns:
            Secret value
            
        Raises:
            ValueError: If secret cannot be accessed
        """
        try:
            # Try direct dbutils access (works in Databricks notebooks)
            try:
                dbutils = globals().get('dbutils')
                if dbutils:
                    return dbutils.secrets.get(scope, key)
            except:
                pass
            
            # Try via PySpark context
            from pyspark.sql import SparkSession
            spark = SparkSession.getActiveSession()
            if not spark:
                spark = SparkSession.builder.getOrCreate()
            
            if spark:
                try:
                    # Access via Spark context
                    sc = spark.sparkContext
                    return sc.parallelize([1]).map(lambda x: __import__('dbutils').secrets.get(scope, key)).collect()[0]
                except:
                    pass
            
            raise ValueError(f"Could not access dbutils for secret resolution")
        except Exception as e:
            logger.error(f"Failed to resolve secret {scope}/{key}: {e}")
            raise ValueError(f"Failed to resolve secret {scope}/{key}: {e}")
    
    def _resolve_connector_secrets(self, connector_config: Dict[str, Any], secret_keys: List[str]) -> Dict[str, Any]:
        """Resolve all declared secret keys in connector configuration.
        
        Resolves each key in secret_keys list, replacing the reference with the actual secret value.
        Fails fast if any secret cannot be resolved.
        
        Args:
            connector_config: Configuration dictionary for the connector
            secret_keys: List of config keys that contain secret references (e.g., ["auth_token", "auth_token_key"])
            
        Returns:
            Updated connector_config with secrets resolved (modifies in place and returns)
            
        Raises:
            ValueError: If any secret cannot be resolved
        """
        if not secret_keys:
            return connector_config
        
        logger.debug(f"Resolving {len(secret_keys)} secret keys: {secret_keys}")
        
        for key in secret_keys:
            if key in connector_config:
                value = connector_config[key]
                if value and isinstance(value, str):
                    try:
                        logger.info(f"Attempting to resolve secret key '{key}' with value: {value[:50]}..." if len(str(value)) > 50 else f"Attempting to resolve secret key '{key}' with value: {value}")
                        resolved = self._resolve_secret_reference(value)
                        connector_config[key] = resolved
                        logger.info(f"Successfully resolved secret key '{key}' to value of length {len(str(resolved))}")
                    except Exception as e:
                        logger.error(f"Failed to resolve secret key '{key}' with value '{value}': {e}")
                        raise ValueError(f"Failed to resolve secret key '{key}' in connector config: {e}")
                else:
                    logger.debug(f"Secret key '{key}' is not a string, skipping resolution")
            else:
                logger.warning(f"Secret key '{key}' not found in connector config")
        
        return connector_config
    
    def get_secret_from_spark_config(self, key: str) -> Optional[str]:
        """Get a secret value from Spark configuration.
        
        In DLT pipelines, secrets can be passed via Spark configuration:
        spark.{key}: "{{secrets/scope/key}}"
        
        Databricks automatically interpolates {{secrets/*}} at pipeline startup,
        so we just need to read the pre-interpolated value from spark.conf.
        
        Args:
            key: Configuration key name (without 'spark.' prefix)
            
        Returns:
            Secret value from Spark config, or None if not found
            
        Example:
            Pipeline config: spark.eloverblik-api-token: "{{secrets/scope-demo-dev/eloverblik-api-token}}"
            Call: config.get_secret_from_spark_config("eloverblik-api-token")
        """
        try:
            spark_key = f"spark.{key}"
            spark = databricks_helper.get_spark()
            secret_value = spark.conf.get(spark_key, None)
            
            if secret_value:
                logger.debug(f"Retrieved secret from Spark config: {key}")
                return secret_value
            else:
                logger.debug(f"Secret not found in Spark config: {key}")
                return None
                
        except Exception as e:
            logger.warning(f"Error retrieving secret from Spark config ({key}): {e}")
            return None
    
    def get_secrets_from_config(self, secret_keys: Dict[str, str]) -> Dict[str, Optional[str]]:
        """Get multiple secrets from configuration.
        
        Resolves all provided secrets from Spark config in a single call.
        Useful for connectors that need multiple secrets (API keys, tokens, etc).
        
        Args:
            secret_keys: Dictionary mapping secret names to config key names
                        Example: {"api_token": "eloverblik-api-token", "refresh_token": "eloverblik-refresh"}
            
        Returns:
            Dictionary mapping secret names to their resolved values (or None if not found)
            
        Example:
            ```python
            secrets = config.get_secrets_from_config({
                "api_token": "eloverblik-api-token",
                "refresh_token": "eloverblik-refresh"
            })
            api_token = secrets.get("api_token")
            ```
        """
        resolved_secrets = {}
        
        for secret_name, config_key in secret_keys.items():
            try:
                value = self.get_secret_from_spark_config(config_key)
                resolved_secrets[secret_name] = value
                logger.debug(f"Resolved secret '{secret_name}' from config key '{config_key}': {'found' if value else 'not found'}")
            except Exception as e:
                logger.warning(f"Error resolving secret '{secret_name}' from config key '{config_key}': {e}")
                resolved_secrets[secret_name] = None
        
        return resolved_secrets
    
    def validate(self) -> bool:
        """Validate that all required configuration is present.
        
        Returns:
            True if valid, raises ValueError otherwise
            
        Raises:
            ValueError: If any required configuration is missing
        """
        required_fields = [
            ("base_catalog", self.base_catalog),
            ("curated_catalog", self.curated_catalog),
            ("base_schema", self.base_schema),
            ("dimensions_schema", self.dimensions_schema),
            ("facts_schema", self.facts_schema),
        ]
        
        missing = [name for name, value in required_fields if not value]
        
        if missing:
            raise ValueError(
                f"Missing required configuration fields: {', '.join(missing)}"
            )
        
        return True
