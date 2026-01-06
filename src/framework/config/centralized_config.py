"""Centralized pipeline configuration - shared metadata across all layers."""
from dataclasses import dataclass
from typing import Optional
from pyspark.sql import SparkSession
from src.framework.helper import databricks_helper, logging_helper

logger = logging_helper.get_logger(__name__)


@dataclass
class CentralizedPipelineConfig:
    """Shared configuration for all pipeline layers (raw, base, curated, enriched).
    
    This dataclass provides centralized, type-safe access to pipeline-level metadata
    that is shared across all connector types and data processing layers.
    """
    
    # Source system context
    source_system_name: str
    environment: str
    
    # Catalogs - shared across all connector types
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
    
    # Default configuration - will be overridden by data contracts
    filetype: str = "parquet"
    loadtype: str = "volume_autoloader"
    
    @classmethod
    def from_spark(cls, spark: SparkSession, source_system_name: str = "lakehouse") -> 'CentralizedPipelineConfig':
        """Create CentralizedPipelineConfig from Spark configuration.
        
        Args:
            spark: Active SparkSession
            source_system_name: Name of the source system (e.g., 'lakehouse', 'review')
            
        Returns:
            CentralizedPipelineConfig instance with all configuration loaded
            
        Example:
            >>> config = CentralizedPipelineConfig.from_spark(spark, "lakehouse")
            >>> print(config.base_catalog)
            'dev_base'
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
            
            # Default configuration
            filetype="parquet",
            loadtype="volume_autoloader",
        )
    
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
    
    def get_secrets_from_config(self, secret_keys: dict) -> dict:
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
