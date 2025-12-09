"""Pipeline configuration models."""
from dataclasses import dataclass
from typing import Dict, Optional, Any
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
            loadtype="volume_autoloader"
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
                for prop in server_config.customProperties:
                    if hasattr(prop, 'key') and prop.key == "loadtype":
                        self.loadtype = prop.value
                        break
    
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
