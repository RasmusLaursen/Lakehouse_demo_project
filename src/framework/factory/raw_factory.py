"""Factory for creating raw layer DLT pipelines."""
from pyspark.sql import SparkSession
from typing import Optional, Any

try:
    import dlt  # type: ignore
except ImportError:
    dlt = None  # type: ignore

from src.framework.helper import (
    get_spark,
    get_dbutils,
    ldp_table,
    get_logger,
    get_validate_data_configuration_contract,
    find_data_contract_path,
    load_data_contract,
    get_data_contract,
    read_dataframe,
    schema_to_table_config,
)
from src.framework.config import (
    CentralizedPipelineConfig,
    ConnectorConfig,
    ConnectorConfigBuilderFactory,
    CatalogSchemaManager
)
from src.framework.connectors import ConnectorFactory

logger = get_logger(__name__)


class RawPipelineFactory:
    """Factory for creating raw layer ingestion pipelines."""
    
    def __init__(self, spark: SparkSession):
        """Initialize the factory.
        
        Args:
            spark: Active SparkSession
        """
        self.spark = spark
    
    def create_pipeline(self, source_system_name: str) -> None:
        """Create raw ingestion pipeline for a source system.
        
        This method creates DLT tables dynamically based on data contracts.
        It processes all schemas defined in the data contract and creates
        corresponding raw layer tables with optional backfill flows.
        
        Args:
            source_system_name: Name of the source system (e.g., 'lakehouse', 'review')
        """
        logger.info(f"Creating raw pipeline for source system: {source_system_name}")
        
        # Load centralized configuration
        centralized_config = CentralizedPipelineConfig.from_spark(self.spark, source_system_name)
        centralized_config.validate()
        
        # Create catalog/schema manager
        catalog_manager = CatalogSchemaManager.from_pipeline_config(centralized_config)
        
        # Load data contract
        data_contract = get_data_contract(
            catalog="source_system", 
            object_name=source_system_name
        )
        
        # Find server configuration for current environment
        server_config = self._get_server_config(data_contract, centralized_config.environment)
        
        # Process schemas in two passes:
        # Pass 1: Create all root call tables (independent)
        # Pass 2: Create all dependent call tables (depends on root tables being materialized)
        logger.info("=== PASS 1: Creating root call tables ===")
        for schema in data_contract.schema_:  # type: ignore
            try:
                is_root_call = True
                if hasattr(schema, 'customProperties') and schema.customProperties:
                    for prop in schema.customProperties:
                        prop_name = getattr(prop, 'property', getattr(prop, 'key', None))
                        prop_value = getattr(prop, 'value', None)
                        if prop_name == "is_root_call" and prop_value:
                            is_root_call = (prop_value == "true" or prop_value is True)
                
                if is_root_call:
                    logger.info(f"Processing root call schema: {schema.name if schema else 'unknown'}")
                    self._process_schema(schema, server_config, centralized_config, catalog_manager)
            except Exception as e:
                logger.error(f"Error processing root schema {schema.name if schema else 'unknown'}: {e}")
                continue
    
    def _get_server_config(self, data_contract: Any, environment: str) -> Any:
        """Find server configuration matching the current environment.
        
        Args:
            data_contract: Data contract specification
            environment: Current environment (dev/test/prod)
            
        Returns:
            Server configuration object or None
        """
        for server in data_contract.servers:  # type: ignore
            if server.server == environment:
                return server
        return None
    
    def _process_schema(self, schema: Any, server_config: Any, centralized_config: CentralizedPipelineConfig, catalog_manager: CatalogSchemaManager) -> None:
        """Process a single schema to create raw table and optional backfill.
        
        Args:
            schema: Schema object from data contract
            server_config: Server configuration from data contract
            centralized_config: Centralized pipeline configuration
            catalog_manager: Catalog and schema manager
        """
        model_name = schema.name
        
        if not model_name:
            logger.warning("Schema with no name found, skipping...")
            return
        
        logger.info(f"Processing model: {model_name}")
        
        # Convert ODCS schema to TableConfig
        config_dict = schema_to_table_config(schema)
        validated_data_config = get_validate_data_configuration_contract(config_dict)
        
        # Create raw layer table
        self._create_raw_table(model_name, schema, server_config, centralized_config)
        
        # Create backfill if configured
        self._create_backfill_if_needed(model_name, validated_data_config, centralized_config)
    
    def _create_raw_table(self, model_name: str, schema: Any, server_config: Any, centralized_config: CentralizedPipelineConfig) -> None:
        """Create raw layer DLT table using connector framework.
        
        Dynamically creates connector based on data contract configuration.
        Factory is completely agnostic to connector types - all logic delegated to connectors.
        
        Args:
            model_name: Name of the model/table
            schema: Schema object from data contract
            server_config: Server configuration from data contract
            centralized_config: Centralized pipeline configuration
        """
        try:
            # Build connector configuration using builder pattern
            base_connector_config = ConnectorConfig.from_server_config(server_config)
            
            # Create appropriate builder based on connector type
            # Pass model_name for per-schema volume mapping in AutoLoader
            builder = ConnectorConfigBuilderFactory.create_builder(
                base_connector_config.connector_type,
                base_connector_config,
                centralized_config,
                model_name  # AutoLoader uses this for per-schema volume assignment
            )
            
            # Build config with optional schema overrides for REST API and volume mapping
            builder = (
                builder
                .merge_schema_overrides(schema)
                .merge_shared_context()
                .resolve_secrets()
            )
            
            # Build Spark schema if the builder supports it (e.g., REST API connectors)
            if hasattr(builder, 'build_spark_schema'):
                builder = builder.build_spark_schema(model_name, schema)
            
            final_config = builder.build()
            
            # Create connector instance
            connector = ConnectorFactory.create(base_connector_config.connector_type, final_config)
            logger.info(f"Created {base_connector_config.connector_type} connector for {model_name}: {type(connector).__name__}")
            
            # Use connector-based API
            ldp_table(
                name=f"{centralized_config.raw_catalog}.{centralized_config.raw_schema}.{model_name}",
                connector=connector,
                comment=f"Raw layer table for {model_name} using {base_connector_config.connector_type} connector",
            )
            logger.info(f"Created raw table: {model_name} using {base_connector_config.connector_type} connector")
        except Exception as e:
            logger.error(f"Error creating raw table {model_name}: {e}")
            raise
    
    def _create_backfill_if_needed(
        self, 
        model_name: str, 
        validated_data_config: Any, 
        centralized_config: CentralizedPipelineConfig
    ) -> None:
        """Create backfill append flow if backfill is configured.
        
        Args:
            model_name: Name of the model/table
            validated_data_config: Validated data configuration
            centralized_config: Centralized pipeline configuration
        """
        backfill = validated_data_config.backfill if hasattr(validated_data_config, "backfill") else None
        
        logger.info(f"Backfill setting for {model_name}: {backfill}")
        
        if backfill is None:
            return
        
        logger.info(f"Backfilling table: {model_name} with historic data.")
        
        try:
            # Create closure properly to avoid variable capture issues
            self._create_backfill_flow(model_name, centralized_config)
        except Exception as e:
            logger.error(f"Error during backfill of table {model_name}: {e}")
            raise
    
    def _create_backfill_flow(self, model_name: str, centralized_config: CentralizedPipelineConfig) -> None:
        """Create DLT append flow for backfill.
        
        This method uses a factory pattern to create proper closures for DLT decorators,
        avoiding the Python closure variable capture bug.
        
        Args:
            model_name: Name of the model/table
            centralized_config: Centralized pipeline configuration
        """
        # Capture variables in local scope to avoid closure issues
        source_catalog = centralized_config.landing_catalog
        source_schema = centralized_config.landing_schema
        target_catalog = centralized_config.raw_catalog
        target_schema = centralized_config.raw_schema
        filetype = centralized_config.filetype
        object_name = model_name
        
        @dlt.append_flow(
            target=f"{target_catalog}.{target_schema}.{model_name}",
            once=True,
            name=f"{model_name}_backfill",
            comment=f"Backfill {model_name} raw registration events",
        )
        def _backfill():
            """Backfill function with properly captured variables."""
            return read_dataframe(
                source_catalog,
                source_schema,
                f"{object_name}_historic",
                filetype=filetype,
            )
        
        logger.info(f"Created backfill flow for {model_name}")


def create_raw_pipeline(source_system_name: str) -> None:
    """Convenience function to create raw pipeline.
    
    Args:
        source_system_name: Name of the source system (e.g., 'lakehouse', 'review')
    """
    spark = get_spark()
    factory = RawPipelineFactory(spark)
    factory.create_pipeline(source_system_name)
