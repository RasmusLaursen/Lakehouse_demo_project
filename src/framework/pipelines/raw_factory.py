"""Factory for creating raw layer DLT pipelines."""
from pyspark.sql import SparkSession
from typing import Optional, Any

try:
    import dlt  # type: ignore
except ImportError:
    dlt = None  # type: ignore

from src.framework.helper import (
    databricks_helper,
    lakeflow_declarative_pipeline,
    logging_helper,
    common,
    read,
    data_contract_helper
)
from src.framework.pipelines.config import PipelineConfig

logger = logging_helper.get_logger(__name__)


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
        
        # Load configuration
        config = PipelineConfig.from_spark(self.spark, source_system_name)
        
        # Load data contract
        data_contract = data_contract_helper.get_data_contract(
            catalog="source_system", 
            object_name=source_system_name
        )
        
        # Find and apply server configuration for current environment
        server_config = self._get_server_config(data_contract, config.environment)
        config.update_from_server_config(server_config)
        
        # Process each schema in the data contract
        for schema in data_contract.schema_:  # type: ignore
            try:
                self._process_schema(schema, config)
            except Exception as e:
                logger.error(f"Error processing schema {schema.name if schema else 'unknown'}: {e}")
                continue
        
        logger.info(f"Completed raw pipeline creation for {source_system_name}")
    
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
    
    def _process_schema(self, schema: Any, config: PipelineConfig) -> None:
        """Process a single schema to create raw table and optional backfill.
        
        Args:
            schema: Schema object from data contract
            config: Pipeline configuration
        """
        model_name = schema.name
        
        if not model_name:
            logger.warning("Schema with no name found, skipping...")
            return
        
        logger.info(f"Processing model: {model_name}")
        
        # Convert ODCS schema to TableConfig
        config_dict = data_contract_helper.schema_to_table_config(schema)
        validated_data_config = common.get_validate_data_configuration_contract(config_dict)
        
        # Create raw layer table
        self._create_raw_table(model_name, config)
        
        # Create backfill if configured
        self._create_backfill_if_needed(model_name, validated_data_config, config)
    
    def _create_raw_table(self, model_name: str, config: PipelineConfig) -> None:
        """Create raw layer DLT table using connector framework.
        
        Dynamically creates connector based on data contract configuration.
        Factory is completely agnostic to connector types - all logic delegated to connectors.
        
        Args:
            model_name: Name of the model/table
            config: Pipeline configuration
        """
        try:
            # Get connector with auto-merged config (catalog/schema/volume for volume sources)
            connector = config.get_connector(model_name)
            
            logger.info(f"Created {config.connector_type} connector for {model_name}: {type(connector).__name__}")
            
            # Use connector-based API
            lakeflow_declarative_pipeline.ldp_table(
                name=f"{config.raw_catalog}.{config.raw_schema}.{model_name}",
                connector=connector,
                comment=f"Raw layer table for {model_name} using {config.connector_type} connector",
            )
            logger.info(f"Created raw table: {model_name} using {config.connector_type} connector")
        except Exception as e:
            logger.error(f"Error creating raw table {model_name}: {e}")
            raise
    
    def _create_backfill_if_needed(
        self, 
        model_name: str, 
        validated_data_config: Any, 
        config: PipelineConfig
    ) -> None:
        """Create backfill append flow if backfill is configured.
        
        Args:
            model_name: Name of the model/table
            validated_data_config: Validated data configuration
            config: Pipeline configuration
        """
        backfill = validated_data_config.backfill if hasattr(validated_data_config, "backfill") else None
        
        logger.info(f"Backfill setting for {model_name}: {backfill}")
        
        if backfill is None:
            return
        
        logger.info(f"Backfilling table: {model_name} with historic data.")
        
        try:
            # Create closure properly to avoid variable capture issues
            self._create_backfill_flow(model_name, config)
        except Exception as e:
            logger.error(f"Error during backfill of table {model_name}: {e}")
            raise
    
    def _create_backfill_flow(self, model_name: str, config: PipelineConfig) -> None:
        """Create DLT append flow for backfill.
        
        This method uses a factory pattern to create proper closures for DLT decorators,
        avoiding the Python closure variable capture bug.
        
        Args:
            model_name: Name of the model/table
            config: Pipeline configuration
        """
        # Capture variables in local scope to avoid closure issues
        source_catalog = config.landing_catalog
        source_schema = config.landing_schema
        target_catalog = config.raw_catalog
        target_schema = config.raw_schema
        filetype = config.filetype
        object_name = model_name
        
        @dlt.append_flow(
            target=f"{target_catalog}.{target_schema}.{model_name}",
            once=True,
            name=f"{model_name}_backfill",
            comment=f"Backfill {model_name} raw registration events",
        )
        def _backfill():
            """Backfill function with properly captured variables."""
            return read.read_volume(
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
    spark = databricks_helper.get_spark()
    factory = RawPipelineFactory(spark)
    factory.create_pipeline(source_system_name)
