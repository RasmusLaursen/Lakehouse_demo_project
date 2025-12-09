"""Factory for creating base layer DLT pipelines."""
from pyspark.sql import SparkSession
from typing import List, Dict, Any, Optional

try:
    import dlt  # type: ignore
except ImportError:
    dlt = None  # type: ignore

from src.framework.helper import (
    data_contract_helper,
    databricks_helper,
    lakeflow_declarative_pipeline,
    logging_helper,
    common,
    dqx_helper
)
from src.framework.pipelines.config import PipelineConfig

logger = logging_helper.get_logger(__name__)


class BasePipelineFactory:
    """Factory for creating base layer CDC pipelines."""
    
    def __init__(self, spark: SparkSession):
        """Initialize the factory.
        
        Args:
            spark: Active SparkSession
        """
        self.spark = spark
        self.ws = dqx_helper.get_ws_client()
        self.dq_engine = dqx_helper.get_dq_engine(self.ws)
    
    def create_pipeline(self, source_system_name: str) -> None:
        """Create base layer CDC pipeline for a source system.
        
        This method creates DLT tables with Change Data Capture (CDC) processing
        based on data contracts. It supports optional data quality validation.
        
        Args:
            source_system_name: Name of the source system (e.g., 'lakehouse', 'review')
        """
        logger.info(f"Creating base pipeline for source system: {source_system_name}")
        
        # Load configuration
        config = PipelineConfig.from_spark(self.spark, source_system_name)
        
        # Load data contract
        data_contract = data_contract_helper.get_data_contract(
            catalog="source_system",
            object_name=source_system_name
        )
        
        # Load data quality configuration
        validated_data_quality_raw = dqx_helper.get_data_quality_configuration(
            catalog="source_system",
            object=source_system_name,
            spark=self.spark
        )
        
        # Ensure it's a list
        validated_data_quality: List[Dict[str, Any]] = []
        if validated_data_quality_raw:
            if isinstance(validated_data_quality_raw, list):
                validated_data_quality = validated_data_quality_raw
            else:
                logger.warning(f"Data quality config is not a list: {type(validated_data_quality_raw)}")
        
        # Process each schema in the data contract
        for schema in data_contract.schema_:  # type: ignore
            try:
                self._process_schema(schema, config, validated_data_quality)
            except Exception as e:
                logger.error(f"Error processing schema {schema.name if schema else 'unknown'}: {e}")
                continue
        
        logger.info(f"Completed base pipeline creation for {source_system_name}")
    
    def _process_schema(
        self,
        schema: Any,
        config: PipelineConfig,
        validated_data_quality: List[Dict[str, Any]]
    ) -> None:
        """Process a single schema to create base table with optional DQ.
        
        Args:
            schema: Schema object from data contract
            config: Pipeline configuration
            validated_data_quality: List of data quality check configurations
        """
        model_name = schema.name
        
        if not model_name:
            logger.warning("Schema with no name found, skipping...")
            return
        
        logger.info(f"Processing model: {model_name}")
        
        # Convert ODCS schema to TableConfig
        config_dict = data_contract_helper.schema_to_table_config(schema)
        validated_data_config = common.get_validate_data_configuration_contract(config_dict)
        
        if not validated_data_config:
            logger.error(f"Validation failed for model: {model_name}. Skipping...")
            return
        
        # Extract CDC configuration
        keys = validated_data_config.keys
        sequence_column = validated_data_config.sequence_column
        stored_as_scd_type = validated_data_config.stored_as_scd_type
        
        logger.info(
            f"Processing table: {model_name} with parameters: "
            f"keys {keys}, sequence_column {sequence_column}, stored_as_scd_type {stored_as_scd_type}"
        )
        
        # Determine source table (with or without DQ)
        source = f"{config.raw_catalog}.{config.raw_schema}.{model_name}"
        
        if validated_data_quality:
            # Create DQ table and use it as source
            source = self._create_dq_table(
                model_name,
                config,
                validated_data_quality
            )
        
        # Create CDC table
        self._create_cdc_table(
            model_name,
            source,
            config,
            keys,
            sequence_column,
            stored_as_scd_type
        )
        
        logger.info(f"Successfully processed table: {model_name}")
    
    def _create_dq_table(
        self,
        model_name: str,
        config: PipelineConfig,
        validated_data_quality: List[Dict[str, Any]]
    ) -> str:
        """Create data quality validation table.
        
        This method creates a proper closure to avoid variable capture bugs.
        
        Args:
            model_name: Name of the model/table
            config: Pipeline configuration
            validated_data_quality: List of data quality check configurations
            
        Returns:
            Fully qualified name of the DQ table
        """
        dq_table_name = f"{config.base_catalog}.{config.base_schema}.{model_name}_dq"
        
        # Capture variables in local scope to avoid closure issues
        raw_catalog = config.raw_catalog
        raw_schema = config.raw_schema
        source_system_name = config.source_system_name
        object_name = model_name
        spark = self.spark
        dq_engine = self.dq_engine
        
        # Filter DQ checks for this specific table
        data_quality_checks = [
            check for check in validated_data_quality
            if check.get("table") == model_name
        ]
        
        if not data_quality_checks:
            logger.warning(f"No data quality checks found for {model_name}")
        
        @dlt.table(
            name=dq_table_name,
            comment=f"Base layer table for {model_name} from {source_system_name} with DQ applied",
            private=True
        )
        def _dq_table():
            """DQ table function with properly captured variables."""
            logger.info(f"Applying data quality for {object_name}")
            source_table = f"{raw_catalog}.{raw_schema}.{object_name}"
            df = spark.readStream.table(source_table)
            dq_results = dq_engine.apply_checks_by_metadata(df, data_quality_checks)
            return dq_results
        
        logger.info(f"Created DQ table: {dq_table_name}")
        return dq_table_name
    
    def _create_cdc_table(
        self,
        model_name: str,
        source: str,
        config: PipelineConfig,
        keys: List[str],
        sequence_column: str,
        stored_as_scd_type: int
    ) -> None:
        """Create CDC table using lakeflow declarative pipeline.
        
        Args:
            model_name: Name of the model/table
            source: Source table (may be DQ table or raw table)
            config: Pipeline configuration
            keys: Primary keys for CDC
            sequence_column: Column used for sequencing changes
            stored_as_scd_type: SCD type (1 or 2)
        """
        lakeflow_declarative_pipeline.ldp_change_data_capture(
            source=source,
            target_catalog=config.base_catalog,
            target_schema=config.base_schema,
            target_object=model_name,
            keys=keys,
            sequence_column=sequence_column,
            stored_as_scd_type=stored_as_scd_type,
            name=f"base_load_{config.base_schema}_{model_name}",
        )
        logger.info(f"Created CDC table: {model_name}")


def create_base_pipeline(source_system_name: str) -> None:
    """Convenience function to create base pipeline.
    
    Args:
        source_system_name: Name of the source system (e.g., 'lakehouse', 'review')
    """
    spark = databricks_helper.get_spark()
    factory = BasePipelineFactory(spark)
    factory.create_pipeline(source_system_name)
