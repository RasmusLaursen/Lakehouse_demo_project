"""Factory for creating curated fact DLT tables."""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col
from typing import Dict, List, Optional, Callable

try:
    import dlt  # type: ignore
except ImportError:
    dlt = None  # type: ignore

from src.framework.helper import databricks_helper, logging_helper, dw
from src.framework.pipelines.config import PipelineConfig

logger = logging_helper.get_logger(__name__)


class CuratedFactFactory:
    """Factory for creating curated fact tables with dimension key lookups."""
    
    def __init__(self, spark: SparkSession, source_system: str = "lakehouse"):
        """Initialize the factory.
        
        Args:
            spark: Active SparkSession
            source_system: Source system name (default: lakehouse)
        """
        self.spark = spark
        self.config = PipelineConfig.from_spark(spark, source_system)
        self.config.validate()
    
    def create_fact(
        self,
        fact_name: str,
        source_table: str,
        dimension_mappings: Dict[str, str],
        source_schema: Optional[str] = None,
        additional_transforms: Optional[Callable[[DataFrame], DataFrame]] = None
    ) -> None:
        """Create a fact table with dimension key lookups.
        
        This method creates a DLT fact table that:
        1. Reads from base layer
        2. Renames columns to match dimension keys
        3. Performs dimension key lookups
        4. Applies any custom transformations
        
        Args:
            fact_name: Name of fact table (e.g., 'fact_lakehouse_rentals')
            source_table: Name of source table in base layer
            dimension_mappings: Dict of {source_column: dimension_key_column}
                               Example: {'seller_id': 'seller_key', 'customer_id': 'customer_key'}
            source_schema: Optional schema name override (default: lakehouse_base_schema)
            additional_transforms: Optional function to apply custom transformations
            
        Example:
            >>> factory.create_fact(
            ...     'fact_lakehouse_rentals',
            ...     'lakehouse_rentals',
            ...     {
            ...         'seller_id': 'seller_key',
            ...         'customer_id': 'customer_key',
            ...         'lakehouse_id': 'lakehouse_key'
            ...     }
            ... )
        """
        logger.info(f"Creating fact table: {fact_name}")
        
        # Capture variables for closure
        config = self.config
        spark = self.spark
        
        # Determine source schema
        if source_schema is None:
            schema = config.base_schema
        else:
            # Get from config
            schema = databricks_helper.get_pipeline_configurations(spark, "schemas").get(source_schema)
        
        # Create the DLT table
        @dlt.table(  # type: ignore
            name=config.get_fact_table_path(fact_name),
            comment=f"Curated layer fact table for {fact_name.replace('fact_', '')}"
        )
        def _fact_table():
            """Fact table with dimension key lookups."""
            logger.info(f"Reading source table: {source_table}")
            
            # Read base fact table
            df = spark.read.table(f"{config.base_catalog}.{schema}.{source_table}")
            
            # Rename columns to dimension keys
            df = df.withColumnsRenamed(dimension_mappings)
            logger.debug(f"Renamed columns: {dimension_mappings}")
            
            # Apply custom transformations before dimension lookup
            if additional_transforms:
                df = additional_transforms(df)
            
            # Perform dimension key lookups
            df = dw.dimension_keys_lookup(
                curated_catalog=config.curated_catalog,
                curated_dimension_schema=config.dimensions_schema,
                fact_df=df
            )
            logger.debug(f"Applied dimension key lookups for {fact_name}")
            
            return df
        
        logger.info(f"Successfully created fact table: {fact_name}")
