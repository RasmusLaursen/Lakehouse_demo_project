"""Factory for creating curated dimension DLT tables."""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, monotonically_increasing_id, when, lit
from typing import Optional, Callable

try:
    import dlt  # type: ignore
except ImportError:
    dlt = None  # type: ignore

from src.framework.helper import get_logger
from src.framework.config import (
    CentralizedPipelineConfig,
    CatalogSchemaManager
)

logger = get_logger(__name__)


class CuratedDimensionFactory:
    """Factory for creating curated dimension tables with standardized patterns."""
    
    def __init__(self, spark: SparkSession, source_system: str = "lakehouse"):
        """Initialize the factory.
        
        Args:
            spark: Active SparkSession
            source_system: Source system name (default: lakehouse)
        """
        self.spark = spark
        self.centralized_config = CentralizedPipelineConfig.from_spark(spark, source_system)
        self.centralized_config.validate()
        self.catalog_manager = CatalogSchemaManager.from_pipeline_config(self.centralized_config)
    
    def create_dimension(
        self,
        dimension_name: str,
        source_table: str,
        business_key_column: str,
        filter_active: bool = True,
        scd_type: int = 2,
        additional_transforms: Optional[Callable[[DataFrame], DataFrame]] = None
    ) -> None:
        """Create a dimension table with standardized pattern.
        
        This method creates a DLT dimension table that:
        1. Reads from base layer
        2. Optionally filters active records (__END_AT IS NULL)
        3. Applies custom transformations (joins, enrichments, etc.)
        4. Renames business key to {entity}_key
        5. Adds surrogate key as {entity}_id
        6. For SCD Type 2: Renames __START_AT to dim_start_time and __END_AT to dim_end_time
        
        Args:
            dimension_name: Name of dimension (e.g., 'dim_customer')
            source_table: Name of source table in base layer
            business_key_column: Column name of business key (e.g., 'customer_id')
            filter_active: Whether to filter for active records only (default: True)
            scd_type: SCD type - 1 (overwrite) or 2 (with history). Default: 2
            additional_transforms: Optional function to apply custom transformations (e.g., joins)
        """
        logger.info(f"Creating dimension: {dimension_name} (SCD Type {scd_type})")
        
        # Capture variables for closure
        catalog_manager = self.catalog_manager
        spark = self.spark
        
        # Create the DLT table
        @dlt.table(  # type: ignore
            name=catalog_manager.get_dimension_table_path(dimension_name),
            comment=f"Curated layer dimension table for {dimension_name.replace('dim_', '')} (SCD{scd_type})"
        )
        def _dimension_table():
            """Dimension table with standardized processing."""
            logger.info(f"Reading source table: {source_table}")
            
            # Read base table
            df = spark.read.table(catalog_manager.get_base_table_path(source_table))
            
            # Filter active records if SCD Type 2
            if filter_active and scd_type == 2:
                df = df.filter(col("__END_AT").isNull())
                logger.debug(f"Filtered active records for {dimension_name}")
            
            # Apply custom transformations (joins, enrichments, etc.)
            if additional_transforms:
                df = additional_transforms(df)
            
            # Add surrogate key
            df = self._add_surrogate_key(df, business_key_column)
            
            # For SCD Type 2: Add start and end time columns
            if scd_type == 2:
                # Rename __START_AT to dim_start_time
                if "__START_AT" in df.columns:
                    df = df.withColumnRenamed("__START_AT", "dim_start_time")
                    logger.debug(f"Renamed __START_AT to dim_start_time for {dimension_name}")
                
                # Rename __END_AT to dim_end_time and convert NULL to future date
                if "__END_AT" in df.columns:
                    df = df.withColumnRenamed("__END_AT", "dim_end_time")
                    # Set NULL end times to a far future date (9999-12-31)
                    df = df.withColumn(
                        "dim_end_time",
                        when(col("dim_end_time").isNull(), lit("9999-12-31")).otherwise(col("dim_end_time"))
                    )
                    logger.debug(f"Renamed __END_AT to dim_end_time and set NULLs to 9999-12-31 for {dimension_name}")
            
            return df
        
        logger.info(f"Successfully created dimension: {dimension_name} (SCD Type {scd_type})")
    
    def _add_surrogate_key(
        self, 
        df: DataFrame, 
        business_key_column: str
    ) -> DataFrame:
        """Add surrogate key to dimension table.
        
        Renames business key to {entity}_key and creates new {entity}_id
        as surrogate key using monotonically_increasing_id().
        
        Args:
            df: Source dataframe
            business_key_column: Name of business key column (e.g., 'customer_id')
            
        Returns:
            DataFrame with surrogate key added
        """
        # Extract entity name (e.g., 'customer' from 'customer_id')
        entity_name = business_key_column.replace("_id", "")
        key_column = f"{entity_name}_key"
        id_column = f"{entity_name}_id"
        
        # Rename business key to {entity}_key
        df = df.withColumnRenamed(business_key_column, key_column)
        
        # Add surrogate key as {entity}_id
        df = df.withColumn(id_column, monotonically_increasing_id())
        
        logger.debug(f"Added surrogate key: {id_column}, renamed {business_key_column} -> {key_column}")
        
        return df
