"""Curated dimension table for seller.

This module uses the factory pattern for generic dimension logic,
while keeping custom transformation logic (joins) in this file.
"""
from pyspark.sql import DataFrame
from src.framework.factory.dimension_factory import CuratedDimensionFactory
from src.framework.helper import get_spark, get_pipeline_configurations
from src.framework.config import CentralizedPipelineConfig, CatalogSchemaManager

def custom_seller_transform(df: DataFrame) -> DataFrame:
    """Apply seller-specific transformations.
    
    Joins with meta_region table to enrich with region name.
    
    Args:
        df: Source seller dataframe from base layer
        
    Returns:
        DataFrame with meta_region joined
    """
    spark = get_spark()
    centralized_config = CentralizedPipelineConfig.from_spark(spark)
    catalog_manager = CatalogSchemaManager.from_pipeline_config(centralized_config)
    
    # Read lookup table
    meta_region_df = spark.read.table(
        catalog_manager.get_base_table_path('meta_region')
    )
    
    # Join to get region name
    df = df.join(
        meta_region_df,
        df["region_name_id"] == meta_region_df["region_name_id"],
        "left"
    ).select(
        df["*"],
        meta_region_df["region_name"].alias("region_name")
    )
    
    return df


# Create the dimension table using factory with custom transform
spark = get_spark()
factory = CuratedDimensionFactory(spark)

factory.create_dimension(
    dimension_name='dim_seller',
    source_table='seller',
    business_key_column='seller_id',
    filter_active=False,  # No SCD for this dimension
    additional_transforms=custom_seller_transform
)
