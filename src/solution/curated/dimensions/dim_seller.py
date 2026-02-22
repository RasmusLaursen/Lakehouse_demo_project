"""Curated dimension table for Eloverblik metering points.

This dimension processes metering point (electricity meter) data from Eloverblik,
excluding metadata columns and standardizing column names to snake_case.
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from src.framework.factory.dimension_factory import create_dimension
from src.framework.helper import get_spark
from src.framework.config import CentralizedPipelineConfig, CatalogSchemaManager
import re

def custom_seller_transform() -> DataFrame:
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

    df = spark.read.table(catalog_manager.base_catalog + "." + catalog_manager.base_schema + ".seller")

    
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
    
    df = df.withColumnRenamed("seller_id", "seller_key")  # Rename business key to *_key convention

    return df

# Create dimension using new factory pattern
create_dimension(
    dimension_name='dim_seller',
    keys=['seller_key'],  # Business key from data contract
    sequence_column='created_at',  # Use CDC sequence column
    transform_fn=custom_seller_transform,
    scd_type=1  # Track history of changes
)