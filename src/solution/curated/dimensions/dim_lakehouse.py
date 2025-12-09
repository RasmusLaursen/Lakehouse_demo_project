"""Curated dimension table for lakehouse.

This module uses the factory pattern for generic dimension logic,
while keeping custom transformation logic (joins) in this file.
"""
from pyspark.sql import DataFrame
from src.framework.factory.dimension_factory import CuratedDimensionFactory
from src.framework.helper import databricks_helper

def custom_lakehouse_transform(df: DataFrame) -> DataFrame:
    """Apply lakehouse-specific transformations.
    
    Joins with meta_lakehouses table to enrich with lakehouse name.
    
    Args:
        df: Source lakehouse dataframe from base layer
        
    Returns:
        DataFrame with meta_lakehouses joined
    """
    spark = databricks_helper.get_spark()
    from src.framework.factory.config import PipelineConfig
    config = PipelineConfig.from_spark(spark)
    
    # Read lookup table
    meta_lakehouse_df = spark.read.table(
        config.get_base_table_path('meta_lakehouses')
    )
    
    # Join to get lakehouse name
    df = df.join(
        meta_lakehouse_df, 
        on="lakehouse_name_id", 
        how="left"
    ).select(
        df["*"],
        meta_lakehouse_df["lakehouse_name"].alias("name")
    )
    
    return df


# Create the dimension table using factory with custom transform
spark = databricks_helper.get_spark()
factory = CuratedDimensionFactory(spark)

factory.create_dimension(
    dimension_name='dim_lakehouse',
    source_table='lakehouse',
    business_key_column='lakehouse_id',
    filter_active=False,  # No SCD for this dimension
    additional_transforms=custom_lakehouse_transform
)
