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

def custom_time_transform() -> DataFrame:
    """Apply time-specific transformations.
    
    Joins with meta_region table to enrich with region name.
    
    Args:
        df: Source time dataframe from base layer
        
    Returns:
        DataFrame with meta_region joined
    """
    spark = get_spark()
    centralized_config = CentralizedPipelineConfig.from_spark(spark)
    catalog_manager = CatalogSchemaManager.from_pipeline_config(centralized_config)

    from pyspark.sql.functions import (
        col, to_timestamp, date_format, hour, minute, 
        dayofweek, dayofmonth, month, year, quarter
    )

    # Generate time dimension with one row per minute
    time_df = spark.range(0, 24 * 60).select(
        (col("id") % (24 * 60)).alias("minutes_of_day"),
        (col("id") / 60).alias("hour_of_day"),
        (col("id") % 60).alias("minute_of_hour")
    ).withColumn(
        "time_key", 
        (col("hour_of_day") * 100 + col("minute_of_hour")).cast("int")
    ).withColumn(
        "time_display",
        date_format(to_timestamp(col("time_key").cast("string"), "HHmm"), "HH:mm")
    ).withColumn(
        "hour_display",
        date_format(to_timestamp(col("hour_of_day").cast("string"), "HH"), "HH")
    ).withColumn(
        "is_business_hours",
        (col("hour_of_day") >= 9) & (col("hour_of_day") < 17)
    ).withColumn(
        "is_peak_hours",
        (col("hour_of_day") >= 17) & (col("hour_of_day") < 20)
    ).select(
        "time_key",
        "time_display",
        "hour_of_day",
        "hour_display",
        "minute_of_hour",
        "is_business_hours",
        "is_peak_hours"
    )

    return time_df

# Create dimension using new factory pattern
create_dimension(
    dimension_name='dim_time',
    keys=['time_key'],  # Business key from data contract
    sequence_column='time_key',  # Use CDC sequence column
    transform_fn=custom_time_transform,
    scd_type=1  # Track history of changes
)