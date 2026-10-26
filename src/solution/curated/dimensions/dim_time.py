"""Curated dimension table for Eloverblik metering points.

This dimension processes metering point (electricity meter) data from Eloverblik,
excluding metadata columns and standardizing column names to snake_case.
"""
from pyspark import pipelines as sdp  # type: ignore[attr-defined]
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, floor, format_string, hash
from src.framework.helper import get_spark
from src.framework.helper.dataframe.curated_audit import add_curated_audit_columns
from src.framework.config import CentralizedPipelineConfig, CatalogSchemaManager

def custom_time_transform() -> DataFrame:
    """Apply time-specific transformations.
    
    Joins with meta_region table to enrich with region name.
    
    Args:
        df: Source time dataframe from base layer
        
    Returns:
        DataFrame with meta_region joined
    """
    spark = get_spark()

    # Generate time dimension with one row per minute
    time_df = spark.range(0, 24 * 60).select(
        (col("id") % (24 * 60)).alias("minutes_of_day"),
        floor(col("id") / 60).cast("int").alias("hour_of_day"),
        (col("id") % 60).cast("int").alias("minute_of_hour")
    ).withColumn(
        "time_key", 
        (col("hour_of_day") * 100 + col("minute_of_hour")).cast("int")
    ).withColumn(
        "time_display",
        format_string("%02d:%02d", col("hour_of_day"), col("minute_of_hour"))
    ).withColumn(
        "hour_display",
        format_string("%02d", col("hour_of_day"))
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

# Static generated dimension: batch source cannot feed auto_cdc, so publish as a materialized view
spark = get_spark()
catalog_manager = CatalogSchemaManager.from_pipeline_config(CentralizedPipelineConfig.from_spark(spark))


@sdp.table(
    name=catalog_manager.get_dimension_table_path('dim_time'),
    comment="Curated layer dimension table for time of day"
)
def dim_time() -> DataFrame:
    df = custom_time_transform()
    df = df.withColumn("time_id", hash(col("time_key")))
    return add_curated_audit_columns(df)