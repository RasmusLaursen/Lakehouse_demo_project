"""Curated dimension table for calendar.

This module uses the factory pattern to create DLT dimension tables.
Calendar dimension reads from enriched layer and has special key handling.
"""
from pyspark import pipelines as sdp # type: ignore[attr-defined]
from pyspark.sql.functions import col, monotonically_increasing_id
from src.framework.helper import get_spark, get_pipeline_configurations, get_logger
from src.framework.config import CentralizedPipelineConfig, CatalogSchemaManager

logger = get_logger(__name__)
spark = get_spark()
centralized_config = CentralizedPipelineConfig.from_spark(spark)
catalog_manager = CatalogSchemaManager.from_pipeline_config(centralized_config)

@sdp.table(
    name=catalog_manager.get_dimension_table_path('dim_calendar'),
    comment="Curated layer dimension table for calendar"
)
def dim_calendar():
    """Calendar dimension from enriched layer."""
    logger.info(f"Reading table: {centralized_config.enriched_catalog}.{centralized_config.enriched_schema}.calendar")
    df = spark.read.table(f"{centralized_config.enriched_catalog}.{centralized_config.enriched_schema}.calendar")
    
    logger.info("Adding 'calendar_id' and 'calendar_key' columns")
    df = df.withColumn("calendar_id", monotonically_increasing_id())
    df = df.withColumn("calendar_key", col("date"))
    
    logger.info("Returning enriched calendar DataFrame")
    return df

