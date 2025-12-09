"""Curated dimension table for calendar.

This module uses the factory pattern to create DLT dimension tables.
Calendar dimension reads from enriched layer and has special key handling.
"""
from pyspark import pipelines as sdp # type: ignore[attr-defined]
from pyspark.sql.functions import col, monotonically_increasing_id
from src.framework.helper import databricks_helper, logging_helper
from src.framework.pipelines.config import PipelineConfig

logger = logging_helper.get_logger(__name__)
spark = databricks_helper.get_spark()
config = PipelineConfig.from_spark(spark)

@sdp.table(
    name=config.get_dimension_table_path('dim_calendar'),
    comment="Curated layer dimension table for calendar"
)
def dim_calendar():
    """Calendar dimension from enriched layer."""
    logger.info(f"Reading table: {config.enriched_catalog}.{config.enriched_schema}.calendar")
    df = spark.read.table(f"{config.enriched_catalog}.{config.enriched_schema}.calendar")
    
    logger.info("Adding 'calendar_id' and 'calendar_key' columns")
    df = df.withColumn("calendar_id", monotonically_increasing_id())
    df = df.withColumn("calendar_key", col("date"))
    
    logger.info("Returning enriched calendar DataFrame")
    return df

