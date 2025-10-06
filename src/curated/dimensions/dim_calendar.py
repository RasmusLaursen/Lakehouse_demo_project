from src.helper import databricks_helper
from src.helper import lakeflow_declarative_pipeline
from src.helper import logging_helper
from src.helper import read
from pyspark.sql.functions import col, monotonically_increasing_id
import dlt
import os

# Initialize logger
logger = logging_helper.get_logger(__name__)

# Initialize Spark session
spark = databricks_helper.get_spark()

enriched_catalog = spark.conf.get("enriched_catalog")
enriched_schema = spark.conf.get("enriched_schema")

target_catalog = spark.conf.get("curated_catalog")
target_schema = spark.conf.get("dimensions_schema")

logger.info("### dim_calendar ###")
logger.debug(f"enriched_catalog: {enriched_catalog}, enriched_schema: {enriched_schema}")
logger.debug(f"target_catalog: {target_catalog}, target_schema: {target_schema}")


@dlt.table(
    name=f"{target_catalog}.{target_schema}.dim_calendar",
    comment="Curated layer dimension table for calendar",
)
def dim_calendar(enriched_catalog=enriched_catalog, enriched_schema=enriched_schema):
    logger.info(f"Reading table: {enriched_catalog}.{enriched_schema}.calendar")
    enriched_calender = spark.read.table(
        f"{enriched_catalog}.{enriched_schema}.calendar"
    )
    logger.info("Adding 'calendar_id' and 'calendar_key' columns")
    enriched_calender = enriched_calender.withColumn(
        "calendar_id", monotonically_increasing_id()
    ).withColumn("calendar_key", col("date"))
    logger.info("Returning enriched calendar DataFrame")
    return enriched_calender
