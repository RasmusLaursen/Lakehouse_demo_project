from src.helper import databricks_helper
from src.helper import lakeflow_declarative_pipeline
from src.helper import logging_helper
from src.helper import read
from pyspark.sql.functions import col, monotonically_increasing_id
import dlt

# Initialize logger
logger = logging_helper.get_logger(__name__)

# Initialize Spark session
spark = databricks_helper.get_spark()

enriched_catalog = spark.conf.get("enriched_catalog")
enriched_schema = spark.conf.get("enriched_schema")

target_catalog = spark.conf.get("curated_catalog")
target_schema = spark.conf.get("dimensions_schema")


@dlt.table(
    name=f"{target_catalog}.{target_schema}.dim_calendar",
    comment="Curated layer dimension table for calendar",
)
def dim_calendar(enriched_catalog=enriched_catalog, enriched_schema=enriched_schema):
    enriched_calender = spark.read.table(
        f"{enriched_catalog}.{enriched_schema}.calendar"
    )
    enriched_calender = enriched_calender.withColumn(
        "calendar_id", monotonically_increasing_id()
    ).withColumn("calendar_key", col("date"))
    return enriched_calender


# lakeflow_declarative_pipeline.ldp_table(
#     name=f"{target_catalog}.{target_schema}.calendar",
#     source_dataframe=date_df,
#     commet=f"Enriched layer table for calendar",
# )
