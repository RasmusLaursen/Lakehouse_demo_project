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

catalogs = databricks_helper.get_pipeline_configurations(spark, "catalogs")
schemas = databricks_helper.get_pipeline_configurations(spark, "schemas")

logger.debug("Catalogs configuration: " + str(catalogs))
logger.debug("Schemas configuration: " + str(schemas))

base_catalog = catalogs.get("base_catalog")
lakehouse_base_schema = schemas.get("lakehouse_base_schema")

target_catalog = catalogs.get("curated_catalog")
target_schema = schemas.get("dimensions_schema")


@dlt.table(
    name=f"{target_catalog}.{target_schema}.dim_seller",
    comment="Curated layer dimension table for seller",
)
def dim_calendar(
    base_catalog=base_catalog, lakehouse_base_schema=lakehouse_base_schema
):
    seller_df = spark.read.table(f"{base_catalog}.{lakehouse_base_schema}.seller")
    meta_region_df = spark.read.table(
        f"{base_catalog}.{lakehouse_base_schema}.meta_region"
    )

    seller_df = seller_df.join(
        meta_region_df,
        seller_df["region_name_id"] == meta_region_df["region_name_id"],
        "left",
    ).select(
        seller_df["*"],
        meta_region_df["region_name"].alias("region_name"),
    )

    seller_df = seller_df.withColumnRenamed("seller_id", "seller_key")
    seller_df = seller_df.withColumn("seller_id", monotonically_increasing_id())
    return seller_df
