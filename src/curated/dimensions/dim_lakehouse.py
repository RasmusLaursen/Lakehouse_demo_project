from src.helper import databricks_helper
from src.helper import logging_helper
from pyspark.sql.functions import monotonically_increasing_id
import dlt

# Initialize logger
logger = logging_helper.get_logger(__name__)

# Initialize Spark session
spark = databricks_helper.get_spark()

base_catalog = spark.conf.get("base_catalog")
lakehouse_base_schema = spark.conf.get("lakehouse_base_schema")

target_catalog = spark.conf.get("curated_catalog")
target_schema = spark.conf.get("dimensions_schema")


@dlt.table(
    name=f"{target_catalog}.{target_schema}.dim_lakehouse",
    comment="Curated layer dimension table for lakehouse",
)
def dim_customer(
    base_catalog=base_catalog, lakehouse_base_schema=lakehouse_base_schema
):
    lakehouse_df = spark.read.table(f"{base_catalog}.{lakehouse_base_schema}.lakehouse")
    meta_lakehouse_df = spark.read.table(f"{base_catalog}.{lakehouse_base_schema}.meta_lakehouses")

    lakehouse_df = lakehouse_df.join(
        meta_lakehouse_df, on="lakehouse_name_id", how="left"
    ).select(
        lakehouse_df["*"],
        meta_lakehouse_df["lakehouse_name"].alias("name"),
    )


    lakehouse_df = lakehouse_df.withColumnRenamed("lakehouse_id", "lakehouse_key")
    lakehouse_df = lakehouse_df.withColumn(
        "lakehouse_id", monotonically_increasing_id()
    )
    return lakehouse_df
