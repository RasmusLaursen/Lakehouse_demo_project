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

base_catalog = spark.conf.get("base_catalog")
lakehouse_base_schema = spark.conf.get("lakehouse_base_schema")

target_catalog = spark.conf.get("curated_catalog")
target_schema = spark.conf.get("dimensions_schema")


@dlt.table(
    name=f"{target_catalog}.{target_schema}.dim_customer",
    comment="Curated layer dimension table for customer",
)
def dim_customer(
    base_catalog=base_catalog, lakehouse_base_schema=lakehouse_base_schema
):
    customer_df = spark.read.table(f"{base_catalog}.{lakehouse_base_schema}.customer")
    customer_df = customer_df.withColumnRenamed("customer_id", "customer_key")
    customer_df = customer_df.withColumn("customer_id", monotonically_increasing_id())
    return customer_df


# lakeflow_declarative_pipeline.ldp_table(
#     name=f"{target_catalog}.{target_schema}.calendar",
#     source_dataframe=date_df,
#     commet=f"Enriched layer table for calendar",
# )
