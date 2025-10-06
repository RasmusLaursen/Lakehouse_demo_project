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
    name=f"{target_catalog}.{target_schema}.temp_dim_customer",
    comment="Curated layer dimension table for customer",
    table_properties={"pipelines.changeDataCaptureMode": "TRACK_CHANGES"},
    temporary=True
)
def dim_customer(
    base_catalog=base_catalog, lakehouse_base_schema=lakehouse_base_schema
):
    customer_df = (
        spark.readStream
        .option("readChangeFeed", "true")
        .table(f"{base_catalog}.{lakehouse_base_schema}.customer_scd")
    )
    customer_df = customer_df.withColumnsRenamed({"customer_id": "customer_key", "__START_AT": "validfrom"})
    customer_df = customer_df.select("customer_key", "validfrom", "name", "email", "phone_number", "birth_date", "loyalty_tier","preferred_payment_method")
    # customer_df = customer_df.withColumn("customer_id", monotonically_increasing_id())
    return customer_df

lakeflow_declarative_pipeline.ldp_change_data_capture(
    source=f"{target_catalog}.{target_schema}.temp_dim_customer",
    target_catalog=target_catalog,
    target_schema=target_schema,
    target_object=f"dim_customer_cdc_scd2",
    keys=["customer_key"],
    sequence_column="validfrom",
    stored_as_scd_type=2,
    name=f"silver_load_{target_schema}_dim_customer_cdc_scd2",
)
logger.info(f"Successfully processed table: dim_customer")