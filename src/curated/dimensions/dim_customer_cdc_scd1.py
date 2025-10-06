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

lakeflow_declarative_pipeline.ldp_change_data_capture(
    source=f"{target_catalog}.{target_schema}.temp_dim_customer",
    target_catalog=target_catalog,
    target_schema=target_schema,
    target_object=f"dim_customer_cdc_scd1",
    keys=["customer_key"],
    sequence_column="validfrom",
    stored_as_scd_type=1,
    name=f"silver_load_{target_schema}_dim_customer_cdc_scd1",
)
logger.info(f"Successfully processed table: dim_customer")
