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
    # table_properties={"pipelines.changeDataCaptureMode": "TRACK_CHANGES"},
    temporary=True,
)
def dim_customer(
    base_catalog=base_catalog, lakehouse_base_schema=lakehouse_base_schema
):
    logger.info(f"Reading source table: {base_catalog}.{lakehouse_base_schema}.customer_scd with CDC enabled")
    customer_df = spark.readStream.option("readChangeFeed", "true").table(
        f"{base_catalog}.{lakehouse_base_schema}.customer"
    )

    payment_method = spark.read.table(f"{base_catalog}.{lakehouse_base_schema}.payment_method")
    loyalty_tier = spark.read.table(f"{base_catalog}.{lakehouse_base_schema}.loyalty_tier")

    customer_df = customer_df.withColumnsRenamed(
        {"customer_id": "customer_key", "__START_AT": "validfrom"}
    )
    logger.info("Selecting columns for temp_dim_customer")
    # Join with payment_method and loyalty_tier tables to enrich customer_df
    customer_df = customer_df.join(
        payment_method,
        customer_df["preferred_payment_method_id"] == payment_method["payment_method_id"],
        "left"
    ).join(
        loyalty_tier,
        customer_df["loyalty_tier_id"] == loyalty_tier["loyalty_tier_id"],
        "left"
    )

    customer_df = customer_df.select(
        "customer_key",
        "validfrom",
        "name",
        "email",
        "phone_number",
        "birth_date",
        "loyalty_tier.loyalty_tier",
        "payment_method.payment_method",
    )
    logger.info(f"Writing temp_dim_customer to {target_catalog}.{target_schema}.temp_dim_customer")
    return customer_df


logger.info(f"Starting CDC SCD2 load for: {target_catalog}.{target_schema}.temp_dim_customer -> {target_catalog}.{target_schema}.dim_customer_cdc_scd2")
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
logger.info(f"Successfully processed table: dim_customer_cdc_scd2")


@dlt.table(
    name=f"{target_catalog}.{target_schema}.dim_customer_mv_scd2_cdc",
    comment="Curated layer dimension table for customer with SCD2 and CDC"
)
def dim_customer_mv_scd2_cdc(
    target_catalog=target_catalog, target_schema=target_schema
):
    logger.info(f"Reading CDC SCD2 table: {target_catalog}.{target_schema}.dim_customer_cdc_scd2 with CDC enabled")
    customer_df = spark.read.table(f"{target_catalog}.{target_schema}.dim_customer_cdc_scd2")
    
    customer_df = customer_df.drop("validfrom")

    customer_df = customer_df.withColumnsRenamed(
        {"__START_AT": "validfrom","__END_AT": "validto"}
    )
    customer_df = customer_df.withColumn("customer_id", monotonically_increasing_id())
    logger.info(f"Writing dim_customer_mv_scd2_cdc to {target_catalog}.{target_schema}.dim_customer_mv_scd2_cdc")
    return customer_df