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
    name=f"{target_catalog}.{target_schema}.dim_customer_mv_scd1",
    comment="Curated layer dimension table for customer",
)
def dim_customer(
    base_catalog=base_catalog, lakehouse_base_schema=lakehouse_base_schema
):
    customer_df = spark.read.table(f"{base_catalog}.{lakehouse_base_schema}.customer")

    customer_df = customer_df.filter(col("__END_AT").isNull())
    customer_df = customer_df.withColumnsRenamed({"customer_id": "customer_key"})

    payment_method = spark.read.table(
        f"{base_catalog}.{lakehouse_base_schema}.payment_method"
    )
    loyalty_tier = spark.read.table(
        f"{base_catalog}.{lakehouse_base_schema}.loyalty_tier"
    )

    logger.info("Selecting columns for temp_dim_customer")
    # Join with payment_method and loyalty_tier tables to enrich customer_df
    customer_df = customer_df.join(
        payment_method,
        customer_df["preferred_payment_method_id"]
        == payment_method["payment_method_id"],
        "left",
    ).join(
        loyalty_tier,
        customer_df["loyalty_tier_id"] == loyalty_tier["loyalty_tier_id"],
        "left",
    )

    customer_df = customer_df.select(
        "customer_key",
        "name",
        "email",
        "phone_number",
        "birth_date",
        "loyalty_tier.loyalty_tier",
        "payment_method.payment_method",
    )

    customer_df = customer_df.withColumn("customer_id", monotonically_increasing_id())
    return customer_df
