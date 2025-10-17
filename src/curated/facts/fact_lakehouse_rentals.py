from src.helper import databricks_helper
from src.helper import logging_helper
from src.helper import dw
from src.helper import lakeflow_declarative_pipeline
import dlt
from pyspark.sql.functions import concat_ws, sha2, col

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
target_schema = schemas.get("facts_schema")
curated_dimension_schema = schemas.get("dimensions_schema")


@dlt.table(
    name=f"{target_catalog}.{target_schema}.fact_lakehouse_rentals",
    comment="Curated layer fact table for lakehouse rentals",
)
def fact_lakehouse_rentals(
    base_catalog=base_catalog,
    lakehouse_base_schema=lakehouse_base_schema,
    curated_catalog=target_catalog,
    curated_dimension_schema=curated_dimension_schema,
):
    lakehouse_rentals_df = spark.read.table(
        f"{base_catalog}.{lakehouse_base_schema}.lakehouse_rentals"
    )

    lakehouse_rentals_df = lakehouse_rentals_df.withColumnsRenamed(
        {
            "seller_id": "seller_key",
            "customer_id": "customer_key",
            "lakehouse_id": "lakehouse_key",
            "order_date": "calendar_order_key",
            "check_in_date": "calendar_checkin_key",
            "check_out_date": "calendar_checkout_key",
        }
    )

    lakehouse_rentals_df = dw.dimension_keys_lookup(
        curated_catalog, curated_dimension_schema, fact_df=lakehouse_rentals_df
    )

    return lakehouse_rentals_df
