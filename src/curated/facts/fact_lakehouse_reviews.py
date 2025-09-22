from src.helper import databricks_helper
from src.helper import lakeflow_declarative_pipeline
from src.helper import logging_helper
from src.helper import read
from src.helper import dw
import dlt

# Initialize logger
logger = logging_helper.get_logger(__name__)

# Initialize Spark session
spark = databricks_helper.get_spark()

base_catalog = spark.conf.get("base_catalog")
review_base_schema = spark.conf.get("review_base_schema")

target_catalog = spark.conf.get("curated_catalog")
target_schema = spark.conf.get("facts_schema")
curated_dimension_schema = spark.conf.get("dimensions_schema")


@dlt.table(
    name=f"{target_catalog}.{target_schema}.fact_lakehouse_reviews",
    comment="Curated layer fact table for lakehouse reviews",
)
def fact_lakehouse_rentals(
    base_catalog=base_catalog,
    review_base_schema=review_base_schema,
    curated_catalog=target_catalog,
    curated_dimension_schema=curated_dimension_schema
):
    lakehouse_rentals_df = spark.read.table(
        f"{base_catalog}.{review_base_schema}.reviews"
    )

    lakehouse_rentals_df = lakehouse_rentals_df.withColumnsRenamed(
        {
            "review_date": "calendar_review_key"
        }
    )

    lakehouse_rentals_df = dw.dimension_keys_lookup(curated_catalog, curated_dimension_schema, fact_df = lakehouse_rentals_df)

    return lakehouse_rentals_df

