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
target_schema = spark.conf.get("facts_schema")
curated_dimension_schema = spark.conf.get("dimensions_schema")


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

    lakehouse_rentals_df = dimension_keys_lookup(
        curated_catalog, curated_dimension_schema, fact_df=lakehouse_rentals_df
    )

    return lakehouse_rentals_df


def dimension_keys_lookup(curated_catalog, curated_dimension_schema, fact_df):
    dimenension_keys = [column for column in fact_df.columns if column.endswith("key")]

    for key in dimenension_keys:
        if key in fact_df.columns:
            dimension_name = key.replace("_key", "")

            if key.count("_") == 1:
                dimension_df = spark.read.table(
                    f"{curated_catalog}.{curated_dimension_schema}.dim_{dimension_name}"
                )
                dimension_key = key
                dimension_role_name = dimension_name
            elif key.count("_") == 2:
                split = key.split("_")
                dimension_key = f"{split[0]}_key"
                dimension_name = f"{split[0]}"
                dimension_role = f"{split[1]}"
                dimension_df = spark.read.table(
                    f"{curated_catalog}.{curated_dimension_schema}.dim_{dimension_name}"
                )
                dimension_role_name = dimension_name + "_" + dimension_role

            fact_df = fact_df.alias("lhr").join(
                dimension_df.select(
                    col(dimension_key).alias(key),
                    col(f"{dimension_name}_id").alias(f"{dimension_role_name}_id"),
                ).alias("dim"),
                on=col(f"lhr.{key}") == col(f"dim.{key}"),
                how="left",
            )

    cols = [c for c in fact_df.columns if not c.endswith("_key")]
    fact_df = fact_df.select(*cols)

    # Reorder columns so columns ending with _id come first
    cols = [c for c in fact_df.columns if c.endswith("_id")] + [
        c for c in fact_df.columns if not c.endswith("_id")
    ]
    fact_df = fact_df.select(*cols)
    return fact_df
