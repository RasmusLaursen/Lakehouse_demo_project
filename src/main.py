from src.landing.lakehouse_synthetic_data import lakehouse_synthetic_data
from src.landing import review_synthetic_data
from src.helper import databricks_helper
from src.helper import logging_helper
from src.helper import common
from src.helper import write
import sys

# Configure logging
logger = logging_helper.get_logger(__name__)

spark = databricks_helper.get_spark()
dbutils = databricks_helper.get_dbutils(spark)


def lakehouse_generate_data(
    landing_catalog: str,
    landing_schema: str,
    records: dict,
):
    entities = {
        "lakehouse_rentals": records["lakehouse_rentals"],
        "customer": records["customers"],
        "seller": records["sellers"],
        "lakehouse": records["lakehouses"],
    }

    for entity_name, entity_records in entities.items():
        save_list_to_volume(
            landing_catalog, landing_schema, entity_name, entity_records
        )
    return entities["lakehouse_rentals"]


def review_generate_data(
    landing_catalog: str,
    landing_schema: str,
    records: list,
):
    entity_name = "reviews"
    entity_records = records
    save_list_to_volume(landing_catalog, landing_schema, entity_name, entity_records)


def save_list_to_volume(landing_catalog, landing_schema, entity_name, entity_records):
    logger.info(
        f"Creating volume for {landing_catalog}.{landing_schema}.{entity_name}..."
    )
    df_entity = spark.createDataFrame(entity_records)
    write.write_volume(
        target_catalog=landing_catalog,
        target_schema=landing_schema,
        target_name=entity_name,
        source_dataframe=df_entity,
        mode="overwrite",
        file_format="parquet",
    )
    logger.info(f"{entity_name.capitalize()} data written successfully.")


def main():
    landing_catalog = common.parse_arguments("landing_catalog")
    lakehouse_landing_schema = common.parse_arguments("lakehouse_landing_schema")
    review_landing_schema = common.parse_arguments("review_landing_schema")

    logger.info(f"Landing Catalog: {landing_catalog}")
    logger.info(f"Landing Schema: {lakehouse_landing_schema}")

    logger.info("Generating synthetic data for lakehouse...")
    lakehouse_data_generator = lakehouse_synthetic_data()

    lakehouse_records = lakehouse_data_generator.generate_lakehouse_synthetic_data()

    logger.info("Starting data generation...")
    lakehouse_rentals = lakehouse_generate_data(
        landing_catalog=landing_catalog,
        landing_schema=lakehouse_landing_schema,
        records=lakehouse_records,
    )
    logger.info("Data generation completed successfully for lakehouse.")

    logger.info(f"Landing Catalog: {landing_catalog}")
    logger.info(f"Landing Schema: {review_landing_schema}")

    logger.info("Generating synthetic data...")
    review_data_generator = review_synthetic_data.LakehouseReviews(
        lakehouse_rentals=lakehouse_rentals
    )

    review_records = review_data_generator.generate_reviews_synthetic_data()

    logger.info("Starting data generation...")
    lakehouse_rentals = review_generate_data(
        landing_catalog=landing_catalog,
        landing_schema=review_landing_schema,
        records=review_records,
    )
    logger.info("Data generation completed successfully for lakehouse.")


if __name__ == "__main__":
    main()
