from src.helper.dynamic_fake_data_generator import DynamicFakeDataGenerator
from src.helper import databricks_helper
from src.helper import logging_helper
from src.helper import common
from src.helper import write

# Configure logging
logger = logging_helper.get_logger(__name__)


spark = databricks_helper.get_spark()
dbutils = databricks_helper.get_dbutils(spark)

def save_list_to_volume(landing_catalog, landing_schema, entity_name, entity_records):
    logger.info(
        f"Creating volume for {landing_catalog}.{landing_schema}.{entity_name}..."
    )
    df_entity = spark.createDataFrame(entity_records)
    write.write_volume(
        target_catalog=landing_catalog,
        target_schema=landing_schema,
        target_name=f"{entity_name}_contract",
        source_dataframe=df_entity,
        mode="overwrite",
        file_format="parquet",
    )
    logger.info(f"{entity_name.capitalize()} data written successfully.")


def main():
    logger.info("Starting synthetic data generation based on data contracts...")
    landing_catalog = common.parse_arguments("landing_catalog")
    data_contracts = common.list_yml_files(catalog="source_system")
    logger.info(f"Data contracts to process: {data_contracts}")

    for contract in data_contracts:
        contract_name = contract.name
        logger.info(f"Processing data contract: {contract_name}")
        contract_name = contract_name.split("/")[-1].replace(".yml", "")
        landing_schema = common.parse_arguments(f"{contract_name}_landing_schema")        
        generator = DynamicFakeDataGenerator(str(contract))

        all_data = generator.generate_all_models()
        
        # Save to parquet files
        for model_name, records in all_data.items():
            logger.info("Saving data for model: " + model_name)
            save_list_to_volume(
                landing_catalog=landing_catalog,
                landing_schema=landing_schema,
                entity_name=model_name,
                entity_records=records
            )
            logger.info(f"{model_name.capitalize()} data written successfully.")
        logger.info("Completed processing for data contract: " + contract_name)
if __name__ == "__main__":
    logger.info("Starting synthetic data generation...")
    main()