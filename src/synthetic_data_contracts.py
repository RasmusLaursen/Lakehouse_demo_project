from src.framework.helper import (
    DynamicFakeDataGenerator,
    get_spark,
    get_dbutils,
    write_volume,
    parse_arguments,
    list_yml_files,
)

from src.framework.helper.core import get_logger

def save_list_to_volume(landing_catalog, landing_schema, entity_name, entity_records, spark, logger):
    logger.info(
        f"Creating volume for {landing_catalog}.{landing_schema}.{entity_name}..."
    )
    df_entity = spark.createDataFrame(entity_records)
    write_volume(
        target_catalog=landing_catalog,
        target_schema=landing_schema,
        target_name=f"{entity_name}",
        source_dataframe=df_entity,
        mode="overwrite",
        file_format="parquet",
    )
    logger.info(f"{entity_name.capitalize()} data written successfully.")


def main(spark=None, logger=None):
    """Main function for synthetic data generation.
    
    Can be called with explicit spark and logger arguments, or will initialize them
    if not provided (for entry point compatibility).
    """
    # Initialize dependencies if not provided
    if logger is None:
        logger = get_logger(__name__)
    if spark is None:
        spark = get_spark()
    
    logger.info("Starting synthetic data generation based on data contracts...")
    landing_catalog = parse_arguments("landing_catalog")
    data_contracts = list_yml_files(catalog="source_system")

    for contract in data_contracts:
        if contract.name in ("lakehouse.yml", "review.yml"):
            contract_name = contract.name
            logger.info(f"Processing data contract: {contract_name}")
            contract_name = contract_name.split("/")[-1].replace(".yml", "")
            landing_schema = parse_arguments(f"{contract_name}_landing_schema")        
            generator = DynamicFakeDataGenerator(str(contract))

            all_data = generator.generate_all_models()
            
            # Save to parquet files
            for model_name, records in all_data.items():
                logger.info("Saving data for model: " + model_name)
                save_list_to_volume(
                    landing_catalog=landing_catalog,
                    landing_schema=landing_schema,
                    entity_name=model_name,
                    entity_records=records,
                    spark=spark,
                    logger=logger
                )
                logger.info(f"{model_name.capitalize()} data written successfully.")
            logger.info("Completed processing for data contract: " + contract_name)


if __name__ == "__main__":
    # Configure logging and spark
    logger = get_logger(__name__)
    spark = get_spark()
    dbutils = get_dbutils(spark)
    
    logger.info("Starting synthetic data generation...")
    main(spark, logger)