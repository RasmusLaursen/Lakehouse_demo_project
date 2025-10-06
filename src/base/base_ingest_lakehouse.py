from src.helper import databricks_helper
from src.helper import lakeflow_declarative_pipeline
from src.helper import logging_helper
from src.helper import common
from src.helper import read
import os
from pydantic import ValidationError
from src.helper.config import TableConfig

# Initialize logger
logger = logging_helper.get_logger(__name__)

# Initialize Spark session
spark = databricks_helper.get_spark()

# Define source system name
source_system_name = "lakehouse"

# Configuration
pipeline_configs = databricks_helper.get_pipeline_configurations_from_spark(
    spark, source_system_name
)

# List tables in the landing schema
table_list = common.list_volumes_in_schema(
    logger,
    spark,
    pipeline_configs["landing_catalog"],
    pipeline_configs[f"{source_system_name}_landing_schema"],
)

# Define path to configuration file
base_path = os.path.join(os.curdir, f"config/{source_system_name}.yml")

# Load ingestion configuration
lakehouse_config = common.try_load_ingest_config(base_path)

# Process each table
if not table_list:
    logger.info("No volumes found in the source schema.")
else:
    for table in table_list:
        table_name = table.object_name
        config = lakehouse_config[table_name]

        try:
            validated_config = TableConfig(**config)
        except ValidationError as e:
            logger.error(
                f"Config validation error for table {table_name}: {e}. Skipping."
            )
            continue

        keys = validated_config.keys
        sequence_column = validated_config.sequence_column
        stored_as_scd_type = validated_config.stored_as_scd_type
        logger.info(
            f"Processing table: {table_name} with parameters: keys {keys}, sequence_column {sequence_column}, stored_as_scd_type {stored_as_scd_type}"
        )

        raw_catalog = pipeline_configs["raw_catalog"]
        target_raw_schema = pipeline_configs[f"{source_system_name}_raw_schema"]
        target_catalog = pipeline_configs["base_catalog"]
        target_schema = pipeline_configs[f"{source_system_name}_base_schema"]

        lakeflow_declarative_pipeline.ldp_change_data_capture(
            source=f"{raw_catalog}.{target_raw_schema}.{table_name}",
            target_catalog=target_catalog,
            target_schema=target_schema,
            target_object=table_name,
            keys=keys,
            sequence_column=sequence_column,
            stored_as_scd_type=stored_as_scd_type,
            name=f"silver_load_{target_schema}_{table_name}",
        )
        logger.info(f"Successfully processed table: {table_name}")
