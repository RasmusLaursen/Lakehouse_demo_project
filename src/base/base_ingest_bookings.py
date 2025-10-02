from src.helper import databricks_helper
from src.helper import lakeflow_declarative_pipeline
from src.helper import logging_helper
from src.helper import commen
from src.helper import read
import os

# Initialize logger
logger = logging_helper.get_logger(__name__)

# Initialize Spark session
spark = databricks_helper.get_spark()

source_system_name = "bookings"

# Configuration
landing_catalog = spark.conf.get("landing_catalog")
lakehouse_landing_schema = spark.conf.get(f"{source_system_name}_landing_schema")

source_catalog = spark.conf.get("raw_catalog")
source_schema = spark.conf.get(f"{source_system_name}_raw_schema")

target_catalog = spark.conf.get("base_catalog")
target_schema = spark.conf.get(f"{source_system_name}_base_schema")

table_list = commen.list_volumes_in_schema(
    logger, spark, landing_catalog, lakehouse_landing_schema
)

base_path = os.path.join(os.curdir, f"config/{source_system_name}.yml")

lakehouse_config = commen.try_load_ingest_config(base_path)

if not table_list:
    logger.info("No volumes found in the source schema.")
else:
    for table in table_list:
        table_name = table.object_name
        config = lakehouse_config[table_name]

        # to do validation of config using pydantic --> https://medium.com/datamindedbe/leveraging-pydantic-for-validation-daf2d51e0627
        # ensure the right fields are present in the config file based on the ldp operation

        keys = config["keys"]
        sequence_column = config["sequence_column"]
        stored_as_scd_type = config["stored_as_scd_type"]
        logger.info(
            f"Processing table: {table_name} with parameters: keys {keys}, sequence_column {sequence_column}, stored_as_scd_type {stored_as_scd_type}"
        )

        lakeflow_declarative_pipeline.ldp_change_data_capture(
            source_catalog=source_catalog,
            source_schema=source_schema,
            source_object=table_name,
            target_catalog=target_catalog,
            target_schema=target_schema,
            target_object=table_name,
            private_name=f"local_{table_name}",
            keys=keys,
            sequence_column=sequence_column,
            stored_as_scd_type=stored_as_scd_type,
            name=f"silver_load_{target_schema}_{table_name}",
        )
        logger.info(f"Successfully processed table: {table_name}")
