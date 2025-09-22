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

source_system_name = "review"

# Configuration
source_catalog = spark.conf.get("raw_catalog")
source_schema = spark.conf.get(f"{source_system_name}_raw_schema")

target_catalog = spark.conf.get("base_catalog")
target_schema = spark.conf.get(f"{source_system_name}_base_schema")

# source_catalog = "raw"
# source_schema = "dev_rahl_lakehouse_raw_dev"

# target_catalog = "base"
# target_schema = "dev_rahl_lakehouse_base_dev"

try:
    # Fetch distinct volume names
    table_list = spark.sql(
        f"""
        SELECT DISTINCT table_name
        FROM {source_catalog}.information_schema.tables
        WHERE table_catalog = '{source_catalog}'
        AND table_schema = '{source_schema}'
        AND table_type != 'MANAGED'
        """
    ).collect()
except Exception as e:
    logger.error(f"Error fetching table list: {e}")
    table_list = []

base_path = os.path.join(os.curdir, f"config/{source_system_name}.yml")

lakehouse_config = commen.try_load_ingest_config(base_path)

if not table_list:
    logger.info("No volumes found in the source schema.")
else:
    for table in table_list:
        # try:
        config = lakehouse_config[table.table_name]

        # to do validation of config using pydantic --> https://medium.com/datamindedbe/leveraging-pydantic-for-validation-daf2d51e0627
        # ensure the right fields are present in the config file based on the ldp operation

        keys = config["keys"]
        sequence_column = config["sequence_column"]
        stored_as_scd_type = config["stored_as_scd_type"]
        logger.info(
            f"Processing table: {table.table_name} with parameters: keys {keys}, sequence_column {sequence_column}, stored_as_scd_type {stored_as_scd_type}"
        )

        lakeflow_declarative_pipeline.ldp_change_data_capture(
            source_catalog=source_catalog,
            source_schema=source_schema,
            source_object=table.table_name,
            target_catalog=target_catalog,
            target_schema=target_schema,
            target_object=table.table_name,
            private_name=f"local_{table.table_name}",
            keys=keys,
            sequence_column=sequence_column,
            stored_as_scd_type=stored_as_scd_type,
            name=f"silver_load_{target_schema}_{table.table_name}",
        )
        logger.info(f"Successfully processed table: {table.table_name}")
        # except Exception as e:
        #     logger.error(f"Error processing table {table.table_name}: {e}")
