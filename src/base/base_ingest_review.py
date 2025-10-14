from src.helper import databricks_helper
from src.helper import lakeflow_declarative_pipeline
from src.helper import logging_helper
from src.helper import common

# Initialize logger
logger = logging_helper.get_logger(__name__)

# Initialize Spark session
spark = databricks_helper.get_spark()

# Define source system name
source_system_name = "review"

# Load ingestion configuration
validated_data_config = common.get_data_configuration(
    catalog="source_system", object=source_system_name
)

catalogs = databricks_helper.get_pipeline_configurations(spark, "catalogs")
schemas = databricks_helper.get_pipeline_configurations(spark, "schemas")

logger.debug("Catalogs configuration: " + str(catalogs))
logger.debug("Schemas configuration: " + str(schemas))

raw_catalog = catalogs.get("raw_catalog")
target_raw_schema = schemas.get(f"{source_system_name}_raw_schema")
target_catalog =  catalogs.get("base_catalog")
target_schema = schemas.get(f"{source_system_name}_base_schema")

# Loop over objects in validated_lakehouse_config.tables and log their names
for object_name, object_config in validated_data_config.objects.items():
    logger.info(f"Validated table config found for: {object_name}")

    keys = object_config.keys
    sequence_column = object_config.sequence_column
    stored_as_scd_type = object_config.stored_as_scd_type

    logger.info(
        f"Processing table: {object_name} with parameters: keys {keys}, sequence_column {sequence_column}, stored_as_scd_type {stored_as_scd_type}"
    )

    lakeflow_declarative_pipeline.ldp_change_data_capture(
        source=f"{raw_catalog}.{target_raw_schema}.{object_name}",
        target_catalog=target_catalog,
        target_schema=target_schema,
        target_object=object_name,
        keys=keys,
        sequence_column=sequence_column,
        stored_as_scd_type=stored_as_scd_type,
        name=f"base_load_{target_schema}_{object_name}",
    )
    logger.info(f"Successfully processed table: {object_name}")
