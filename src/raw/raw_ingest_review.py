from src.helper import databricks_helper
from src.helper import lakeflow_declarative_pipeline
from src.helper import logging_helper
from src.helper import common

# Initialize logger
logger = logging_helper.get_logger(__name__)

# Initialize Spark session
spark = databricks_helper.get_spark()

source_system_name = "review"

environment = spark.conf.get("environment")

source_catalog = spark.conf.get("landing_catalog")
source_schema = spark.conf.get(f"{source_system_name}_landing_schema")

target_catalog = spark.conf.get("raw_catalog")
target_schema = spark.conf.get(f"{source_system_name}_raw_schema")

validated_data_config = common.get_data_configuration(
    catalog="source_system", object=source_system_name
)

filetype = (
    validated_data_config.file_type
    if hasattr(validated_data_config, "file_type")
    else "json"
)
loadtype = (
    validated_data_config.load_type
    if hasattr(validated_data_config, "load_type")
    else "volume_autoloader"
)

for object_name, object_config in validated_data_config.objects.items():
    try:
        logger.info(f"Processing object: {object_name}")
        lakeflow_declarative_pipeline.ldp_table(
            name=f"{target_catalog}.{target_schema}.{object_name}",
            source_catalog=source_catalog,
            source_schema=source_schema,
            objectname=object_name,
            loadtype=loadtype,
            filetype=filetype,
            comment=f"Raw layer table for {object_name} volume",
        )
    except Exception as e:
        logger.error(f"Error processing object {object_name}: {e}")
        raise
