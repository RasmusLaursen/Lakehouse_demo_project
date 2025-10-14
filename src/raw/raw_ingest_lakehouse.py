from src.helper import databricks_helper
from src.helper import lakeflow_declarative_pipeline
from src.helper import logging_helper
from src.helper import common
from src.helper import read
import dlt

# Initialize logger
logger = logging_helper.get_logger(__name__)

# Initialize Spark session
spark = databricks_helper.get_spark()

source_system_name = "lakehouse"

environment = spark.conf.get("environment")

catalogs = databricks_helper.get_pipeline_configurations(spark, "catalogs")
schemas = databricks_helper.get_pipeline_configurations(spark, "schemas")

logger.debug("Catalogs configuration: " + str(catalogs))
logger.debug("Schemas configuration: " + str(schemas))

source_catalog = catalogs.get("landing_catalog")
source_schema = schemas.get(f"{source_system_name}_landing_schema")

target_catalog = catalogs.get("raw_catalog")
target_schema = schemas.get(f"{source_system_name}_raw_schema")

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
        continue

    backfill = object_config.backfill if hasattr(object_config, "backfill") else None

    logger.info(f"Backfill setting for {object_name}: {backfill}")

    if backfill is not None:
        logger.info(f"Backfilling table: {object_name} with historic data.")
        try:

            @dlt.append_flow(
                target=f"{target_catalog}.{target_schema}.{object_name}",
                once=True,
                name=f"{object_name}_backfill",
                comment=f"Backfill {object_name} Raw registration events",
            )
            def _backfill(
                source_catalog=source_catalog,
                source_schema=source_schema,
                object_name=object_name,
            ):
                return read.read_volume(
                    source_catalog,
                    source_schema,
                    f"{object_name}_historic",
                    filetype=filetype,
                )

        except Exception as e:
            logger.error(f"Error during backfill of table {object_name}_backfill: {e}")
            continue
