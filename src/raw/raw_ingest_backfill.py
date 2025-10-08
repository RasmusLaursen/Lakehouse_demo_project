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

environment = spark.conf.get("environment")

source_catalog = spark.conf.get("landing_catalog")
source_schema = spark.conf.get("lakehouse_landing_schema")

target_catalog = spark.conf.get("raw_catalog")
target_schema = spark.conf.get("lakehouse_raw_schema")

volume_list = common.list_volumes_in_schema(
    spark, source_catalog, source_schema
)

for volume in volume_list:
    if volume.object_name == "lakehouse_rentals":
        logger.info(f"Processesing volume: {volume.object_name}")

        lakeflow_declarative_pipeline.ldp_table(
            name=f"{target_catalog}.{target_schema}.{volume.object_name}_test",
            source_catalog=source_catalog,
            source_schema=source_schema,
            objectname=f"{volume.object_name}",
            loadtype="volume_autoloader",
            filetype="parquet",
            comment=f"Raw layer table for {volume.object_name} volume",
        )

        logger.info(f"Backfilling table: {volume.object_name} with historic data.")
        try:
            @dlt.append_flow(
                target=f"{target_catalog}.{target_schema}.{volume.object_name}_test",
                once=True,
                name=f"{volume.object_name}_backfill",
                comment=f"Backfill {volume.object_name} Raw registration events"
            )
            def backfill(source_catalog=source_catalog, source_schema=source_schema, validated_config=volume.object_name):
                return read.read_volume(source_catalog, source_schema, f"{validated_config}_historic", filetype="parquet")
            
        except Exception as e:
            logger.error(f"Error during backfill of table {volume.object_name}_backfill: {e}")
            continue
    logger.info(f"Successfully processed table: {volume.object_name}")
