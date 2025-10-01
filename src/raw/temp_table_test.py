from src.helper import databricks_helper
from src.helper import lakeflow_declarative_pipeline
from src.helper import logging_helper
from src.helper import commen
import dlt
from src.helper import read

# Initialize logger
logger = logging_helper.get_logger(__name__)

# Initialize Spark session
spark = databricks_helper.get_spark()

environment = spark.conf.get("environment")

source_catalog = spark.conf.get("landing_catalog")
source_schema = spark.conf.get("bookings_landing_schema")

target_catalog = spark.conf.get("raw_catalog")
target_schema = spark.conf.get("bookings_raw_schema")

volume_list = commen.list_volumes_in_schema(
    logger, spark, source_catalog, source_schema
)

for volume in volume_list:
    logger.info(f"Processesing volume: {volume.object_name}")

    @dlt.table(
        name=f"{target_catalog}.{target_schema}.{volume.object_name}_temp",
        comment=f"Raw layer table for {volume.object_name} volume",
        temporary=True
    )
    def load_volume_table():
        return read.read_cloudfiles_autoloader(
                source_catalog=source_catalog,
                source_schema=source_schema,
                objectname=volume.object_name,
                filetype="json",
                add_audit_column=True,
            )