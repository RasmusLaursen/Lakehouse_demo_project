from src.framework.helper import get_spark, get_pipeline_configurations
from src.framework.helper import ldp_table, ldp_change_data_capture
from src.framework.helper import get_logger
from src.framework.helper import get_data_configuration

# Initialize logger
logger = get_logger(__name__)

# Initialize Spark session
spark = get_spark()

dimension_name = "customer_cdc_scd1"

catalogs = get_pipeline_configurations(spark, "catalogs")
schemas = get_pipeline_configurations(spark, "schemas")

logger.debug("Catalogs configuration: " + str(catalogs))
logger.debug("Schemas configuration: " + str(schemas))

base_catalog = catalogs.get("base_catalog")
lakehouse_base_schema = schemas.get("lakehouse_base_schema")

target_catalog = catalogs.get("curated_catalog")
target_schema = schemas.get("dimensions_schema")

# Load ingestion configuration
validated_data_config = get_data_configuration(
    catalog="curated", object="dimensions"
)

dimension_config = validated_data_config.objects["customer_cdc_scd1"]

ldp_change_data_capture(
    source=f"{target_catalog}.{target_schema}.temp_dim_customer",
    target_catalog=target_catalog,
    target_schema=target_schema,
    target_object=f"dim_{dimension_name}",
    keys=dimension_config.keys,
    sequence_column=dimension_config.sequence_column,
    stored_as_scd_type=dimension_config.stored_as_scd_type,
    name=f"dimension_load_{target_schema}_{dimension_name}",
)
logger.info(f"Successfully processed table: dim_{dimension_name}")
