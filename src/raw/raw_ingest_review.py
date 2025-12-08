from src.helper import (
    databricks_helper,
    lakeflow_declarative_pipeline,
    logging_helper,
    common,
    read,
    data_contract_helper
)

import dlt


# Initialize logger
logger = logging_helper.get_logger(__name__)

# Initialize Spark session
spark = databricks_helper.get_spark()

source_system_name = "review"

environment = spark.conf.get("environment")

catalogs = databricks_helper.get_pipeline_configurations(spark, "catalogs")
schemas = databricks_helper.get_pipeline_configurations(spark, "schemas")

logger.debug("Catalogs configuration: " + str(catalogs))
logger.debug("Schemas configuration: " + str(schemas))

source_catalog = catalogs.get("landing_catalog")
source_schema = schemas.get(f"{source_system_name}_landing_schema")

target_catalog = catalogs.get("raw_catalog")
target_schema = schemas.get(f"{source_system_name}_raw_schema")

data_contract_specification = data_contract_helper.get_data_contract(
    catalog="source_system", object_name=source_system_name
)

# Find the server configuration for the current environment
server_config = None
for server in data_contract_specification.servers: # type: ignore
    if server.server == environment:
        server_config = server
        break

filetype = (
    server_config.format
    if server_config and hasattr(server_config, "format")
    else "toast"
)
loadtype = "volume_autoloader"
if server_config and hasattr(server_config, "customProperties") and server_config.customProperties:
    for prop in server_config.customProperties:
        if hasattr(prop, 'key') and prop.key == "loadtype":
            loadtype = prop.value
            break

for schema in data_contract_specification.schema_: # type: ignore
    model_name = schema.name

    # Convert ODCS schema to TableConfig
    config_dict = data_contract_helper.schema_to_table_config(schema)
    validated_data_config = common.get_validate_data_configuration_contract(config_dict)    


    # if not validated_data_config:
    #     logger.error(f"Validation failed for model: {model_name}. Skipping...")
    #     continue

    try:
        logger.info(f"Processing model: {model_name}")
        lakeflow_declarative_pipeline.ldp_table(
            name=f"{target_catalog}.{target_schema}.{model_name}",
            source_catalog=source_catalog,
            source_schema=source_schema,
            objectname=f"{model_name}_contract",
            loadtype=loadtype, # type: ignore
            filetype=filetype,
            comment=f"Raw layer table for {model_name} volume",
        )
    except Exception as e:
        logger.error(f"Error processing model {model_name}: {e}")
        continue

    backfill = validated_data_config.backfill if hasattr(validated_data_config, "backfill") else None

    logger.info(f"Backfill setting for {model_name}: {backfill}")

    if backfill is not None:
        logger.info(f"Backfilling table: {model_name} with historic data.")
        try:

            @dlt.append_flow(
                target=f"{target_catalog}.{target_schema}.{model_name}",
                once=True,
                name=f"{model_name}_backfill",
                comment=f"Backfill {model_name} Raw registration events",
            )
            def _backfill(
                source_catalog=source_catalog,
                source_schema=source_schema,
                object_name=model_name,
            ):
                return read.read_volume(
                    source_catalog,
                    source_schema,
                    f"{object_name}_historic",
                    filetype=filetype,
                )

        except Exception as e:
            logger.error(f"Error during backfill of table {model_name}_backfill: {e}")
            continue
