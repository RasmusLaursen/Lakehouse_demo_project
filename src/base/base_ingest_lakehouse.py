import dlt
from src.helper import data_contract_helper, databricks_helper, lakeflow_declarative_pipeline, logging_helper, common
from src.helper.config import TableConfig
from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient

dq_engine = DQEngine(WorkspaceClient())

# Initialize logger
logger = logging_helper.get_logger(__name__)

# Initialize Spark session
spark = databricks_helper.get_spark()

# Define source system name
source_system_name = "lakehouse"

data_contract_specification = data_contract_helper.get_data_contract(
    catalog="source_system", object_name=source_system_name
)

validated_data_quality = common.get_data_quality_configuration(
    catalog="source_system", 
    object=source_system_name,
    dq_engine=dq_engine
)

catalogs = databricks_helper.get_pipeline_configurations(spark, "catalogs")
schemas = databricks_helper.get_pipeline_configurations(spark, "schemas")

logger.debug("Catalogs configuration: " + str(catalogs))
logger.debug("Schemas configuration: " + str(schemas))

raw_catalog = catalogs.get("raw_catalog")
target_raw_schema = schemas.get(f"{source_system_name}_raw_schema")
target_catalog = catalogs.get("base_catalog")
target_schema = schemas.get(f"{source_system_name}_base_schema")

# Loop over schemas in data contract and process each one
for schema in data_contract_specification.schema_: # type: ignore
    
    model_name = schema.name
    
    if not model_name:
        logger.warning(f"Schema with no name found, skipping...")
        continue
    
    # Convert ODCS schema to TableConfig
    config_dict = data_contract_helper.schema_to_table_config(schema)
    
    # Validate the configuration
    try:
        validated_data_config = TableConfig(**config_dict)
    except Exception as e:
        logger.error(f"Validation failed for model: {model_name}. Error: {e}")
        continue
    
    source = f"{raw_catalog}.{target_raw_schema}.{model_name}"
    logger.info(f"Validated table config found for: {model_name}")

    keys = validated_data_config.keys
    sequence_column = validated_data_config.sequence_column
    stored_as_scd_type = validated_data_config.stored_as_scd_type

    logger.info(
        f"Processing table: {model_name} with parameters: keys {keys}, sequence_column {sequence_column}, stored_as_scd_type {stored_as_scd_type}"
    )

    if validated_data_config.data_quality and validated_data_quality:
        source = f"{target_catalog}.{target_schema}.{model_name}_dq"
        @dlt.table(
            name=source,
            comment=f"Base layer table for {model_name} from {source_system_name} with DQ applied",
            private=True
        )
        def temp_table_with_dq(
            object_name=model_name,
            raw_catalog=raw_catalog,
            target_raw_schema=target_raw_schema,
            validated_data_quality=validated_data_quality
        ):
            data_quality_checks = []
            
            # validated_data_quality is a list of dicts
            if validated_data_quality:
                for check in validated_data_quality:
                    if check.get("table") == object_name:
                        data_quality_checks.append(check)
            
            if not data_quality_checks:
                logger.warning(f"No data quality checks found for {object_name}")
            
            logger.info(f"Applying data quality for {object_name}")
            source_table = f"{raw_catalog}.{target_raw_schema}.{object_name}"
            df = spark.readStream.table(source_table)

            dq_results = dq_engine.apply_checks_by_metadata(df, data_quality_checks)

            return dq_results

    lakeflow_declarative_pipeline.ldp_change_data_capture(
        source=source,
        target_catalog=target_catalog,
        target_schema=target_schema,
        target_object=model_name,
        keys=keys,
        sequence_column=sequence_column,
        stored_as_scd_type=stored_as_scd_type,
        name=f"base_load_{target_schema}_{model_name}",
    )
    logger.info(f"Successfully processed table: {model_name}")
