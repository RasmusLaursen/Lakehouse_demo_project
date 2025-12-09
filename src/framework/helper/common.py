

from pyspark.sql import DataFrame
from pyspark.sql.functions import struct, current_timestamp, lit
from typing import Dict, Any, Generator, List
from src.framework.helper import logging_helper
import sys
import yaml
from pathlib import Path
from src.framework.helper.config import LayerConfig, TableConfig
from pydantic import ValidationError
from src.framework.helper import data_contract_helper

# Initialize logger
logger = logging_helper.get_logger(__name__)

def get_path_for_data_configuration(catalog: str, object: str) -> Path:
    """
    Constructs the path to the data configuration file based on the provided catalog and object names.

    Args:
        catalog (str): The name of the catalog.
        object (str): The name of the object.

    Returns:
        LayerConfig: The validated LayerConfig instance.
    """

    if catalog == "curated":
        return Path(f"../../data_configuration/{catalog}/{object}.yml")
    else:
        return Path(f"../data_configuration/{catalog}/{object}.yml")
    
def get_validate_data_configuration_contract(config: Dict[str, Any]) -> TableConfig:
    """
    Validates the provided data configuration dictionary against the TableConfig schema.

    Args:
        config (Dict[str, Any]): The data configuration dictionary to validate.

    Returns:
        TableConfig: The validated TableConfig instance.
    """
    try:
        validated_data_config = TableConfig(**config)
    except ValidationError as e:
        logger.error(f"TableConfig validation error: {e}")
        raise   
    return validated_data_config


def get_data_configuration(catalog: str, object: str) -> LayerConfig:
    """
    Constructs the path to the data configuration file based on the provided catalog and object names.

    Args:
        catalog (str): The name of the catalog.
        object (str): The name of the object.

    Returns:
        Path: The constructed path to the data configuration file.
    """
    data_configuration_path = get_path_for_data_configuration(
        catalog=catalog, object=object
    )

    if not data_configuration_path.is_file():
        raise FileNotFoundError(
            f"Data configuration file not found: {data_configuration_path}"
        )

    data_configuration = try_load_ingest_config(data_configuration_path)

    # Validate data_configuration against LayerConfig
    try:
        validated_data_config = LayerConfig(**data_configuration)
        return validated_data_config
    except ValidationError as e:
        logger.error(f"LayerConfig validation error: {e}")
        raise


def add_audit_columns(df: DataFrame) -> DataFrame:
    """
    Adds audit columns to the given DataFrame.

    This function appends a metadata column to the DataFrame, which includes
    the source system and the current ingest timestamp.

    Parameters:
    df (DataFrame): The input DataFrame to which audit columns will be added.

    Returns:
    DataFrame: A new DataFrame with the added audit columns.
    """
    metadata = struct(
        lit("lakehouse_dummy_data").alias("SourceSystem"),
        current_timestamp().alias("ingest_timestamp"),
    )
    df = df.withColumn("_metadata_ldp", metadata)
    return df


def try_load_ingest_config(base_path: Path) -> Any:
    """
    Try to load the base configuration file from the specified path.

    This function attempts to read a YAML configuration file located at the
    given base path. If the file is found and successfully parsed, the
    configuration is returned as a dictionary. In case of a failure, such as
    the file not being found or a YAML parsing error, a warning is logged
    and an empty dictionary is returned.

    Args:
        base_path (Path): The path to the YAML configuration file.

    Returns:
        Dict[str, Any]: The loaded configuration as a dictionary, or an
        empty dictionary if loading fails.
    """
    try:
        config = data_contract_helper.load_yaml_file(base_path)
        logger.info(f"Loaded base configuration from {base_path}")
        return config
    except (FileNotFoundError, ValueError) as e:
        logger.warning(f"Failed to load base configuration: {e}")
        return {}

def list_yml_files(catalog: str) -> Generator[Path, None, None]:
    yml_dir = f"/Workspace/Users/rasmuslaursen@live.dk/.bundle/lakehouse_demo_project/developer/files/data_contracts/{catalog}/"
    yml_files = Path(yml_dir).glob("*.yml")
    return yml_files


def parse_arguments(variable_name: str, default: Any = None) -> Any:
    """
    Parses command line arguments to find the value associated with a given variable name.

    Command line arguments should be in the format `varname=value`. This function
    iterates over the arguments provided (excluding the program name) and returns
    the value corresponding to the specified variable name.

    Args:
        variable_name (str): The name of the variable to search for in the command line arguments.
        default (Any): Default value to return if variable is not found.

    Returns:
        Any: The value associated with the variable name if found, otherwise default.
    """
    logger.debug(f"Command line arguments: {sys.argv[1:]}")
    for variable in sys.argv[1:]:
        if "=" not in variable:
            continue
        varname = variable.split("=")[0]
        if varname.replace("--", "") == variable_name:
            varvalue = variable.split("=")[1]
            logger.debug(f"{varname} value: {varvalue}")
            return varvalue
    
    logger.debug(f"{variable_name} not found in command line arguments. Using default: {default}")
    return default


def list_volumes_in_schema(
    spark, source_catalog: str, source_schema: str, include_historic=False
) -> list:
    """
    Fetches a list of distinct volume names from a specified schema in the source catalog.

    Args:
        spark: A SparkSession object used to execute SQL queries.
        source_catalog (str): The name of the source catalog to query.
        source_schema (str): The name of the schema within the source catalog to query.

    Returns:
        list: A list of distinct volume names. If an error occurs during the query,
              an empty list is returned.
    """
    try:
        # Fetch distinct volume names
        volume_dll = f"""SELECT DISTINCT volume_name as object_name
        FROM {source_catalog}.information_schema.volumes
        WHERE volume_catalog = '{source_catalog}'
        AND volume_schema = '{source_schema}'"""

        if not include_historic:
            volume_dll += " AND volume_name not like '%historic%'"

        volume_list = spark.sql(
            f"""
                {volume_dll}
            """
        ).collect()
        logger.info(
            f"Found {len(volume_list)} volumes in schema {source_schema}. using {volume_dll}"
        )
    except Exception as e:
        logger.error(f"Error fetching volume list: {e}")
        volume_list = []
    return volume_list


def list_tables_in_schema(logger, spark, source_catalog, source_schema):
    """
    Fetches a list of distinct table names from a specified schema in a given catalog.

    Args:
        logger: A logging object used to log errors.
        spark: A SparkSession object used to execute SQL queries.
        source_catalog (str): The name of the catalog from which to fetch the tables.
        source_schema (str): The name of the schema from which to fetch the tables.

    Returns:
        list: A list of distinct table names in the specified schema.
              Returns an empty list if an error occurs during the fetch operation.
    """
    try:
        table_list = spark.sql(
            f"""
        SELECT DISTINCT table_name
        FROM {source_catalog}.information_schema.tables
        WHERE table_catalog = '{source_catalog}'
        AND table_schema = '{source_schema}'
        AND table_type != 'MANAGED'
        """
        ).collect()
        return table_list
    except Exception as e:
        logger.error(f"Error fetching table list: {e}")
        return []
