from pyspark.sql import DataFrame
from pyspark.sql.functions import struct, current_timestamp, lit
import yaml
from typing import Dict, Any
from src.helper import logging_helper
import sys

# Initialize logger
logger = logging_helper.get_logger(__name__)


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
    df = df.withColumn("_metadata", metadata)
    return df


def _load_yaml_file(file_path):
    """
    Load a YAML file and return its contents.

    Args:
        file_path (str): The path to the YAML file to be loaded.

    Returns:
        dict: The contents of the YAML file as a dictionary.

    Raises:
        FileNotFoundError: If the specified file does not exist.
        ValueError: If there is an error parsing the YAML file.
    """
    try:
        with open(file_path, "r") as file:
            data = yaml.safe_load(file)
            return data
    except FileNotFoundError:
        raise FileNotFoundError(f"The file at {file_path} was not found.")
    except yaml.YAMLError as e:
        raise ValueError(f"Error parsing YAML file: {e}")


def try_load_ingest_config(base_path: str) -> Dict[str, Any]:
    """
    Try to load the base configuration file from the specified path.

    This function attempts to read a YAML configuration file located at the 
    given base path. If the file is found and successfully parsed, the 
    configuration is returned as a dictionary. In case of a failure, such as 
    the file not being found or a YAML parsing error, a warning is logged 
    and an empty dictionary is returned.

    Args:
        base_path (str): The path to the YAML configuration file.

    Returns:
        Dict[str, Any]: The loaded configuration as a dictionary, or an 
        empty dictionary if loading fails.
    """
    # base_path = os.path.join(os.path.dirname(__file__), filename)
    try:
        config = _load_yaml_file(base_path)
        logger.info(f"Loaded base configuration from {base_path}")
        return config
    except (FileNotFoundError, yaml.YAMLError) as e:
        logger.warning(f"Failed to load base configuration: {e}")
        return {}


def parse_arguments(variable_name: str) -> Any:
    """
    Parses command line arguments to find the value associated with a given variable name.

    Command line arguments should be in the format `varname=value`. This function
    iterates over the arguments provided (excluding the program name) and returns
    the value corresponding to the specified variable name.

    Args:
        variable_name (str): The name of the variable to search for in the command line arguments.

    Returns:
        Any: The value associated with the variable name if found, otherwise None.
    """
    logger.debug(f"Command line arguments: {sys.argv[1:]}")
    for variable in sys.argv[
        1:
    ]:  # Now we're going to iterate over argv[1:] (argv[0] is the program name)
        if (
            "=" not in variable
        ):  # Then skip this value because it doesn't have the varname=value format
            continue
        varname = variable.split("=")[0]  # Get what's left of the '='
        if varname.replace("--", "") == variable_name:
            varvalue = variable.split("=")[1]  # Get what's right of the '='
            logger.debug(f"{varname} value: {varvalue}")
            return varvalue
    logger.debug(f"{variable_name} not found in command line arguments.")
    return None


def list_volumes_in_schema(logger, spark, source_catalog, source_schema) -> list:
    """
    Fetches a list of distinct volume names from a specified schema in the source catalog.

    Args:
        logger: A logging object used to log errors.
        spark: A SparkSession object used to execute SQL queries.
        source_catalog (str): The name of the source catalog to query.
        source_schema (str): The name of the schema within the source catalog to query.

    Returns:
        list: A list of distinct volume names. If an error occurs during the query,
              an empty list is returned.
    """
    try:
        # Fetch distinct volume names
        volume_list = spark.sql(
            f"""
        SELECT DISTINCT volume_name as object_name
        FROM {source_catalog}.information_schema.volumes
        WHERE volume_catalog = '{source_catalog}'
        AND volume_schema = '{source_schema}'
        """
        ).collect()
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
    return table_list
