from pyspark.sql import DataFrame
from pyspark.sql.functions import struct, current_timestamp, lit
import yaml
from typing import Dict, Any
from src.helper import logging_helper
import sys

# Initialize logger
logger = logging_helper.get_logger(__name__)


def add_audit_columns(df: DataFrame) -> DataFrame:
    metadata = struct(
        lit("lakehouse_dummy_data").alias("SourceSystem"),
        current_timestamp().alias("ingest_timestamp"),
    )
    df = df.withColumn("_metadata", metadata)
    return df


def _load_yaml_file(file_path):
    try:
        with open(file_path, "r") as file:
            data = yaml.safe_load(file)
            return data
    except FileNotFoundError:
        raise FileNotFoundError(f"The file at {file_path} was not found.")
    except yaml.YAMLError as e:
        raise ValueError(f"Error parsing YAML file: {e}")


def try_load_ingest_config(base_path: str) -> Dict[str, Any]:
    """Try to load the base configuration file."""
    # base_path = os.path.join(os.path.dirname(__file__), filename)
    try:
        config = _load_yaml_file(base_path)
        logger.info(f"Loaded base configuration from {base_path}")
        return config
    except (FileNotFoundError, yaml.YAMLError) as e:
        logger.warning(f"Failed to load base configuration: {e}")
        return {}


def parse_arguments(variable_name: str) -> Any:
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
    try:
        # Fetch distinct volume names
        volume_list = spark.sql(
            f"""
        SELECT DISTINCT volume_name
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
