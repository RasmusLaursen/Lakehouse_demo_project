"""Utility functions for common operations."""
import sys
from pathlib import Path
from typing import Any, Generator, List, Optional
from src.framework.helper.core import get_logger

logger = get_logger(__name__)


def parse_arguments(variable_name: str, default: Any = None) -> Any:
    """
    Parses command line arguments to find the value associated with a given variable name.

    Command line arguments should be in the format `varname=value`. This function
    iterates over the arguments provided (excluding the program name) and returns
    the value corresponding to the specified variable name.

    Args:
        variable_name: The name of the variable to search for in the command line arguments
        default: Default value to return if variable is not found

    Returns:
        The value associated with the variable name if found, otherwise default
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


def list_yml_files(catalog: str) -> Generator[Path, None, None]:
    """
    List all YAML files in a catalog directory.
    
    Args:
        catalog: The catalog name
        
    Yields:
        Path objects for each .yml file found
    """
    yml_dir = f"/Workspace/Users/rasmuslaursen@live.dk/.bundle/lakehouse_demo_project/developer/files/data_contracts/{catalog}/"
    yml_files = Path(yml_dir).glob("*.yml")
    return yml_files


def list_volumes_in_schema(
    spark, source_catalog: str, source_schema: str, include_historic: bool = False
) -> list:
    """
    Fetches a list of distinct volume names from a specified schema in the source catalog.

    Args:
        spark: A SparkSession object used to execute SQL queries
        source_catalog: The name of the source catalog to query
        source_schema: The name of the schema within the source catalog to query
        include_historic: Whether to include volumes with 'historic' in their name

    Returns:
        A list of distinct volume names. If an error occurs during the query,
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

        volume_list = spark.sql(volume_dll).collect()
        logger.info(
            f"Found {len(volume_list)} volumes in schema {source_schema}. using {volume_dll}"
        )
    except Exception as e:
        logger.error(f"Error fetching volume list: {e}")
        volume_list = []
    return volume_list


def list_tables_in_schema(spark, source_catalog: str, source_schema: str) -> list:
    """
    Fetches a list of distinct table names from a specified schema in a given catalog.

    Args:
        spark: A SparkSession object used to execute SQL queries
        source_catalog: The name of the catalog from which to fetch the tables
        source_schema: The name of the schema from which to fetch the tables

    Returns:
        A list of distinct table names in the specified schema.
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
