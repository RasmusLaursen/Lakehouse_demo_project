"""Spark session and Databricks utilities."""
from pyspark.sql import SparkSession
import json
from typing import Any


def get_spark() -> SparkSession:
    """
    Creates and returns a SparkSession object.

    This function attempts to create a DatabricksSession first. If the Databricks
    library is not available, it falls back to creating a standard SparkSession.

    Returns:
        SparkSession: An active SparkSession object.
    """
    try:
        from databricks.connect import DatabricksSession
        return DatabricksSession.builder.getOrCreate()
    except ImportError:
        return SparkSession.builder.getOrCreate()


def get_dbutils(spark: SparkSession = None):
    """
    Retrieves the DBUtils object for the given Spark session.

    In Databricks runtime, dbutils is available in the global namespace.
    This function attempts to access it from there first before falling back
    to creating a DBUtils instance.

    Args:
        spark (SparkSession): Optional Spark session (not used if dbutils is in globals)

    Returns:
        DBUtils or dbutils object from global namespace
    """
    try:
        # Try to get dbutils from globals (Databricks runtime)
        return globals()['dbutils']
    except KeyError:
        # Fall back to creating DBUtils if not in Databricks runtime
        if spark is None:
            spark = get_spark()
        from pyspark.dbutils import DBUtils
        return DBUtils(spark)


def get_pipeline_configurations(spark, configuration_name: str) -> Any:
    """
    Retrieves pipeline configurations from Spark conf.

    Args:
        spark (SparkSession): The Spark session.
        configuration_name (str): Specific configuration name to fetch.

    Returns:
        dict: Dictionary of configuration key-value pairs
    """
    configuration = spark.conf.get(configuration_name)
    try:
        parsed = json.loads(configuration)
        return parsed
    except (TypeError, json.JSONDecodeError):
        return {configuration_name: configuration}


def get_pipeline_configurations_from_spark(
    spark, source_system_name: str = None
) -> dict:
    """
    Retrieves pipeline configurations from Spark conf.

    Args:
        spark (SparkSession): The Spark session.
        source_system_name (str, optional): Source system name for system-specific configs.

    Returns:
        dict: Dictionary of configuration key-value pairs.
    """
    config_keys = [
        "landing_catalog",
        "raw_catalog",
        "base_catalog",
        "enriched_catalog",
        "curated_catalog",
    ]

    if source_system_name:
        config_keys.extend([
            f"{source_system_name}_landing_schema",
            f"{source_system_name}_raw_schema",
            f"{source_system_name}_base_schema",
        ])

    configs = {}
    for key in config_keys:
        value = spark.conf.get(key, None)
        configs[key] = value
    return configs
