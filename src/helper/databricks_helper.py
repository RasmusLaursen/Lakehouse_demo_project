from pyspark.sql import SparkSession


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


from pyspark.dbutils import DBUtils


def get_dbutils(spark: SparkSession) -> DBUtils:
    """
    Retrieves the DBUtils object for the given Spark session.

    This function creates and returns a DBUtils instance that can be used
    to interact with Databricks utilities within the provided Spark session.

    Args:
        spark (SparkSession): The Spark session from which to create the DBUtils.

    Returns:
        DBUtils: An instance of DBUtils associated with the provided Spark session.
    """
    return DBUtils(spark)


# TODO 
# Find smarter way to parse all catalogs and schemas from configuration of pipeline

def get_pipeline_configurations_from_spark(
    spark, source_system_name: str = None
) -> dict:
    """
    Retrieves pipeline configurations from Spark conf.

    Args:
        spark (SparkSession): The Spark session.
        config_keys (list): List of configuration keys to fetch.

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
        config_keys.extend(
            [
                f"{source_system_name}_landing_schema",
                f"{source_system_name}_raw_schema",
                f"{source_system_name}_base_schema",
            ]
        )

    configs = {}
    for key in config_keys:
        value = spark.conf.get(key, None)
        configs[key] = value
    return configs
