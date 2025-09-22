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
