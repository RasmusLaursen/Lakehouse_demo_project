from pyspark.sql import SparkSession


def get_spark() -> SparkSession:
    try:
        from databricks.connect import DatabricksSession

        return DatabricksSession.builder.getOrCreate()
    except ImportError:
        return SparkSession.builder.getOrCreate()


from pyspark.dbutils import DBUtils


def get_dbutils(spark: SparkSession) -> DBUtils:
    return DBUtils(spark)
