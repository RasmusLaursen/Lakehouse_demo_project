"""DataFrame reading utilities."""
from pyspark.sql import SparkSession, DataFrame
from src.framework.helper.core import get_spark, get_logger

logger = get_logger(__name__)


def read_stream_table(
    source_catalog: str,
    source_schema: str,
    objectname: str,
    add_audit_column: bool = False,
    source_system: str = "unknown",
) -> DataFrame:
    """
    Reads a streaming table from the specified catalog and schema.

    Args:
        source_catalog (str): The name of the source catalog.
        source_schema (str): The name of the source schema.
        objectname (str): The name of the object (table) to read.
        add_audit_column (bool, optional): If True, adds audit columns to the DataFrame. Defaults to False.
        source_system (str, optional): The name of the source system for audit columns. Defaults to "unknown".

    Returns:
        DataFrame: A streaming DataFrame representing the table.
    """
    spark = get_spark()
    df = spark.readStream.table(f"{source_catalog}.{source_schema}.{objectname}")
    if add_audit_column:
        from src.framework.helper.dataframe.audit import add_audit_columns
        df = add_audit_columns(df=df, source_system=source_system)
    return df


def read_table(source_catalog: str, source_schema: str, objectname: str) -> DataFrame:
    """
    Reads a table from a specified catalog and schema in Spark.

    Args:
        source_catalog (str): The name of the source catalog.
        source_schema (str): The name of the source schema.
        objectname (str): The name of the object (table) to read.

    Returns:
        DataFrame: A Spark DataFrame containing the data from the specified table.
    """
    spark = get_spark()
    df = spark.read.table(f"{source_catalog}.{source_schema}.{objectname}")
    return df


def read_dataframe(
    source_catalog: str,
    source_schema: str,
    objectname: str,
    add_audit_columns_flag: bool = False,
    source_system: str = "unknown",
) -> DataFrame:
    """
    Reads a DataFrame from a specified source catalog, schema, and object name.

    Args:
        source_catalog (str): The name of the source catalog.
        source_schema (str): The name of the source schema.
        objectname (str): The name of the object to read.
        add_audit_columns_flag (bool, optional): If True, adds audit columns to the DataFrame. Defaults to False.
        source_system (str, optional): The name of the source system for audit columns. Defaults to "unknown".

    Returns:
        DataFrame: The resulting DataFrame read from the specified source.
    """
    spark = get_spark()
    df = spark.read.table(f"{source_catalog}.{source_schema}.{objectname}")
    if add_audit_columns_flag:
        from src.framework.helper.dataframe.audit import add_audit_columns
        df = add_audit_columns(df=df, source_system=source_system)
    return df
