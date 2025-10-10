from pyspark.sql import SparkSession, DataFrame
from src.helper import common
from src.helper import logging_helper
from src.helper import databricks_helper

# Initialize logger
logger = logging_helper.get_logger(__name__)

spark = databricks_helper.get_spark()


def read_stream_table(
    source_catalog: str,
    source_schema: str,
    objectname: str,
    add_audit_column: bool = False,
) -> DataFrame:
    """
    Reads a streaming table from the specified catalog and schema.

    Args:
        source_catalog (str): The name of the source catalog.
        source_schema (str): The name of the source schema.
        objectname (str): The name of the object (table) to read.
        add_audit_column (bool, optional): If True, adds audit columns to the DataFrame. Defaults to False.

    Returns:
        DataFrame: A streaming DataFrame representing the table.
    """
    df = spark.readStream.table(f"{source_catalog}.{source_schema}.{objectname}")
    if add_audit_column:
        df = common.add_audit_columns(df=df)
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
    df = spark.read.table(f"{source_catalog}.{source_schema}.{objectname}")
    return df


def read_volume(
    source_catalog: str,
    source_schema: str,
    volume_name: str,
    file_path: str = "",
    file_format: str = "parquet",
    add_audit_column: bool = False,
    **options,
) -> DataFrame:
    """
    Reads data from a Databricks Unity Catalog volume using PySpark.

    Args:
        source_catalog (str): The name of the source catalog.
        source_schema (str): The name of the source schema.
        volume_name (str): The name of the volume.
        file_path (str, optional): The path within the volume to read from. Defaults to "".
        file_format (str, optional): The format of the files to read (e.g., 'parquet', 'csv', 'json'). Defaults to "parquet".
        add_audit_column (bool, optional): If True, adds audit columns to the DataFrame. Defaults to False.
        **options: Additional options to pass to the DataFrameReader (e.g., header=True for CSV).

    Returns:
        DataFrame: A Spark DataFrame containing the data from the specified volume.

    Example:
        >>> df = read_volume("my_catalog", "my_schema", "my_volume", "data/customers.parquet")
        >>> df_csv = read_volume("my_catalog", "my_schema", "my_volume", "data/customers.csv", "csv", header=True)
    """
    volume_path = f"/Volumes/{source_catalog}/{source_schema}/{volume_name}/{file_path}"

    logger.info(f"Reading from volume path: {volume_path} with format: {file_format}")

    try:
        df = spark.read.format(file_format).options(**options).load(volume_path)

        if add_audit_column:
            df = common.add_audit_columns(df=df)

        logger.info(f"Successfully read data from volume: {volume_name}")
        return df

    except Exception as e:
        logger.error(f"Failed to read from volume {volume_name}: {str(e)}")
        raise


def read_volume_autoloader(
    source_catalog: str,
    source_schema: str,
    objectname: str,
    filetype: str,
    add_audit_column: bool = False,
) -> DataFrame:
    """
    Reads data from cloud files using Spark's structured streaming.

    Parameters:
    ----------
    source_catalog : str
        The name of the source catalog where the files are stored.
    source_schema : str
        The schema within the source catalog.
    objectname : str
        The name of the object (file or directory) to read.
    filetype : str
        The format of the files to read (e.g., 'csv', 'json', etc.).
    add_audit_column : bool, optional
        If True, adds audit columns to the DataFrame (default is False).

    Returns:
    -------
    DataFrame
        A Spark DataFrame representing the streamed data from the specified cloud files.

    Example:
    --------
    >>> df = read_volume_autoloader("my_catalog", "my_schema", "my_object", "csv", True)
    >>> df.printSchema()
    """
    df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", filetype)
        .load(f"/Volumes/{source_catalog}/{source_schema}/{objectname}/")
    )
    if add_audit_column:
        df = common.add_audit_columns(df=df)
    return df


def read_dataframe(
    source_catalog: str,
    source_schema: str,
    objectname: str,
    add_audit_columns: bool = False,
) -> DataFrame:
    """
    Reads a DataFrame from a specified source catalog, schema, and object name.

    Args:
        source_catalog (str): The name of the source catalog.
        source_schema (str): The name of the source schema.
        objectname (str): The name of the object to read.
        add_audit_columns (bool, optional): If True, adds audit columns to the DataFrame. Defaults to False.

    Returns:
        DataFrame: The resulting DataFrame read from the specified source.
    """
    df = spark.read.dataframe(f"{source_catalog}.{source_schema}.{objectname}")
    if add_audit_columns:
        df = common.add_audit_columns(df=df)
    return df
