"""DataFrame audit column utilities."""
from pyspark.sql import DataFrame
from pyspark.sql.functions import struct, current_timestamp, lit
from src.framework.helper.core import get_logger

logger = get_logger(__name__)


def add_audit_columns(df: DataFrame, source_system: str = "unknown") -> DataFrame:
    """
    Adds audit columns to the given DataFrame.

    This function appends a metadata column to the DataFrame, which includes
    the source system and the current ingest timestamp.

    Args:
        df (DataFrame): The input DataFrame to which audit columns will be added.
        source_system (str, optional): The name of the source system. Defaults to "unknown".

    Returns:
        DataFrame: A new DataFrame with the added audit columns.
    """
    metadata = struct(
        lit(source_system).alias("SourceSystem"),
        current_timestamp().alias("ingest_timestamp"),
    )
    df = df.withColumn("_metadata_ldp", metadata)
    logger.debug(f"Added audit columns for source system: {source_system}")
    return df
