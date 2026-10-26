"""DataFrame writing utilities."""
from pyspark.sql import DataFrame
from src.framework.helper.core import get_logger

logger = get_logger(__name__)


def write_volume(
    target_catalog: str,
    target_schema: str,
    target_name: str,
    source_dataframe: DataFrame,
    mode: str = "overwrite",
    file_format: str = "csv",
) -> None:
    """
    Writes a DataFrame to a specified location in a given format.

    Args:
        target_catalog (str): The target catalog where the DataFrame will be saved.
        target_schema (str): The target schema within the catalog.
        target_name (str): The name of the target file.
        source_dataframe (DataFrame): The DataFrame to be written to the target location.
        mode (str, optional): The write mode. Defaults to "overwrite".
        file_format (str, optional): The format in which to save the DataFrame. Defaults to "csv".

    Returns:
        None
    """
    logger.info(f"Writing to {target_catalog}.{target_schema}.{target_name} with mode {mode}")
    source_dataframe.write.mode(mode).format(file_format).save(
        f"dbfs:/Volumes/{target_catalog}/{target_schema}/{target_name}"
    )
    logger.info(f"Successfully wrote DataFrame to volume {target_name}")
