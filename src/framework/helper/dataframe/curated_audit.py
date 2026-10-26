"""Curated layer audit column utilities for dimensions and facts."""

from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp, current_user
from src.framework.helper.core import get_logger

logger = get_logger(__name__)


def add_curated_audit_columns(df: DataFrame) -> DataFrame:
    """Add standard audit columns to curated tables (dimensions and facts).

    Adds:
    - created_at: Timestamp when record was created
    - updated_at: Timestamp when record was last updated
    - created_by: User who created the record

    Args:
        df: Input dataframe

    Returns:
        DataFrame with audit columns added
    """
    df = df.withColumn("created_at", current_timestamp())
    df = df.withColumn("updated_at", current_timestamp())
    df = df.withColumn("created_by", current_user())

    logger.debug("Added curated audit columns: created_at, updated_at, created_by")
    return df
