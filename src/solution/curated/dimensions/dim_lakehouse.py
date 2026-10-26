"""Curated dimension table for lakehouse.

This module uses the factory pattern for generic dimension logic,
while keeping custom transformation logic (joins) in this file.
"""

from pyspark.sql import DataFrame
from src.framework.factory.dimension_factory import create_dimension
from src.framework.helper import get_spark
from src.framework.config import CentralizedPipelineConfig, CatalogSchemaManager


def custom_lakehouse_transform() -> DataFrame:
    """Apply lakehouse-specific transformations.

    Joins with meta_lakehouses table to enrich with lakehouse name.

    Args:
        df: Source lakehouse dataframe from base layer

    Returns:
        DataFrame with meta_lakehouses joined
    """
    spark = get_spark()
    centralized_config = CentralizedPipelineConfig.from_spark(spark)
    catalog_manager = CatalogSchemaManager.from_pipeline_config(centralized_config)

    df = spark.read.table(
        catalog_manager.base_catalog + "." + catalog_manager.base_schema + ".lakehouse"
    )

    # Read lookup table
    meta_lakehouse_df = spark.read.table(
        catalog_manager.get_base_table_path("meta_lakehouses")
    )

    # Join to get lakehouse name
    df = df.join(meta_lakehouse_df, on="lakehouse_name_id", how="left").select(
        df["*"], meta_lakehouse_df["lakehouse_name"].alias("name")
    )

    df = df.withColumnRenamed(
        "lakehouse_id", "lakehouse_key"
    )  # Rename business key to *_key convention

    return df


# Create dimension using new factory pattern
create_dimension(
    dimension_name="dim_lakehouse",
    keys=["lakehouse_key"],  # Business key from data contract
    sequence_column="created_at",  # Use CDC sequence column
    transform_fn=custom_lakehouse_transform,
    scd_type=1,  # Track history of changes
)
