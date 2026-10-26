"""Curated dimension table for Eloverblik metering points.

This dimension processes metering point (electricity meter) data from Eloverblik,
excluding metadata columns and standardizing column names to snake_case.
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from src.framework.factory.dimension_factory import create_dimension
from src.framework.helper import get_spark
from src.framework.config import CentralizedPipelineConfig, CatalogSchemaManager
import re


def transform_metering_points() -> DataFrame:
    """Transform metering points data by excluding metadata columns and converting to snake_case.
    
    Transformations applied:
    1. Read from base layer meteringpoints table
    2. Filter for active records (__END_AT IS NULL)
    3. Exclude metadata columns: _errors, _warning, __START_AT, __END_AT
    4. Convert all column names to snake_case
    
    Returns:
        DataFrame with cleaned columns and snake_case names
    """
    # Get spark and read source table
    spark = get_spark()
    centralized_config = CentralizedPipelineConfig.from_spark(spark, "eloverblik")
    catalog_manager = CatalogSchemaManager.from_pipeline_config(centralized_config)
    
    # Read from base layer
    base_table_path = catalog_manager.get_base_table_path("meteringpoints")
    # Base table is an AUTO CDC (MERGE) target, so stream its change feed instead of the table
    df = (
        spark.readStream
        .option("readChangeFeed", "true")
        .table(base_table_path)
        .filter(col("_change_type").isin("insert", "update_postimage"))
    )
    
    # Filter for active records
    df = df.filter(col("__END_AT").isNull())
    
    # Columns to exclude (_commit_version is kept as the CDC sequence column)
    exclude_columns = [
        "_errors", "_warning", "__START_AT", "__END_AT",
        "_change_type", "_commit_timestamp",
    ]
    
    # Select all columns except excluded ones
    selected_columns = [c for c in df.columns if c not in exclude_columns]
    df = df.select(*selected_columns)
    
    # Convert column names to snake_case
    def to_snake_case(name: str) -> str:
        """Convert column name to snake_case."""
        # Replace special characters with underscore
        name = re.sub(r'[^a-zA-Z0-9]', '_', name)
        # Insert underscore before uppercase letters
        name = re.sub(r'(?<!^)(?=[A-Z])', '_', name)
        # Convert to lowercase and remove duplicate underscores
        name = name.lower()
        name = re.sub(r'_+', '_', name)
        # Remove leading/trailing underscores
        name = name.strip('_')
        return name
    
    # Rename all columns to snake_case
    for old_name in df.columns:
        new_name = to_snake_case(old_name)
        if old_name != new_name:
            df = df.withColumnRenamed(old_name, new_name)

    # Rename metering_point_id to metering_point_key if it exists
    if "metering_point_id" in df.columns:
        df = df.withColumnRenamed("metering_point_id", "meteringpoint_key")
    
    return df


# Create dimension using new factory pattern
create_dimension(
    dimension_name='dim_meteringpoint',
    keys=['meteringpoint_key'],  # Business key from data contract
    sequence_column='commit_version',  # Base table commit version (snake_cased _commit_version)
    transform_fn=transform_metering_points,
    scd_type=1,  # Track history of changes
    source_system='eloverblik',
    except_column_list=['commit_version'],
)
