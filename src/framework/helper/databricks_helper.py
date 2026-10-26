"""Backward compatibility module for databricks utilities.

This module re-exports functions from src.framework.helper.core for backward compatibility
with code that imports databricks_helper directly.
"""

from src.framework.helper.core.spark import (
    get_spark,
    get_dbutils,
    get_pipeline_configurations,
    get_pipeline_configurations_from_spark,
)

__all__ = [
    "get_spark",
    "get_dbutils",
    "get_pipeline_configurations",
    "get_pipeline_configurations_from_spark",
]
