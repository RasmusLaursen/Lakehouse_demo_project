"""DEPRECATED: Use src.framework.helper.quality module instead.

This module is deprecated and will be removed in a future version.
Please migrate to the new organized helper modules:
  - from src.framework.helper import get_data_quality_configuration
  - from src.framework.helper.quality import get_data_quality_configuration
"""
import warnings
from src.framework.helper import databricks_helper, logging_helper
from databricks.sdk import WorkspaceClient

warnings.warn(
    "dqx_helper module is deprecated. Use src.framework.helper.quality instead.",
    DeprecationWarning,
    stacklevel=2
)
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.config import FileChecksStorageConfig
from databricks.labs.dqx.profiler.generator import DQGenerator
from pyspark.sql import SparkSession
from pathlib import Path
from typing import Optional, List, Dict, Any
from src.framework.helper import data_contract_helper

# Initialize logger
logger = logging_helper.get_logger(__name__)

def get_ws_client() -> WorkspaceClient:
    ws = WorkspaceClient()
    return ws

def get_dq_engine(ws: WorkspaceClient) -> DQEngine:
    dq_engine = DQEngine(ws)
    return dq_engine

def get_dqx_generator(ws : WorkspaceClient, spark: SparkSession) -> DQGenerator:
    generator = DQGenerator(workspace_client=ws)
    return generator

def get_data_quality_configuration(catalog: str, object: str, spark: SparkSession) -> Optional[List[Dict[str, Any]]]:
    """Get data quality configuration from data contract.
    
    Args:
        catalog: The catalog name (e.g., 'source_system', 'curated')
        object: The object/contract name
        spark: Active SparkSession
        
    Returns:
        List of data quality checks if found and valid, None otherwise
    """
    ws = get_ws_client()
    dq_engine = get_dq_engine(ws)
    generator = get_dqx_generator(ws, spark)

    # Use centralized path resolution
    data_quality_path = data_contract_helper.find_data_contract_path(catalog, object)

    if not data_quality_path:
        logger.warning(f"Data quality configuration file not found: {object}.yml in catalog '{catalog}'")
        return None

    checks = generator.generate_rules_from_contract(
        contract_file=str(data_quality_path)
    )
    # checks: list[dict] = dq_engine.load_checks(config=FileChecksStorageConfig(location=str(data_quality_path)))


    status = dq_engine.validate_checks(checks)
    if status.has_errors:
        logger.warning(f"Data quality checks failed. {status}")
        return {}
    else:
        return checks
