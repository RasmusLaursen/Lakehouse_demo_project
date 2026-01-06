"""Data quality utilities using Databricks DQX."""
from src.framework.helper.core import get_spark
from src.framework.helper.core import get_logger
from src.framework.helper.contracts import load_data_contract, find_data_contract_path
from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.profiler.generator import DQGenerator
from pyspark.sql import SparkSession
from typing import Optional, List, Dict, Any

logger = get_logger(__name__)


def get_ws_client() -> WorkspaceClient:
    """Get Databricks workspace client."""
    ws = WorkspaceClient()
    return ws


def get_dq_engine(ws: WorkspaceClient) -> DQEngine:
    """Get DQX engine instance."""
    dq_engine = DQEngine(ws)
    return dq_engine


def get_dqx_generator(ws: WorkspaceClient, spark: SparkSession) -> DQGenerator:
    """Get DQX generator instance."""
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
    data_quality_path = find_data_contract_path(catalog, object)

    if not data_quality_path:
        logger.warning(f"Data quality configuration file not found: {object}.yml in catalog '{catalog}'")
        return None

    checks = generator.generate_rules_from_contract(
        contract_file=str(data_quality_path)
    )

    status = dq_engine.validate_checks(checks)
    if status.has_errors:
        logger.warning(f"Data quality checks failed. {status}")
        return {}
    else:
        return checks
