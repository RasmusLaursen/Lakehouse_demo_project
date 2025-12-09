from src.framework.helper import databricks_helper, logging_helper
from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.config import FileChecksStorageConfig
from databricks.labs.dqx.profiler.generator import DQGenerator
from pyspark.sql import SparkSession
from pathlib import Path

# Initialize logger
logger = logging_helper.get_logger(__name__)

spark = databricks_helper.get_spark()

def get_ws_client() -> WorkspaceClient:
    ws = WorkspaceClient()
    return ws

def get_dq_engine(ws: WorkspaceClient) -> DQEngine:
    dq_engine = DQEngine(ws)
    return dq_engine

def get_dqx_generator(ws : WorkspaceClient, spark: SparkSession) -> DQGenerator:
    generator = DQGenerator(workspace_client=ws, spark=spark)
    return generator

def get_data_quality_configuration(catalog:str, object:str, spark: SparkSession):
    ws = get_ws_client()
    dq_engine = get_dq_engine(ws)
    generator = get_dqx_generator(ws, spark)

    # Try multiple paths to find data contract file
    possible_paths = [
        Path(f"data_contracts/{catalog}/{object}.yml"),           # From project root
        Path(f"../data_contracts/{catalog}/{object}.yml"),        # From src/
        Path(f"../../data_contracts/{catalog}/{object}.yml"),     # From src/framework/
        Path(f"../../../data_contracts/{catalog}/{object}.yml"),  # From src/framework/helper/
    ]
    
    data_quality_path = None
    for path in possible_paths:
        if path.is_file():
            data_quality_path = path
            break

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
