from src.helper import databricks_helper
from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.config import FileChecksStorageConfig
from databricks.labs.dqx.profiler.generator import DQGenerator
from pyspark.sql import SparkSession
from pathlib import Path

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

    data_quality_path = ''

    if catalog == "curated":
        data_quality_path = Path(f"../../../data_contracts/{catalog}/{object}.yml")
    else:
        data_quality_path = Path(f"../../data_contracts/{catalog}/{object}.yml")

    if not data_quality_path.is_file():
        logger.warning(f"Data quality configuration file not found: {data_quality_path}")
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
