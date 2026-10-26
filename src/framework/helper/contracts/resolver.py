"""Data contract path resolution utilities."""
from pathlib import Path
from typing import Optional
from src.framework.helper.core import get_logger

logger = get_logger(__name__)


def find_data_contract_path(catalog: str, object_name: str) -> Optional[Path]:
    """
    Find a data contract file by searching multiple possible paths.
    
    This utility centralizes path resolution logic used across multiple modules.
    
    Args:
        catalog (str): The name of the catalog (e.g., 'source_system', 'curated').
        object_name (str): The name of the object/contract.
    
    Returns:
        Optional[Path]: The path to the contract file if found, None otherwise.
    """
    possible_paths = [
        Path(f"data_contracts/{catalog}/{object_name}.yml"),
        Path(f"../data_contracts/{catalog}/{object_name}.yml"),
        Path(f"../../data_contracts/{catalog}/{object_name}.yml"),
        Path(f"../../../data_contracts/{catalog}/{object_name}.yml"),
        Path(__file__).parent.parent.parent.parent.parent / "data_contracts" / catalog / f"{object_name}.yml"
    ]
    
    for path in possible_paths:
        if path.is_file():
            logger.debug(f"Found data contract at: {path}")
            return path
    
    return None
