"""Data contract loading and parsing utilities."""
from pathlib import Path
from typing import Any, Dict, Optional
from open_data_contract_standard.model import OpenDataContractStandard
from open_data_contract_standard.model import SchemaObject, CustomProperty
import yaml

from src.framework.helper.core import get_logger
from src.framework.helper.contracts.resolver import find_data_contract_path

logger = get_logger(__name__)


def load_yaml_file(file_path: Path) -> Dict[str, Any]:
    """
    Load a YAML file and return its contents.

    Args:
        file_path (Path): The path to the YAML file to be loaded.

    Returns:
        Dict[str, Any]: The contents of the YAML file as a dictionary.

    Raises:
        FileNotFoundError: If the specified file does not exist.
        ValueError: If there is an error parsing the YAML file.
    """
    try:
        with open(file_path, "r") as file:
            data = yaml.safe_load(file)
            return data
    except FileNotFoundError:
        raise FileNotFoundError(f"The file at {file_path} was not found.")
    except yaml.YAMLError as e:
        raise ValueError(f"Error parsing YAML file: {e}")


def get_data_contract_path(catalog: str, object_name: str) -> Path:
    """
    Constructs the path to the data contract file based on the provided catalog and object names.

    Args:
        catalog (str): The name of the catalog (e.g., 'source_system', 'curated').
        object_name (str): The name of the object/contract.

    Returns:
        Path: The constructed path to the data contract file.
        
    Raises:
        FileNotFoundError: If the contract file cannot be found.
    """
    data_contract_path = find_data_contract_path(catalog, object_name)
    
    if data_contract_path is None:
        raise FileNotFoundError(
            f"Data contract file not found: {object_name}.yml in catalog '{catalog}'."
        )
    
    return data_contract_path


def load_data_contract(contract_path: Path) -> OpenDataContractStandard:
    """
    Loads and parses a data contract from the specified YAML file using ODCS.

    Args:
        contract_path (Path): The path to the data contract YAML file.

    Returns:
        OpenDataContractStandard: The parsed ODCS data contract object.

    Raises:
        FileNotFoundError: If the contract file is not found.
        ValueError: If the contract fails validation.
    """
    if not contract_path.is_file():
        raise FileNotFoundError(
            f"Data contract file not found: {contract_path}"
        )

    try:
        contract_data = load_yaml_file(contract_path)
        data_contract = OpenDataContractStandard(**contract_data)
        logger.info(f"Successfully loaded data contract from {contract_path}")
        return data_contract
    except Exception as e:
        logger.error(f"Failed to parse data contract from {contract_path}: {e}")
        raise ValueError(f"Invalid data contract: {e}")


def get_data_contract(catalog: str, object_name: str) -> OpenDataContractStandard:
    """
    Retrieves and parses the data contract for a specific catalog and object.

    Args:
        catalog (str): The name of the catalog.
        object_name (str): The name of the object.

    Returns:
        OpenDataContractStandard: The parsed data contract.
    """
    contract_path = get_data_contract_path(catalog, object_name)
    return load_data_contract(contract_path)
