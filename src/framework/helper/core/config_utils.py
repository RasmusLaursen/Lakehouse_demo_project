"""Common configuration and data utilities."""
from pathlib import Path
from typing import Dict, Any, Optional
from pydantic import ValidationError
from src.framework.helper.core import get_logger
from src.framework.helper.config import LayerConfig, TableConfig

logger = get_logger(__name__)


def get_path_for_data_configuration(catalog: str, object: str) -> Path:
    """
    Constructs the path to the data configuration file based on the provided catalog and object names.

    Args:
        catalog: The name of the catalog
        object: The name of the object

    Returns:
        Path to the configuration file
    """
    if catalog == "curated":
        return Path(f"../../data_configuration/{catalog}/{object}.yml")
    else:
        return Path(f"../data_configuration/{catalog}/{object}.yml")


def get_validate_data_configuration_contract(config: Dict[str, Any]) -> TableConfig:
    """
    Validates the provided data configuration dictionary against the TableConfig schema.

    Args:
        config: The data configuration dictionary to validate

    Returns:
        The validated TableConfig instance
        
    Raises:
        ValidationError if validation fails
    """
    try:
        validated_data_config = TableConfig(**config)
    except ValidationError as e:
        logger.error(f"TableConfig validation error: {e}")
        raise   
    return validated_data_config


def try_load_ingest_config(base_path: Path) -> Any:
    """
    Try to load the base configuration file from the specified path.

    This function attempts to read a YAML configuration file located at the
    given base path. If the file is found and successfully parsed, the
    configuration is returned as a dictionary. In case of a failure, such as
    the file not being found or a YAML parsing error, a warning is logged
    and an empty dictionary is returned.

    Args:
        base_path: The path to the YAML configuration file

    Returns:
        The loaded configuration as a dictionary, or an
        empty dictionary if loading fails
    """
    from src.framework.helper.contracts import load_yaml_file
    try:
        config = load_yaml_file(base_path)
        logger.info(f"Loaded base configuration from {base_path}")
        return config
    except (FileNotFoundError, ValueError) as e:
        logger.warning(f"Failed to load base configuration: {e}")
        return {}


def get_data_configuration(catalog: str, object: str) -> LayerConfig:
    """
    Constructs the path to the data configuration file based on the provided catalog and object names.

    Args:
        catalog: The name of the catalog
        object: The name of the object

    Returns:
        The validated LayerConfig instance
        
    Raises:
        FileNotFoundError if configuration file not found
        ValidationError if validation fails
    """
    data_configuration_path = get_path_for_data_configuration(
        catalog=catalog, object=object
    )

    if not data_configuration_path.is_file():
        raise FileNotFoundError(
            f"Data configuration file not found: {data_configuration_path}"
        )

    data_configuration = try_load_ingest_config(data_configuration_path)

    # Validate data_configuration against LayerConfig
    try:
        validated_data_config = LayerConfig(**data_configuration)
        return validated_data_config
    except ValidationError as e:
        logger.error(f"LayerConfig validation error: {e}")
        raise
