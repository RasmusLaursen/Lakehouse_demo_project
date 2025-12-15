"""
Data Contract Helper Module

This module provides utilities for loading, parsing, and validating data contracts
using the Open Data Contract Standard (ODCS).
"""

from pathlib import Path
from typing import Any, Dict, List, Optional
from open_data_contract_standard.model import OpenDataContractStandard
from open_data_contract_standard.model import SchemaObject
from open_data_contract_standard.model import CustomProperty
import yaml

from src.framework.helper import logging_helper, common

# Initialize logger
logger = logging_helper.get_logger(__name__)


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
    data_contract_path = common.find_data_contract_path(catalog, object_name)
    
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


def get_schema_from_contract(
    data_contract: OpenDataContractStandard,
    schema_name: Optional[str] = None
) -> SchemaObject:
    """
    Retrieves a specific schema from the data contract.

    Args:
        data_contract (OpenDataContractStandard): The data contract object.
        schema_name (str, optional): The name of the schema to retrieve. 
                                     If None, returns the first schema.

    Returns:
        SchemaObject: The requested schema object.

    Raises:
        ValueError: If the schema is not found in the contract.
    """
    if not data_contract.schema_:
        raise ValueError("No schemas found in data contract")

    if schema_name is None:
        return data_contract.schema_[0]

    for schema in data_contract.schema_:
        if schema.name == schema_name:
            return schema

    raise ValueError(f"Schema '{schema_name}' not found in data contract")


def get_custom_property(
    schema: SchemaObject,
    property_name: str,
    default: Any = None
) -> Any:
    """
    Retrieves a custom property value from a schema object.

    Args:
        schema (SchemaObject): The schema object.
        property_name (str): The name of the custom property to retrieve.
        default (Any): Default value if property not found.

    Returns:
        Any: The value of the custom property or default.
    """
    if not schema.customProperties:
        return default

    for prop in schema.customProperties:
        if isinstance(prop, CustomProperty) and prop.property == property_name:
            return prop.value
        elif isinstance(prop, dict) and prop.get("property") == property_name:
            return prop.get("value")

    return default


def get_all_schemas(data_contract: OpenDataContractStandard) -> List[SchemaObject]:
    """
    Retrieves all schemas from the data contract.

    Args:
        data_contract (OpenDataContractStandard): The data contract object.

    Returns:
        List[SchemaObject]: List of all schema objects in the contract.
    """
    return data_contract.schema_ or []


def get_schema_names(data_contract: OpenDataContractStandard) -> List[str]:
    """
    Retrieves all schema names from the data contract.

    Args:
        data_contract (OpenDataContractStandard): The data contract object.

    Returns:
        List[str]: List of schema names.
    """
    return [schema.name for schema in get_all_schemas(data_contract)] # type: ignore


def validate_data_contract(contract_path: Path) -> bool:
    """
    Validates a data contract file without fully loading it.

    Args:
        contract_path (Path): Path to the data contract file.

    Returns:
        bool: True if valid, False otherwise.
    """
    try:
        load_data_contract(contract_path)
        return True
    except Exception as e:
        logger.error(f"Data contract validation failed: {e}")
        return False


def get_scd_config(schema: SchemaObject) -> Dict[str, Any]:
    """
    Extracts SCD (Slowly Changing Dimension) configuration from schema custom properties.

    Args:
        schema (SchemaObject): The schema object.

    Returns:
        Dict[str, Any]: SCD configuration containing type, keys, etc.
    """
    scd_config = get_custom_property(schema, "scd_config", {})
    
    if not scd_config:
        # Try legacy format where scd_type might be separate
        scd_type = get_custom_property(schema, "scd_type")
        keys = get_custom_property(schema, "keys", [])
        
        if scd_type:
            scd_config = {
                "type": scd_type,
                "keys": keys
            }
    
    return scd_config


def get_loadtype(schema: SchemaObject) -> str:
    """
    Retrieves the load type from schema custom properties.

    Args:
        schema (SchemaObject): The schema object.

    Returns:
        str: The load type (e.g., 'volume_autoloader', 'batch', etc.)
    """
    return get_custom_property(schema, "loadtype", "batch")


def get_primary_keys(schema: SchemaObject) -> List[str]:
    """
    Extracts primary key column names from schema properties.

    Args:
        schema (SchemaObject): The schema object.

    Returns:
        List[str]: List of primary key column names.
    """
    if not schema.properties:
        return []
    
    primary_keys = []
    for prop in schema.properties:
        if hasattr(prop, 'primaryKey') and prop.primaryKey:
            primary_keys.append(prop.name)
    
    return primary_keys


def schema_to_table_config(schema: SchemaObject) -> Dict[str, Any]:
    """
    Converts an ODCS SchemaObject to a TableConfig-compatible dictionary.

    Args:
        schema (SchemaObject): The schema object from ODCS contract.

    Returns:
        Dict[str, Any]: Configuration dictionary compatible with TableConfig model.
    """
    # Get SCD configuration
    scd_type = get_custom_property(schema, "scd_type", 1)
    
    # Get primary keys
    keys = get_custom_property(schema, "keys")
    if not keys:
        keys = get_primary_keys(schema)
    
    # Get sequence column (default to common patterns)
    sequence_column = get_custom_property(schema, "sequence_column", "_metadata_ldp.ingest_timestamp")
    
    # Get track history configuration
    track_history_column_list = get_custom_property(schema, "track_history_columns")
    track_history_except_column_list = get_custom_property(schema, "track_history_except")
    
    # Get column lists
    column_list = get_custom_property(schema, "column_list")
    except_column_list = get_custom_property(schema, "except_column_list")
    
    # Get other optional fields
    backfill = get_custom_property(schema, "backfill")
    apply_as_deletes = get_custom_property(schema, "apply_as_deletes")
    apply_as_truncates = get_custom_property(schema, "apply_as_truncates")
    ignore_null_updates = get_custom_property(schema, "ignore_null_updates", False)
    data_quality = get_custom_property(schema, "data_quality", False)
    
    config = {
        "keys": keys if isinstance(keys, list) else [keys] if keys else [],
        "sequence_column": sequence_column,
        "stored_as_scd_type": int(scd_type),
        "data_quality": data_quality,
        "ignore_null_updates": ignore_null_updates,
    }
    
    # Add optional fields only if they exist
    if backfill:
        config["backfill"] = backfill
    if track_history_column_list:
        config["track_history_column_list"] = track_history_column_list
    if track_history_except_column_list:
        config["track_history_except_column_list"] = track_history_except_column_list
    if column_list:
        config["column_list"] = column_list
    if except_column_list:
        config["except_column_list"] = except_column_list
    if apply_as_deletes:
        config["apply_as_deletes"] = apply_as_deletes
    if apply_as_truncates:
        config["apply_as_truncates"] = apply_as_truncates
    
    return config


def schema_properties_to_spark_schema(schema: SchemaObject):
    """
    Convert data contract schema properties to a Spark StructType.
    
    Converts ODCS schema properties (with name and type) to PySpark StructType
    for use with Spark DataSource API. This avoids needing to infer schema from
    API responses and allows schema definition to come from the data contract.
    
    Args:
        schema (SchemaObject): The schema object from ODCS contract.
        
    Returns:
        StructType: PySpark schema definition
        
    Example:
        >>> schema_obj = get_schema_from_contract(contract, "MeteringPoints")
        >>> spark_schema = schema_properties_to_spark_schema(schema_obj)
        >>> df = spark.read.format("rest_api").schema(spark_schema).load()
    """
    from pyspark.sql.types import (
        StructType, StructField, StringType, IntegerType, LongType,
        DoubleType, FloatType, BooleanType, TimestampType, DateType,
        DecimalType
    )
    
    if not schema.properties:
        logger.warning(f"Schema '{schema.name}' has no properties. Returning generic schema.")
        return StructType([StructField("data", StringType(), True)])
    
    fields = []
    type_mapping = {
        "string": StringType(),
        "integer": IntegerType(),
        "long": LongType(),
        "int": IntegerType(),
        "bigint": LongType(),
        "double": DoubleType(),
        "float": FloatType(),
        "boolean": BooleanType(),
        "bool": BooleanType(),
        "timestamp": TimestampType(),
        "date": DateType(),
        "decimal": DecimalType(38, 18),
    }
    
    for prop in schema.properties:
        col_name = prop.name
        # Use logicalType which is the ODCS standard field (prefer it over deprecated type/physicalType)
        col_type_str = (prop.logicalType or prop.physicalType or "string").lower()
        col_required = not (hasattr(prop, 'required') and prop.required == False)  # Default to required
        
        # Map data contract type to PySpark type
        pyspark_type = type_mapping.get(col_type_str, StringType())
        
        fields.append(StructField(col_name, pyspark_type, col_required))
        logger.debug(f"Added field: {col_name} ({col_type_str} -> {type(pyspark_type).__name__})")
    
    logger.info(f"Created Spark schema for '{schema.name}' with {len(fields)} fields")
    return StructType(fields)
