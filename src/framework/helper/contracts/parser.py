"""Data contract schema parsing and custom property utilities."""
from typing import Any, List, Optional
from open_data_contract_standard.model import OpenDataContractStandard
from open_data_contract_standard.model import SchemaObject, CustomProperty

from src.framework.helper.core import get_logger

logger = get_logger(__name__)


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
    return [schema.name for schema in get_all_schemas(data_contract)]  # type: ignore


def get_primary_keys(schema: SchemaObject) -> Optional[List[str]]:
    """
    Extracts primary key column names from a schema object.

    Args:
        schema (SchemaObject): The schema object from ODCS contract.

    Returns:
        Optional[List[str]]: List of primary key column names, or None if not defined.
    """
    # Try to get from customProperties
    keys = get_custom_property(schema, "keys")
    if keys:
        return keys if isinstance(keys, list) else [keys]

    # Try to get from customProperties "primary_keys"
    primary_keys = get_custom_property(schema, "primary_keys")
    if primary_keys:
        return primary_keys if isinstance(primary_keys, list) else [primary_keys]

    # Try to extract from fields marked as primary key
    if schema.fields:
        pk_fields = [field.name for field in schema.fields if getattr(field, 'isPrimaryKey', False)]
        if pk_fields:
            return pk_fields

    return None

