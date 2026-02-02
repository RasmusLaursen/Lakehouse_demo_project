"""Schema converters - Convert ODCS schemas to internal configurations."""

from typing import Any, Dict, List
from open_data_contract_standard.model import SchemaObject

from src.framework.helper.contracts.parser import (
    get_custom_property,
    get_primary_keys,
)


def schema_to_table_config(schema: SchemaObject) -> Dict[str, Any]:
    """Converts an ODCS SchemaObject to a TableConfig-compatible dictionary.

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


def get_scd_config(schema: SchemaObject) -> int:
    """Get SCD type configuration from schema.

    Args:
        schema (SchemaObject): The schema object from ODCS contract.

    Returns:
        int: SCD type (1 or 2)
    """
    return int(get_custom_property(schema, "scd_type", 1))


def get_loadtype(schema: SchemaObject) -> str:
    """Get load type from schema.

    Args:
        schema (SchemaObject): The schema object from ODCS contract.

    Returns:
        str: Load type (full, incremental, etc.)
    """
    return get_custom_property(schema, "loadtype", "incremental")


def validate_data_contract(contract_path: str) -> bool:
    """Validate a data contract file.

    Args:
        contract_path (str): Path to the contract file.

    Returns:
        bool: True if valid, False otherwise.
    """
    try:
        from src.framework.helper.contracts.loader import load_data_contract
        load_data_contract(contract_path)
        return True
    except Exception:
        return False


def schema_properties_to_spark_schema(schema: SchemaObject):
    """Convert ODCS schema properties to Spark schema.

    Args:
        schema (SchemaObject): The schema object from ODCS contract.

    Returns:
        StructType: Spark schema
    """
    from pyspark.sql.types import StructType, StructField, StringType
    
    # Create basic Spark schema from properties
    fields = []
    if schema.properties:
        # schema.properties is a list of property objects with 'name' and 'type' attributes
        for prop in schema.properties:
            # Get property name - handle both object attributes and dict access
            prop_name = getattr(prop, 'name', prop.get('name') if isinstance(prop, dict) else None)
            if prop_name:
                # Default to StringType for simplicity - real implementation would map types
                fields.append(StructField(prop_name, StringType(), True))
    
    return StructType(fields) if fields else None

