"""Framework helper utilities - organized by concern."""

# ============================================================================
# New organized helper API
# ============================================================================

# Core utilities
from src.framework.helper.core import (
    get_logger,
    get_spark,
    get_dbutils,
    get_pipeline_configurations,
    get_pipeline_configurations_from_spark,
)

# Backward compatibility - allow direct module imports
from src.framework.helper import databricks_helper
from src.framework.helper import logging_helper

# Configuration models
from src.framework.helper.config import (
    TableConfig,
    LayerConfig,
    DefaultTblProperties,
    InternalAuditColumns,
)

# Data contracts
from src.framework.helper.contracts import (
    find_data_contract_path,
    load_data_contract,
    load_yaml_file,
    get_data_contract,
    get_data_contract_path,
    get_schema_from_contract,
    get_custom_property,
    get_all_schemas,
    get_schema_names,
    get_primary_keys,
    schema_to_table_config,
    get_scd_config,
    get_loadtype,
    validate_data_contract,
    schema_properties_to_spark_schema,
)

# DataFrame operations
from src.framework.helper.dataframe import (
    read_stream_table,
    read_table,
    read_dataframe,
    write_volume,
    add_audit_columns,
)

# Pipeline (DLT) utilities
from src.framework.helper.pipeline import (
    ldp_table,
    ldp_view,
    ldp_create_streaming_table,
    ldp_change_data_capture,
)

# Quality utilities (DQX)
from src.framework.helper.quality import (
    get_ws_client,
    get_dq_engine,
    get_dqx_generator,
    get_data_quality_configuration,
)

# Catalog introspection utilities
from src.framework.helper.catalog import (
    dimension_keys_lookup,
    get_table_properties,
)

# Synthetic data generation
from src.framework.helper.synthetic import (
    DynamicFakeDataGenerator,
    Lakehouses,
    Regions,
    LoyaltyTier,
    PaymentMethod,
    CustomerProfile,
    LakehouseProfile,
    SellerProfile,
    LakehouseRental,
)

# Configuration and utilities
from src.framework.helper.core.config_utils import (
    get_path_for_data_configuration,
    get_validate_data_configuration_contract,
    try_load_ingest_config,
    get_data_configuration,
)

# General utilities
from src.framework.helper.utils import (
    parse_arguments,
    list_yml_files,
    list_volumes_in_schema,
    list_tables_in_schema,
)

__all__ = [
    # Core utilities
    "get_logger",
    "get_spark",
    "get_dbutils",
    "get_pipeline_configurations",
    "get_pipeline_configurations_from_spark",
    # Backward compatibility modules
    "databricks_helper",
    "logging_helper",
    # Configuration models
    "TableConfig",
    "LayerConfig",
    "DefaultTblProperties",
    "InternalAuditColumns",
    # Data contracts
    "find_data_contract_path",
    "load_data_contract",
    "load_yaml_file",
    "get_data_contract",
    "get_data_contract_path",
    "get_schema_from_contract",
    "get_custom_property",
    "get_all_schemas",
    "get_schema_names",
    "get_primary_keys",
    "schema_to_table_config",
    "get_scd_config",
    "get_loadtype",
    "validate_data_contract",
    "schema_properties_to_spark_schema",
    # DataFrame operations
    "read_stream_table",
    "read_table",
    "read_dataframe",
    "write_volume",
    "add_audit_columns",
    # Pipeline utilities
    "ldp_table",
    "ldp_view",
    "ldp_create_streaming_table",
    "ldp_change_data_capture",
    # Quality utilities
    "get_ws_client",
    "get_dq_engine",
    "get_dqx_generator",
    "get_data_quality_configuration",
    # Catalog utilities
    "dimension_keys_lookup",
    "get_table_properties",
    # Synthetic data
    "DynamicFakeDataGenerator",
    "Lakehouses",
    "Regions",
    "LoyaltyTier",
    "PaymentMethod",
    "CustomerProfile",
    "LakehouseProfile",
    "SellerProfile",
    "LakehouseRental",
    # Configuration utilities
    "get_path_for_data_configuration",
    "get_validate_data_configuration_contract",
    "try_load_ingest_config",
    "get_data_configuration",
    # General utilities
    "parse_arguments",
    "list_yml_files",
    "list_volumes_in_schema",
    "list_tables_in_schema",
]
