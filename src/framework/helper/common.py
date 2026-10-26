"""DEPRECATED: Backward compatibility module re-exporting from new submodules.

This module is deprecated and kept only for backward compatibility.
All functionality has been migrated to organized submodules:

New Locations:
  - Core config utils: src.framework.helper.core.config_utils
  - Utilities: src.framework.helper.utils.general
  - Contracts: src.framework.helper.contracts.resolver
  - DataFrame utilities: src.framework.helper.dataframe

Migration Guide:
  OLD: from src.framework.helper.common import find_data_contract_path
  NEW: from src.framework.helper.contracts import find_data_contract_path
  
  OLD: from src.framework.helper.common import parse_arguments
  NEW: from src.framework.helper.utils import parse_arguments
  
  OLD: from src.framework.helper.common import get_data_configuration
  NEW: from src.framework.helper.core.config_utils import get_data_configuration
  
  OLD: from src.framework.helper.common import add_audit_columns
  NEW: from src.framework.helper.dataframe.audit import add_audit_columns
"""
import warnings

warnings.warn(
    "common module is deprecated. Import from specific submodules instead: "
    "contracts, utils, core.config_utils, or dataframe.audit",
    DeprecationWarning,
    stacklevel=2
)

# Re-export from new locations for backward compatibility
from src.framework.helper.contracts.resolver import find_data_contract_path
from src.framework.helper.core.config_utils import (
    get_path_for_data_configuration,
    get_validate_data_configuration_contract,
    get_data_configuration,
    try_load_ingest_config,
)
from src.framework.helper.utils.general import (
    parse_arguments,
    list_yml_files,
    list_volumes_in_schema,
    list_tables_in_schema,
)
from src.framework.helper.dataframe.audit import add_audit_columns

__all__ = [
    # Deprecated re-exports (use new modules directly)
    "find_data_contract_path",
    "get_path_for_data_configuration",
    "get_validate_data_configuration_contract",
    "get_data_configuration",
    "try_load_ingest_config",
    "add_audit_columns",
    "parse_arguments",
    "list_yml_files",
    "list_volumes_in_schema",
    "list_tables_in_schema",
]
