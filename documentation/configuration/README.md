# Configuration System

## Overview

The configuration system manages multi-layered, type-safe configuration for the Lakehouse pipeline with support for multiple connector types. This directory contains all configuration-related classes and builders.

## Structure

```
src/framework/config/
├── __init__.py                          # Main exports
├── centralized_config.py                # CentralizedPipelineConfig class
├── connector_config.py                  # ConnectorConfig wrapper class
├── catalog_schema_manager.py            # CatalogSchemaManager class
├── secret_resolver.py                   # SecretResolver class
└── builders/                            # Builder pattern implementations
    ├── __init__.py
    ├── base_config_builder.py           # Abstract template base
    ├── volume_config_builder.py         # Volume connector
    ├── rest_api_config_builder.py       # REST API connector
    ├── jdbc_config_builder.py           # JDBC connector
    ├── autoloader_config_builder.py     # AutoLoader connector
    └── builder_factory.py               # Factory for builder instantiation
```

## Core Components

### 1. **CentralizedPipelineConfig** (`centralized_config.py`)
Manages shared metadata across all pipeline layers.

- **Catalogs**: landing, raw, base, curated, enriched
- **Schemas**: organized by layer (landing, raw, base, dimensions, facts, enriched)
- **Environment**: dev, test, prod
- **Source System**: identifies which system is being processed
- **Defaults**: filetype, loadtype

See: [centralized_config.md](./centralized_config.md)

### 2. **ConnectorConfig** (`connector_config.py`)
Wraps and manages connector-specific configuration dictionary.

- **Connector Type**: identifies the connector (volume, rest_api, jdbc, etc.)
- **Config Dict**: stores connector-specific parameters
- **Methods**: get, set, merge, extract_secrets, to_dict, from_server_config

See: [connector_config.md](./connector_config.md)

### 3. **CatalogSchemaManager** (`catalog_schema_manager.py`)
Constructs fully qualified table paths for all layers.

- **Base Method**: `get_table_path(catalog, schema, table)`
- **Layer-Specific**: get_raw_table_path, get_base_table_path, get_dimension_table_path, get_fact_table_path
- **Path Construction**: ensures consistent catalog.schema.table format

See: [catalog_schema_manager.md](./catalog_schema_manager.md)

### 4. **SecretResolver** (`secret_resolver.py`)
Resolves secret references to actual values.

- **Formats**: Spark config, Databricks secrets, protocol format
- **Integration**: Databricks secret management
- **Fail-Fast**: raises errors on unresolvable secrets

See: [secret_resolver.md](./secret_resolver.md)

## Builder Pattern

Implements the Template Method Pattern for configuration building.

### Base Class
- **BaseConfigBuilder** (`builders/base_config_builder.py`): Abstract base with template methods

### Connector-Specific Builders
- **VolumeConfigBuilder** (`builders/volume_config_builder.py`): Databricks Volumes
- **RestApiConfigBuilder** (`builders/rest_api_config_builder.py`): REST API endpoints
- **JdbcConfigBuilder** (`builders/jdbc_config_builder.py`): JDBC databases
- **AutoLoaderConfigBuilder** (`builders/autoloader_config_builder.py`): Autoloader connector

### Factory
- **ConnectorConfigBuilderFactory** (`builders/builder_factory.py`): Creates appropriate builder

See: [builders/README.md](./builders/README.md)

## Configuration Building Workflow

```
Data Contract (YAML)
        ↓
ConnectorConfig.from_server_config()
        ↓
ConnectorConfigBuilderFactory.create_builder()
        ↓
builder.merge_schema_overrides()
        ↓
builder.merge_shared_context()
        ↓
builder.resolve_secrets()
        ↓
builder.build()
        ↓
Final Config Dict (ready for connector)
```

## Design Patterns

- **Template Method Pattern**: BaseConfigBuilder defines common steps, subclasses implement connector-specific logic
- **Factory Pattern**: ConnectorConfigBuilderFactory creates appropriate builder based on type
- **Strategy Pattern**: Different merge_shared_context() implementations per connector

## Usage Example

```python
from src.framework.config import (
    CentralizedPipelineConfig,
    ConnectorConfig,
    ConnectorConfigBuilderFactory,
)

# Load centralized config
centralized_config = CentralizedPipelineConfig.from_spark(spark, "lakehouse")

# Create connector config
connector_config = ConnectorConfig("rest_api", {
    "endpoint": "https://api.example.com",
    "auth_type": "bearer",
})

# Build final configuration
builder = ConnectorConfigBuilderFactory.create_builder(
    "rest_api",
    connector_config,
    centralized_config
)

final_config = (builder
    .merge_shared_context()
    .merge_schema_overrides(schema)
    .resolve_secrets()
    .build())
```

## Documentation Files

### Core Classes
- [CentralizedPipelineConfig](./centralized_config.md)
- [ConnectorConfig](./connector_config.md)
- [CatalogSchemaManager](./catalog_schema_manager.md)
- [SecretResolver](./secret_resolver.md)

### Builders
- [Builders Overview](./builders/README.md)
- [BaseConfigBuilder](./builders/base_config_builder.md)
- [VolumeConfigBuilder](./builders/volume_config_builder.md)
- [RestApiConfigBuilder](./builders/rest_api_config_builder.md)
- [JdbcConfigBuilder](./builders/jdbc_config_builder.md)
- [AutoLoaderConfigBuilder](./builders/autoloader_config_builder.md)
- [ConnectorConfigBuilderFactory](./builders/builder_factory.md)

### Guides
- [Architecture Overview](./ARCHITECTURE.md)
- [Adding New Schema Properties](./ADDING_SCHEMA_CONFIG.md)
- [Adding New Connectors](./ADDING_NEW_CONNECTOR.md)

### Reference
- [Quick Reference](./QUICK_REFERENCE.md)

## Extension Points

### Adding New Schema Property
1. Add property name to `SCHEMA_LEVEL_PROPERTIES` in BaseConfigBuilder
2. Implement custom merge logic if needed
3. Write tests

See: [ADDING_SCHEMA_CONFIG.md](./ADDING_SCHEMA_CONFIG.md)

### Adding New Connector Type
1. Create builder class extending BaseConfigBuilder
2. Implement `merge_shared_context()`
3. Register with ConnectorConfigBuilderFactory
4. Add properties to CentralizedPipelineConfig
5. Write tests

See: [ADDING_NEW_CONNECTOR.md](./ADDING_NEW_CONNECTOR.md)

## Testing

See: `tests/unit/test_config.py` (45 comprehensive tests)

```bash
# Run all tests
python -m pytest tests/unit/test_config.py -v

# Run specific test class
python -m pytest tests/unit/test_config.py::TestConnectorConfig -v

# Run with coverage
python -m pytest tests/unit/test_config.py --cov=src.framework.config
```

## See Also

- [Configuration Documentation Index](../CONFIGURATION_DOCUMENTATION_INDEX.md)
- [Quick Reference](./QUICK_REFERENCE.md)
- [Architecture Overview](./ARCHITECTURE.md)
