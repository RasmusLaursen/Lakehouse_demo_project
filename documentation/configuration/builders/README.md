# Builders

## Location
`src/framework/config/builders/`

## Overview

The builders implement the **Template Method Pattern** to handle configuration building for different connector types. Each builder extends `BaseConfigBuilder` and implements connector-specific logic.

## Structure

```
builders/
├── __init__.py                          # Exports all builders and factory
├── base_config_builder.py               # Abstract template base class
├── volume_config_builder.py             # Volume connector
├── rest_api_config_builder.py           # REST API connector
├── jdbc_config_builder.py               # JDBC connector
├── autoloader_config_builder.py         # AutoLoader connector
└── builder_factory.py                   # Factory for builder instantiation
```

## Template Method Pattern

The base class defines the overall algorithm, and subclasses implement specific steps.

```
BaseConfigBuilder (Abstract)
│
├─ merge_shared_context()        [ABSTRACT - subclass implements]
├─ merge_schema_overrides()      [CONCRETE - common logic]
├─ resolve_secrets()             [CONCRETE - common logic]
└─ build()                       [CONCRETE - common logic]
```

## Builder Types

### VolumeConfigBuilder
For Databricks Volume connectors.

**Implements**: 
- `merge_shared_context()` - Adds landing catalog, schema, and format

See: [volume_config_builder.md](./volume_config_builder.md)

### RestApiConfigBuilder
For REST API endpoint connectors.

**Implements**:
- `merge_shared_context()` - Adds raw catalog and schema
- `build_spark_schema()` - Builds Spark schema from contract
- `pre_load_oauth2_token()` - Pre-exchanges OAuth2 tokens

See: [rest_api_config_builder.md](./rest_api_config_builder.md)

### JdbcConfigBuilder
For JDBC database connectors.

**Implements**:
- `merge_shared_context()` - Adds host, port, database, credentials

See: [jdbc_config_builder.md](./jdbc_config_builder.md)

### AutoLoaderConfigBuilder
For Databricks AutoLoader connector.

**Implements**:
- `merge_shared_context()` - Adds cloud paths and credentials

See: [autoloader_config_builder.md](./autoloader_config_builder.md)

## Building Configuration

All builders follow the same workflow:

```python
builder = ConnectorConfigBuilderFactory.create_builder(
    connector_type="rest_api",
    base_config=connector_config,
    centralized_config=centralized_config
)

config = (builder
    .merge_shared_context()         # Add connector context
    .merge_schema_overrides(schema) # Apply schema properties
    .resolve_secrets()               # Resolve {{...}} references
    .build())                        # Return final dict
```

## Common Methods

All builders inherit these methods from `BaseConfigBuilder`:

### `merge_schema_overrides(schema)`
Apply schema-level property overrides.

```python
builder.merge_schema_overrides(schema)
```

### `resolve_secrets()`
Resolve all secret references in configuration.

```python
builder.resolve_secrets()
```

### `build()`
Build and return final configuration dictionary.

```python
final_config = builder.build()
```

## Schema-Level Properties

Properties that can be overridden at the schema level:

```python
SCHEMA_LEVEL_PROPERTIES = {
    # REST API properties
    "pagination_config",
    "params",
    "secret_keys",
    "mode",
    "table_name",
    "method",
    "data_path",
    
    # Workflow properties
    "is_root_call",
    "depends_on",
    "workflow_step",
    
    # Generic properties
    "custom_timeout",
    "retry_policy",
}
```

## Factory

`ConnectorConfigBuilderFactory` creates appropriate builder instances.

```python
from src.framework.config import ConnectorConfigBuilderFactory

builder = ConnectorConfigBuilderFactory.create_builder(
    "rest_api",
    connector_config,
    centralized_config
)
```

See: [builder_factory.md](./builder_factory.md)

## Usage Example

```python
from src.framework.config import (
    CentralizedPipelineConfig,
    ConnectorConfig,
    ConnectorConfigBuilderFactory,
)

# Create configs
centralized = CentralizedPipelineConfig(
    landing_catalog="landing",
    raw_catalog="raw",
    # ... more properties
)

connector = ConnectorConfig("rest_api", {
    "endpoint": "https://api.example.com",
    "auth_type": "bearer",
    "auth_token": "{{secrets/scope/token}}",
})

# Create builder via factory
builder = ConnectorConfigBuilderFactory.create_builder(
    "rest_api",
    connector,
    centralized
)

# Build configuration
config = (builder
    .merge_shared_context()
    .merge_schema_overrides(schema)
    .resolve_secrets()
    .build())

print(config)
# Output:
# {
#     'endpoint': 'https://...',
#     'auth_type': 'bearer',
#     'auth_token': 'actual_token_value',
#     'raw_catalog': 'raw',
#     'raw_schema': 'raw',
# }
```

## Extending with Custom Builders

To add support for a new connector type:

1. Create builder class extending `BaseConfigBuilder`
2. Implement `merge_shared_context()`
3. Register with factory
4. Write tests

See: [ADDING_NEW_CONNECTOR.md](../ADDING_NEW_CONNECTOR.md)

## Design Patterns

### Template Method Pattern
Base class defines the steps, subclasses implement specific logic.

### Fluent API
Methods return `self` for easy chaining.

```python
builder.step1().step2().step3().build()
```

## Related Documentation

- [BaseConfigBuilder](./base_config_builder.md)
- [VolumeConfigBuilder](./volume_config_builder.md)
- [RestApiConfigBuilder](./rest_api_config_builder.md)
- [JdbcConfigBuilder](./jdbc_config_builder.md)
- [AutoLoaderConfigBuilder](./autoloader_config_builder.md)
- [ConnectorConfigBuilderFactory](./builder_factory.md)

## See Also
- [Configuration Overview](../README.md)
- [Architecture Guide](../ARCHITECTURE.md)
- [Adding New Connectors](../ADDING_NEW_CONNECTOR.md)
- [Adding Schema Properties](../ADDING_SCHEMA_CONFIG.md)
