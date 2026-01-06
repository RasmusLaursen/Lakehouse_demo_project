# ConnectorConfigBuilderFactory

## Location
`src/framework/config/builders/builder_factory.py`

## Purpose
Factory for creating appropriate builder instances based on connector type. Implements the Factory Pattern.

## Factory Pattern

Maps connector types to builder classes and instantiates the correct builder.

```
Connector Type: "rest_api"
        ↓
Factory.create_builder("rest_api", ...)
        ↓
Looks up BUILDER_MAPPING
        ↓
Finds: RestApiConfigBuilder
        ↓
Instantiates and returns builder
```

## Properties

### BUILDER_MAPPING
Maps connector types to builder classes.

```python
BUILDER_MAPPING = {
    "volume": VolumeConfigBuilder,
    "autoloader": AutoLoaderConfigBuilder,
    "rest_api": RestApiConfigBuilder,
    "rest_api_ds": RestApiConfigBuilder,
    "rest_api_workflow_ds": RestApiConfigBuilder,
    "jdbc": JdbcConfigBuilder,
}
```

## Methods

### `create_builder(connector_type, base_config, centralized_config, model_name=None)`
Create appropriate builder instance for given connector type.

```python
builder = ConnectorConfigBuilderFactory.create_builder(
    "rest_api",
    connector_config,
    centralized_config
)

# With model_name (used by AutoLoader for per-schema volume mapping)
builder = ConnectorConfigBuilderFactory.create_builder(
    "autoloader",
    connector_config,
    centralized_config,
    model_name="customer"  # Enables per-schema volume mapping
)
```

**Parameters**:
- `connector_type`: Type of connector (string)
- `base_config`: ConnectorConfig instance
- `centralized_config`: CentralizedPipelineConfig instance
- `model_name`: Optional model/schema name (used by AutoLoader for per-schema configuration)

**Returns**: Appropriate builder instance

**Raises**: `ValueError` if connector type not registered

**Behavior**:
- Looks up connector_type in BUILDER_MAPPING
- For AutoLoader: passes model_name to builder for per-schema volume mapping
- For other builders: model_name is ignored
- Instantiates matching builder
- Logs builder creation
- Returns builder instance

**Special Cases**:
- **AutoLoader with model_name**: Enables per-schema volume mapping (volume set to model_name)
- **AutoLoader without model_name**: Uses source_system_name for volume (fallback)
- **Other connectors**: model_name parameter ignored

### `register_builder(connector_type, builder_class)`
Register custom builder at runtime.

```python
ConnectorConfigBuilderFactory.register_builder(
    "myconnector",
    MyConnectorBuilder
)
```

**Parameters**:
- `connector_type`: Type name (string)
- `builder_class`: Builder class extending BaseConfigBuilder

**Behavior**:
- Adds to BUILDER_MAPPING
- Allows dynamic builder registration
- Logs registration
- Future calls to create_builder("myconnector") will use this class

### `get_registered_types()`
Get list of all registered connector types.

```python
types = ConnectorConfigBuilderFactory.get_registered_types()
# Output: ['volume', 'autoloader', 'rest_api', 'jdbc', ...]
```

**Returns**: List of registered connector type strings

## Usage Examples

### Basic Usage
```python
from src.framework.config import (
    ConnectorConfig,
    CentralizedPipelineConfig,
    ConnectorConfigBuilderFactory,
)

# Create configurations
centralized = CentralizedPipelineConfig(...)
connector = ConnectorConfig("volume", {...})

# Use factory to create builder
builder = ConnectorConfigBuilderFactory.create_builder(
    "volume",
    connector,
    centralized
)

# Build configuration
config = builder.merge_shared_context().build()
```

### Different Connector Types
```python
# Volume
builder = ConnectorConfigBuilderFactory.create_builder(
    "volume",
    volume_config,
    centralized
)

# REST API
builder = ConnectorConfigBuilderFactory.create_builder(
    "rest_api",
    rest_api_config,
    centralized
)

# REST API Workflow
builder = ConnectorConfigBuilderFactory.create_builder(
    "rest_api_workflow_ds",
    rest_api_config,
    centralized
)

# JDBC
builder = ConnectorConfigBuilderFactory.create_builder(
    "jdbc",
    jdbc_config,
    centralized
)
```

### Checking Available Types
```python
# Get all supported types
types = ConnectorConfigBuilderFactory.get_registered_types()
print(types)
# Output: ['volume', 'autoloader', 'rest_api', 'rest_api_ds', 'rest_api_workflow_ds', 'jdbc']

# Check if type is supported
if "s3" in types:
    print("S3 is supported")
else:
    print("S3 not yet supported")
```

### Registering Custom Builder
```python
from src.framework.config.builders.base_config_builder import BaseConfigBuilder

class S3ConfigBuilder(BaseConfigBuilder):
    def merge_shared_context(self):
        # Custom S3 implementation
        pass

# Register custom builder
ConnectorConfigBuilderFactory.register_builder("s3", S3ConfigBuilder)

# Now can create S3 builders
builder = ConnectorConfigBuilderFactory.create_builder(
    "s3",
    s3_config,
    centralized
)
```

### Error Handling
```python
try:
    builder = ConnectorConfigBuilderFactory.create_builder(
        "unknown_type",
        config,
        centralized
    )
except ValueError as e:
    print(f"Unknown connector type: {e}")
```

## Supported Connector Types

| Type | Builder | Purpose |
|------|---------|---------|
| `volume` | VolumeConfigBuilder | Databricks Volumes |
| `autoloader` | AutoLoaderConfigBuilder | Databricks AutoLoader |
| `rest_api` | RestApiConfigBuilder | REST API endpoints |
| `rest_api_ds` | RestApiConfigBuilder | REST API datasource |
| `rest_api_workflow_ds` | RestApiConfigBuilder | REST API workflow datasource |
| `jdbc` | JdbcConfigBuilder | JDBC databases |

## Adding New Connector Support

### Step 1: Create Builder Class
```python
from src.framework.config.builders.base_config_builder import BaseConfigBuilder

class MyConnectorBuilder(BaseConfigBuilder):
    def merge_shared_context(self):
        # Implementation
        pass
```

### Step 2: Register with Factory
```python
ConnectorConfigBuilderFactory.register_builder(
    "myconnector",
    MyConnectorBuilder
)
```

### Step 3: Use
```python
builder = ConnectorConfigBuilderFactory.create_builder(
    "myconnector",
    config,
    centralized
)
```

## Workflow

```
create_builder("rest_api", config, centralized)
    ├─ Look up "rest_api" in BUILDER_MAPPING
    ├─ Find RestApiConfigBuilder
    ├─ Log: "Creating RestApiConfigBuilder for rest_api"
    ├─ Instantiate: RestApiConfigBuilder(config, centralized)
    └─ Return builder instance
```

## Design Rationale

- **Factory Pattern**: Centralizes builder instantiation
- **Extensible**: New builders can be registered at runtime
- **Type-Safe**: Fails fast with clear error on unknown type
- **Discoverable**: Can list all supported types
- **Flexible**: Supports multiple variants of same builder (rest_api, rest_api_ds, rest_api_workflow_ds)

## Integration Points

- **BaseConfigBuilder**: Base class for all builders
- **All Builder Subclasses**: Registered and instantiated by factory
- **ConnectorConfig**: Passed to builders
- **CentralizedPipelineConfig**: Passed to builders

## Related Classes
- [BaseConfigBuilder](./base_config_builder.md)
- [VolumeConfigBuilder](./volume_config_builder.md)
- [RestApiConfigBuilder](./rest_api_config_builder.md)
- [JdbcConfigBuilder](./jdbc_config_builder.md)
- [AutoLoaderConfigBuilder](./autoloader_config_builder.md)

## See Also
- [Builders Overview](./README.md)
- [Configuration Overview](../README.md)
- [Adding New Connectors](../ADDING_NEW_CONNECTOR.md)
