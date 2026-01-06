# BaseConfigBuilder

## Location
`src/framework/config/builders/base_config_builder.py`

## Purpose
Abstract base class implementing the Template Method Pattern for configuration building. Defines the common workflow that all connector-specific builders follow.

## Template Method Pattern

The base class defines the algorithm structure, and subclasses override specific methods.

```python
# The concrete workflow (defined in base class)
def build(self):
    return self.config.to_dict()

# Concrete implementation (common to all)
def merge_schema_overrides(self, schema):
    # Extract and merge schema properties
    pass

def resolve_secrets(self):
    # Resolve all secret references
    pass

# Abstract method (subclasses implement)
def merge_shared_context(self):
    raise NotImplementedError("Subclasses must implement")
```

## Properties

### config
The `ConnectorConfig` instance holding configuration.

```python
@property
def config(self):
    return self._config
```

### pipeline_config
The `CentralizedPipelineConfig` instance providing shared metadata.

```python
@property
def pipeline_config(self):
    return self._pipeline_config
```

### SCHEMA_LEVEL_PROPERTIES
Set of property names that can be overridden at schema level.

```python
SCHEMA_LEVEL_PROPERTIES = {
    "pagination_config",
    "params",
    "secret_keys",
    "mode",
    "table_name",
    "is_root_call",
    "depends_on",
    "workflow_step",
    "method",
    "data_path",
    "custom_timeout",
    "retry_policy",
    # ... more
}
```

## Methods

### `__init__(base_config, pipeline_config)`
Initialize builder with connector and pipeline configurations.

```python
builder = VolumeConfigBuilder(connector_config, centralized_config)
```

### `merge_shared_context()` (Abstract)
Add connector-specific context from centralized configuration.

Must be implemented by subclasses.

```python
def merge_shared_context(self):
    """Subclass implementation example."""
    context = {
        "landing_catalog": self.pipeline_config.landing_catalog,
    }
    context_to_add = {
        k: v for k, v in context.items() 
        if k not in self.config.to_dict()
    }
    if context_to_add:
        self.config = self.config.merge(context_to_add)
    return self
```

### `merge_schema_overrides(schema)`
Merge schema-level property overrides.

```python
builder.merge_schema_overrides(schema)
```

**Behavior**:
- Extracts properties from `schema.customProperties`
- Checks if property name is in `SCHEMA_LEVEL_PROPERTIES`
- Merges matching properties into configuration
- Logs number of properties applied

### `resolve_secrets()`
Resolve secret references (e.g., `{{secrets/scope/key}}`) to actual values.

```python
builder.resolve_secrets()
```

**Behavior**:
- Extracts secret references from configuration
- Creates `SecretResolver` instance
- Resolves each secret
- Updates configuration with resolved values
- Logs resolution progress

### `build()`
Build and return final configuration dictionary.

```python
final_config = builder.build()
```

**Returns**: Dictionary with all configuration items.

## Usage Examples

### Basic Usage (Via Subclass)
```python
from src.framework.config import ConnectorConfigBuilderFactory

# Factory creates appropriate subclass instance
builder = ConnectorConfigBuilderFactory.create_builder(
    "volume",
    connector_config,
    centralized_config
)

# Use the fluent API
config = (builder
    .merge_shared_context()
    .merge_schema_overrides(schema)
    .resolve_secrets()
    .build())
```

### Method Chaining
```python
# Fluent API - methods return self
config = (builder
    .merge_shared_context()
    .merge_schema_overrides(schema)
    .resolve_secrets()
    .build())
```

### Step-by-Step
```python
builder = ConnectorConfigBuilderFactory.create_builder(
    "rest_api",
    connector_config,
    centralized_config
)

# Step 1: Add connector context
builder.merge_shared_context()

# Step 2: Apply schema properties
builder.merge_schema_overrides(schema)

# Step 3: Resolve secrets
builder.resolve_secrets()

# Step 4: Get final config
final_config = builder.build()
```

### With Logging
```python
import logging

logging.basicConfig(level=logging.INFO)

builder = VolumeConfigBuilder(connector_config, centralized_config)

# Logs show each step
config = (builder
    .merge_shared_context()      # INFO: Added 3 volume context items
    .merge_schema_overrides(schema)  # INFO: Applied 2 schema-level overrides
    .resolve_secrets()           # INFO: Resolving 1 secrets
    .build())                    # INFO: Built final config with 5 keys
```

## Creating Custom Builders

To create a new builder for a different connector type:

```python
from src.framework.config.builders.base_config_builder import BaseConfigBuilder

class MyConnectorBuilder(BaseConfigBuilder):
    """Builder for my custom connector."""
    
    def merge_shared_context(self):
        """Add my connector-specific context."""
        context = {
            "my_setting": self.pipeline_config.my_config,
        }
        
        current_config = self.config.to_dict()
        context_to_add = {
            k: v for k, v in context.items() 
            if k not in current_config
        }
        
        if context_to_add:
            self.config = self.config.merge(context_to_add)
        
        return self
```

## Schema Properties Workflow

```
Input: schema object with customProperties
         │
         ├─ Extract customProperties list
         │
         ├─ For each property:
         │   ├─ Check if name in SCHEMA_LEVEL_PROPERTIES
         │   ├─ If yes, extract value
         │   └─ Merge into config
         │
         └─ Log: "Applied N schema-level overrides"
```

## Secret Resolution Workflow

```
Input: config with secret references like {{secrets/scope/key}}
         │
         ├─ Extract secret keys from config
         │   └─ config.extract_secrets()
         │
         ├─ For each secret key:
         │   ├─ Get secret value
         │   ├─ Create SecretResolver
         │   ├─ Resolve the value
         │   └─ Update config
         │
         └─ Log: "Successfully resolved N secrets"
```

## Design Rationale

- **Template Method**: Defines algorithm structure while allowing customization
- **Fluent API**: Enables readable method chaining
- **Common Logic**: Merge schema and resolve secrets in base class
- **Extensible**: Subclasses only implement connector-specific parts
- **Logging**: Provides visibility into configuration building process

## Integration Points

- **ConnectorConfigBuilderFactory**: Creates appropriate subclass instance
- **VolumeConfigBuilder**: Extends for Volume connector
- **RestApiConfigBuilder**: Extends for REST API connector
- **JdbcConfigBuilder**: Extends for JDBC connector
- **AutoLoaderConfigBuilder**: Extends for AutoLoader connector

## Related Classes
- [ConnectorConfig](../connector_config.md)
- [CentralizedPipelineConfig](../centralized_config.md)
- [SecretResolver](../secret_resolver.md)
- [Builder Subclasses](./README.md)

## See Also
- [Builders Overview](./README.md)
- [Configuration Overview](../README.md)
- [Architecture Guide](../ARCHITECTURE.md)
- [Adding New Connectors](../ADDING_NEW_CONNECTOR.md)
