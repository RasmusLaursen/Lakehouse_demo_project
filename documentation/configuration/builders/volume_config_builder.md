# VolumeConfigBuilder

## Location
`src/framework/config/builders/volume_config_builder.py`

## Purpose
Builder for Databricks Volume connector configuration. Adds volume-specific metadata from the centralized configuration.

## Extends
`BaseConfigBuilder`

## Responsibilities
- Add landing catalog to configuration
- Add landing schema to configuration
- Add file format to configuration
- Merge only if not already configured

## Methods

### `merge_shared_context()`
Add Volume-specific context from centralized configuration.

```python
builder.merge_shared_context()
```

**Adds**:
- `catalog`: landing_catalog
- `schema`: landing_schema
- `format`: filetype

**Behavior**:
- Extracts values from centralized configuration
- Only adds if not already in connector configuration
- Logs number of items added
- Returns self for method chaining

**Example**:
```python
builder = VolumeConfigBuilder(
    ConnectorConfig("volume", {"path": "/Volumes/..."}),
    CentralizedPipelineConfig(
        landing_catalog="landing",
        landing_schema="landing",
        filetype="parquet"
    )
)

builder.merge_shared_context()

# Configuration now includes:
# - path (from connector)
# - catalog: "landing"
# - schema: "landing"
# - format: "parquet"
```

## Usage Examples

### Basic Usage
```python
from src.framework.config import (
    ConnectorConfig,
    CentralizedPipelineConfig,
    VolumeConfigBuilder,
)

# Create configurations
centralized = CentralizedPipelineConfig(
    landing_catalog="landing",
    landing_schema="landing",
    filetype="parquet",
)

connector = ConnectorConfig("volume", {
    "path": "/Volumes/landing/data"
})

# Create builder
builder = VolumeConfigBuilder(connector, centralized)

# Build configuration
config = (builder
    .merge_shared_context()
    .merge_schema_overrides(schema)
    .resolve_secrets()
    .build())

print(config)
# {
#     'path': '/Volumes/landing/data',
#     'catalog': 'landing',
#     'schema': 'landing',
#     'format': 'parquet'
# }
```

### Via Factory
```python
from src.framework.config import ConnectorConfigBuilderFactory

builder = ConnectorConfigBuilderFactory.create_builder(
    "volume",
    connector_config,
    centralized_config
)

config = (builder
    .merge_shared_context()
    .merge_schema_overrides(schema)
    .resolve_secrets()
    .build())
```

### Respecting Existing Configuration
```python
# If connector already has catalog configured,
# it won't be overwritten
connector = ConnectorConfig("volume", {
    "path": "/Volumes/...",
    "catalog": "custom_catalog"  # Already set
})

builder = VolumeConfigBuilder(connector, centralized)
builder.merge_shared_context()

# Will NOT overwrite custom_catalog
assert builder.config.get("catalog") == "custom_catalog"

# But will add schema and format
assert builder.config.get("schema") == "landing"
assert builder.config.get("format") == "parquet"
```

## Workflow

```
VolumeConfigBuilder.merge_shared_context()
    ├─ Get landing_catalog from centralized
    ├─ Get landing_schema from centralized
    ├─ Get filetype from centralized
    │
    ├─ Build context dict
    │
    ├─ Check current config for each key
    │   └─ Only add if not already present
    │
    ├─ Merge context if anything to add
    │   └─ Log: "Added X volume context items"
    │
    └─ Return self
```

## Design Rationale

- **Minimal Context**: Adds only volume-specific settings
- **Respects Existing**: Doesn't overwrite already-configured values
- **Logging**: Provides visibility into what was added
- **Chainable**: Returns self for fluent API

## Integration Points

- **ConnectorConfigBuilderFactory**: Creates this builder for "volume" type
- **BaseConfigBuilder**: Inherits merge_schema_overrides, resolve_secrets, build
- **CentralizedPipelineConfig**: Source of landing catalog and schema

## Related Classes
- [BaseConfigBuilder](./base_config_builder.md)
- [ConnectorConfig](../connector_config.md)
- [CentralizedPipelineConfig](../centralized_config.md)

## See Also
- [Builders Overview](./README.md)
- [Configuration Overview](../README.md)
