# Quick Reference

## At a Glance

| Component | Purpose | Location |
|-----------|---------|----------|
| **CentralizedPipelineConfig** | Shared pipeline metadata | `centralized_config.md` |
| **ConnectorConfig** | Connector-specific dict wrapper | `connector_config.md` |
| **CatalogSchemaManager** | Table path construction | `catalog_schema_manager.md` |
| **SecretResolver** | Secret reference resolution | `secret_resolver.md` |
| **BaseConfigBuilder** | Abstract builder template | `builders/base_config_builder.md` |
| **VolumeConfigBuilder** | Volume connector specifics | `builders/volume_config_builder.md` |
| **RestApiConfigBuilder** | REST API connector specifics | `builders/rest_api_config_builder.md` |
| **ConnectorConfigBuilderFactory** | Builder instantiation | `builders/builder_factory.md` |

## Common Tasks

### Load Configuration

```python
from src.framework.config import (
    CentralizedPipelineConfig,
    ConnectorConfig,
    ConnectorConfigBuilderFactory,
)

# Load from Spark (DLT pipelines)
centralized = CentralizedPipelineConfig.from_spark(spark, "lakehouse")

# Create connector config
connector = ConnectorConfig("rest_api", {"endpoint": "https://..."})

# Build configuration
builder = ConnectorConfigBuilderFactory.create_builder("rest_api", connector, centralized)
config = builder.merge_shared_context().resolve_secrets().build()
```

### Add New Schema Property

1. Add to `SCHEMA_LEVEL_PROPERTIES` in `BaseConfigBuilder`
2. Implement custom merge logic if needed
3. Use in data contract

See: [ADDING_SCHEMA_CONFIG.md](./ADDING_SCHEMA_CONFIG.md)

### Add New Connector Type

1. Create builder extending `BaseConfigBuilder`
2. Implement `merge_shared_context()`
3. Register with factory
4. Write tests

See: [ADDING_NEW_CONNECTOR.md](./ADDING_NEW_CONNECTOR.md)

### Resolve Secrets

```python
from src.framework.config import SecretResolver

resolver = SecretResolver()

# Supports multiple formats
token = resolver.resolve("{{spark.api-token}}")
secret = resolver.resolve("{{secrets/scope/key}}")
proto = resolver.resolve("secret://scope/key")
```

### Build Table Paths

```python
from src.framework.config import CatalogSchemaManager

manager = CatalogSchemaManager.from_pipeline_config(config)

# Layer-specific paths
raw_path = manager.get_raw_table_path("customer")
dim_path = manager.get_dimension_table_path("dim_customer")
fact_path = manager.get_fact_table_path("fact_sales")
```

## Configuration Building Workflow

```
ConnectorConfig("volume", {...})
    ↓
VolumeConfigBuilder
    ├─ merge_shared_context()      # Add catalog, schema
    ├─ merge_schema_overrides()    # Apply schema properties
    ├─ resolve_secrets()            # {{...}} → actual value
    └─ build()                      # Final dict
```

## Secret Formats

| Format | Example | Resolution |
|--------|---------|-----------|
| Spark Config | `{{spark.api-token}}` | Via spark.conf.get() |
| Databricks Secrets | `{{secrets/scope/key}}` | Via dbutils.secrets.get() |
| Secret Protocol | `secret://scope/key` | Via dbutils.secrets.get() |
| Plain Value | `plain_text_123` | Returned as-is |

## Schema-Level Properties

```python
{
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

## Builder Methods

### Common (All Builders)
```python
builder.merge_schema_overrides(schema)  # Apply schema properties
builder.resolve_secrets()                # Resolve {{...}}
builder.build()                          # Final config dict
```

### Connector-Specific
```python
# VolumeConfigBuilder
builder.merge_shared_context()           # Volume context

# RestApiConfigBuilder
builder.merge_shared_context()           # REST API context
builder.build_spark_schema(model, schema)  # Build schema
builder.pre_load_oauth2_token(model)     # OAuth2 pre-load
```

## Factory Methods

```python
# Create builder
builder = ConnectorConfigBuilderFactory.create_builder(
    "rest_api", connector_config, centralized_config
)

# Get registered types
types = ConnectorConfigBuilderFactory.get_registered_types()

# Register custom builder
ConnectorConfigBuilderFactory.register_builder("custom", CustomBuilder)
```

## Testing

```bash
# Run all tests
python -m pytest tests/unit/test_config.py -v

# Run specific test class
python -m pytest tests/unit/test_config.py::TestConnectorConfig -v

# Run with coverage
python -m pytest tests/unit/test_config.py --cov=src.framework.config
```

## Documentation Structure

```
configuration/
├── README.md                              # Overview & structure
├── ARCHITECTURE.md                        # Design & patterns
├── QUICK_REFERENCE.md                     # This file
├── ADDING_SCHEMA_CONFIG.md               # Add schema properties
├── ADDING_NEW_CONNECTOR.md               # Add connectors
├── centralized_config.md                  # CentralizedPipelineConfig
├── connector_config.md                    # ConnectorConfig
├── catalog_schema_manager.md              # CatalogSchemaManager
├── secret_resolver.md                     # SecretResolver
└── builders/
    ├── README.md                          # Builders overview
    ├── base_config_builder.md             # BaseConfigBuilder
    ├── volume_config_builder.md           # VolumeConfigBuilder
    ├── rest_api_config_builder.md         # RestApiConfigBuilder
    ├── jdbc_config_builder.md             # JdbcConfigBuilder
    ├── autoloader_config_builder.md       # AutoLoaderConfigBuilder
    └── builder_factory.md                 # Factory
```

## Error Handling

| Error | Cause | Solution |
|-------|-------|----------|
| `ValueError: Unknown connector type` | Type not registered | Register builder with factory |
| `AttributeError: object has no attribute` | Missing mock property | Add field to mock |
| `ValueError: Endpoint required` | Missing config | Check data contract |
| `ValueError: S3 bucket configured` | Missing required field | Add to CentralizedPipelineConfig |

## Best Practices

✅ **DO**:
- Use factory to create builders
- Chain methods for readability
- Validate configuration early
- Handle exceptions gracefully
- Log for debugging
- Write tests for custom builders

❌ **DON'T**:
- Create builders directly
- Assume values exist
- Skip validation
- Ignore logging
- Add logic outside merge_shared_context()
- Skip tests

## Quick Patterns

### Parse JSON Property
```python
import json
json_value = json.loads(prop_value)
self.config.set("parsed", json_value)
```

### Conditional Merge
```python
if not self.config.get("key"):
    self.config.set("key", value)
```

### Extract from Schema
```python
for prop in schema.customProperties:
    name = getattr(prop, 'property')
    value = getattr(prop, 'value')
```

## See Also

- [Configuration Overview](./README.md)
- [Architecture Guide](./ARCHITECTURE.md)
- [Core Classes Documentation](./README.md)
- [Builders Documentation](./builders/README.md)
