# AutoLoaderConfigBuilder

## Location
`src/framework/config/builders/autoloader_config_builder.py`

## Purpose
Builder for Databricks AutoLoader connector configuration. Provides support for AutoLoader file ingestion patterns.

## Extends
`BaseConfigBuilder`

## Status
✅ Implemented and active for production use

## Methods

### `__init__(base_config, centralized_config, model_name=None)`
Initialize AutoLoaderConfigBuilder with optional model name for per-schema volume mapping.

```python
builder = AutoLoaderConfigBuilder(
    base_config,
    centralized_config,
    model_name="customer"  # Optional: triggers per-schema volume mapping
)
```

**Parameters**:
- `base_config`: Base ConnectorConfig from server configuration
- `centralized_config`: CentralizedPipelineConfig with shared metadata
- `model_name`: Optional model/schema name for per-schema volume mapping

**Note**: `model_name` triggers per-schema volume logic in `merge_shared_context()`. When provided, volume is set to the model name instead of source system name.

### `merge_shared_context()`
Add AutoLoader-specific context from centralized configuration.

```python
builder.merge_shared_context()
```

**What It Does**:
1. Sets landing layer catalog/schema from centralized config
2. Sets file format (parquet, delta, etc.)
3. Sets volume name based on `model_name` parameter:
   - If `model_name` provided: volume = model_name (per-schema mapping)
   - If `model_name` is None: volume = source_system_name (fallback)

**Configuration Added**:
```python
# With model_name="customer" (per-schema):
{
    "catalog": "landing_dev",                   # e.g., "landing_dev"
    "schema": "dev_rasmuslaursen_lakehouse",   # e.g., "dev_rasmuslaursen_lakehouse"
    "format": "parquet",                        # e.g., "parquet"
    "volume": "customer",                       # model_name used
}

# Without model_name (source system default):
{
    "catalog": "landing_dev",                   # e.g., "landing_dev"
    "schema": "dev_rasmuslaursen_lakehouse",   # e.g., "dev_rasmuslaursen_lakehouse"
    "format": "parquet",                        # e.g., "parquet"
    "volume": "lakehouse",                      # source_system_name used
}
```

**Returns**: Self for method chaining

## Actual Usage

### Basic AutoLoader Configuration (without per-schema mapping)
```python
from src.framework.config import (
    ConnectorConfig,
    CentralizedPipelineConfig,
    AutoLoaderConfigBuilder,
)

# Create base config
connector_config = ConnectorConfig(
    connector_type="autoloader",
    config={
        "loadtype": "volume_autoloader",
        "source_type": "volume",
        "add_audit_columns": True,
    }
)

# Create builder WITHOUT model_name (uses source_system_name)
builder = AutoLoaderConfigBuilder(
    connector_config,
    CentralizedPipelineConfig.from_spark(spark, "lakehouse")
)

# Build config - volume will be "lakehouse" (source_system_name)
config = (builder
    .merge_shared_context()
    .build())
```

### Per-Schema Volume Mapping (with model_name)
```python
# Create builder WITH model_name for per-schema volume mapping
builder = AutoLoaderConfigBuilder(
    connector_config,
    CentralizedPipelineConfig.from_spark(spark, "lakehouse"),
    model_name="customer"  # Per-schema mapping
)

# Build config - volume will be "customer" (model_name)
config = (builder
    .merge_shared_context()
    .build())
```

### Via Factory (Recommended)
```python
from src.framework.config import ConnectorConfigBuilderFactory

# Factory handles model_name parameter automatically
builder = ConnectorConfigBuilderFactory.create_builder(
    "autoloader",
    connector_config,
    centralized_config,
    model_name="customer"  # Passed to AutoLoaderConfigBuilder
)

# merge_shared_context will use model_name for volume mapping
config = builder.merge_shared_context().build()
```

## Data Flow

### For Volume-Based AutoLoader with Per-Schema Mapping

```
1. Server config defines:
   - connector_type: "autoloader"
   - source_type: "volume"

2. RawFactory passes model_name to builder:
   - model_name="customer" (the schema being processed)

3. merge_shared_context() with model_name sets:
   - catalog: "landing_dev"
   - schema: "dev_rasmuslaursen_lakehouse"
   - volume: "customer"   (model_name used instead of source_system_name)

4. Final config for customer schema:
   - catalog: "landing_dev"
   - schema: "dev_rasmuslaursen_lakehouse"
   - volume: "customer"
   - path: /Volumes/landing_dev/dev_rasmuslaursen_lakehouse/customer/

5. For next schema (e.g., seller):
   - model_name="seller"
   - volume is set to "seller"
   - path: /Volumes/landing_dev/dev_rasmuslaursen_lakehouse/seller/
```

This per-schema volume mapping ensures each table reads from the correct volume.

## Configuration Added

The AutoLoaderConfigBuilder automatically sets these fields when `merge_shared_context()` is called:

```python
{
    "catalog": "landing_dev",
    "schema": "dev_rasmuslaursen_lakehouse",
    "format": "parquet",
    "volume": "customer",  # Set based on model_name parameter
}

## Example Data Contract

```yaml
kind: DataContract
apiVersion: v3.0.0

servers:
  - server: dev
    type: "databricks"
    environment: "development"
    format: "parquet"
    customProperties:
      - property: loadtype
        value: "volume_autoloader"
      - property: connector_type
        value: "autoloader"
      - property: connector_config
        value:
          source_type: "volume"
          add_audit_columns: true

schema:
  - name: customer
    description: "Customer data"
    type: table
    customProperties:
      - property: mode
        value: streaming
      - property: scd_type
        value: 1
      - property: keys
        value: ["customer_id"]
      - property: loadtype
        value: "volume_autoloader"
    properties:
      - name: customer_id
        type: integer
        required: true
      - name: name
        type: string
```

## Related Documentation
- [BaseConfigBuilder](./base_config_builder.md)
- [ConnectorConfigBuilderFactory](./builder_factory.md)
- [RawFactory](../factory/raw_factory.md)

````    "source_path": self.pipeline_config.autoloader_path,
    "checkpoint_path": self.pipeline_config.autoloader_checkpoint,
    "schema_mode": self.pipeline_config.autoloader_schema_mode,
    "cloud_provider": self.pipeline_config.cloud_provider,
}
```

### Expected Configuration Properties
- `source_path`: Path to source files (s3://, abfss://, etc.)
- `checkpoint_path`: Checkpoint directory for AutoLoader
- `schema_mode`: "addNewColumns", "rescue", "failOnNewColumns"
- `source_format`: "cloudFiles"
- `format`: File format (parquet, csv, json, etc.)
- `cloud_provider`: AWS, Azure, GCP
- `credentials`: Cloud provider credentials (as secrets)

## Supported Cloud Providers (Planned)

- Amazon S3
- Azure Blob Storage / ADLS
- Google Cloud Storage

## AutoLoader Specific Features (Planned)

- Schema detection and evolution
- Checkpoint management
- Fault tolerance
- Incremental processing
- File notification queue integration

## Integration Points

- **ConnectorConfigBuilderFactory**: Creates this builder for "autoloader" type
- **BaseConfigBuilder**: Inherits common methods
- **CentralizedPipelineConfig**: Source of AutoLoader configuration (when added)

## Notes

- This builder is currently a placeholder
- Implementation will follow same pattern as VolumeConfigBuilder and RestApiConfigBuilder
- Cloud credentials should be stored as secrets and resolved via SecretResolver
- Checkpoint paths should be in Volumes or Unity Catalog
- Same fluent API pattern as other builders

## Related Classes
- [BaseConfigBuilder](./base_config_builder.md)
- [VolumeConfigBuilder](./volume_config_builder.md)
- [RestApiConfigBuilder](./rest_api_config_builder.md)
- [ConnectorConfig](../connector_config.md)
- [CentralizedPipelineConfig](../centralized_config.md)

## See Also
- [Builders Overview](./README.md)
- [Configuration Overview](../README.md)
- [Adding New Connectors](../ADDING_NEW_CONNECTOR.md)
- [Databricks AutoLoader Documentation](https://docs.databricks.com/ingestion/cloud-object-storage/index.html)
