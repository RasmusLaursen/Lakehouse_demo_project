# CentralizedPipelineConfig

## Location
`src/framework/config/centralized_config.py`

## Purpose
Manages pipeline-wide shared metadata that is reused across all layers and connectors.

## Responsibilities
- Store pipeline-wide catalogs and schemas
- Manage environment-specific settings (dev, test, prod)
- Provide source system context
- Validate required configuration

## Properties

### Catalogs (Unity Catalog)
```python
landing_catalog: str      # Landing zone catalog
raw_catalog: str          # Raw zone catalog
base_catalog: str         # Base zone catalog
curated_catalog: str      # Curated zone catalog
enriched_catalog: str     # Enriched zone catalog
```

### Schemas (Organized by Layer)
```python
landing_schema: str       # Landing layer schema
raw_schema: str           # Raw layer schema
base_schema: str          # Base layer schema
dimensions_schema: str    # Dimensions (curated)
facts_schema: str         # Facts (curated)
enriched_schema: str      # Enriched zone schema
```

### Metadata
```python
source_system_name: str   # "lakehouse", "review", etc.
environment: str          # "dev", "test", "prod"
```

### Defaults
```python
filetype: str = "parquet"
loadtype: str = "volume_autoloader"
```

## Methods

### `__init__(**kwargs)`
Initialize configuration with keyword arguments.

```python
config = CentralizedPipelineConfig(
    source_system_name="lakehouse",
    environment="dev",
    landing_catalog="landing",
    raw_catalog="raw",
    # ... more properties
)
```

### `validate()`
Validate that all required fields are present.

```python
try:
    config.validate()
except ValueError as e:
    print(f"Configuration invalid: {e}")
```

**Raises**: `ValueError` if any required catalog or schema is missing.

### `from_spark(spark, source_system_name)`
Load configuration from Spark configuration (DLT pipelines).

```python
spark = SparkSession.builder.getOrCreate()
config = CentralizedPipelineConfig.from_spark(spark, "lakehouse")
```

Looks for Spark config keys:
- `spark.lakehouse.landing_catalog`
- `spark.lakehouse.raw_catalog`
- `spark.lakehouse.base_catalog`
- `spark.lakehouse.curated_catalog`
- `spark.lakehouse.enriched_catalog`
- `spark.lakehouse.landing_schema`
- `spark.lakehouse.raw_schema`
- `spark.lakehouse.base_schema`
- `spark.lakehouse.dimensions_schema`
- `spark.lakehouse.facts_schema`
- `spark.lakehouse.enriched_schema`

## Usage Examples

### Basic Usage
```python
config = CentralizedPipelineConfig(
    source_system_name="lakehouse",
    environment="dev",
    landing_catalog="landing",
    raw_catalog="raw",
    base_catalog="base",
    curated_catalog="curated",
    enriched_catalog="enriched",
    landing_schema="landing",
    raw_schema="raw",
    base_schema="base",
    dimensions_schema="dimensions",
    facts_schema="facts",
    enriched_schema="enriched",
)

# Validate configuration
config.validate()

# Access properties
print(config.base_catalog)      # "base"
print(config.raw_schema)        # "raw"
```

### From Spark Configuration
```python
from pyspark.sql import SparkSession
from src.framework.config import CentralizedPipelineConfig

spark = SparkSession.builder.getOrCreate()

# In DLT pipeline, Spark config is pre-configured
config = CentralizedPipelineConfig.from_spark(spark, "lakehouse")
config.validate()
```

### With CatalogSchemaManager
```python
from src.framework.config import CatalogSchemaManager

config = CentralizedPipelineConfig(...)
manager = CatalogSchemaManager.from_pipeline_config(config)

# Use manager to build paths
raw_table = manager.get_raw_table_path("customer")
```

### With Builders
```python
from src.framework.config import ConnectorConfigBuilderFactory

builder = ConnectorConfigBuilderFactory.create_builder(
    "rest_api",
    connector_config,
    config  # CentralizedPipelineConfig
)

config = builder.merge_shared_context().build()
```

## Design Rationale

- **Centralized**: Single source of truth for pipeline metadata
- **Type-Safe**: Dataclass with type hints
- **Validated**: Fails fast with clear error messages
- **Flexible**: Supports environment-specific configurations
- **Reusable**: All layers and connectors use same config

## Integration Points

- **CatalogSchemaManager**: Uses catalogs and schemas to construct paths
- **Builders**: Provide context to connector-specific builders
- **Data Contracts**: Environment selection determines which config to load

## Related Classes
- [ConnectorConfig](./connector_config.md)
- [CatalogSchemaManager](./catalog_schema_manager.md)
- [Builders](./builders/README.md)

## See Also
- [Configuration Overview](./README.md)
- [Architecture Guide](./ARCHITECTURE.md)
