# CatalogSchemaManager

## Location
`src/framework/config/catalog_schema_manager.py`

## Purpose
Constructs fully qualified table paths for all layers using the canonical format: `catalog.schema.table`.

## Responsibilities
- Build canonical table paths
- Provide layer-specific path builders
- Eliminate hardcoded path construction
- Ensure consistent naming conventions

## Properties

### catalogs
Dictionary storing catalog names for each layer.

```python
{
    'landing': 'landing',
    'raw': 'raw',
    'base': 'base',
    'dimensions': 'curated',
    'facts': 'curated',
    'enriched': 'enriched',
}
```

### schemas
Dictionary storing schema names for each layer.

```python
{
    'landing': 'landing',
    'raw': 'raw',
    'base': 'base',
    'dimensions': 'dimensions',
    'facts': 'facts',
    'enriched': 'enriched',
}
```

## Methods

### `__init__(catalogs, schemas)`
Initialize with catalog and schema mappings.

```python
manager = CatalogSchemaManager(
    catalogs={'landing': 'landing', 'raw': 'raw', ...},
    schemas={'landing': 'landing', 'raw': 'raw', ...}
)
```

### `get_table_path(catalog, schema, table)`
Get fully qualified table path: `catalog.schema.table`.

```python
path = manager.get_table_path("mycat", "myschema", "mytable")
# Output: "mycat.myschema.mytable"
```

### `get_base_table_path(table_name)`
Get base layer table path using preconfigured catalog and schema.

```python
path = manager.get_base_table_path("customer")
# Output: "base.base.customer"
```

### `get_raw_table_path(table_name)`
Get raw layer table path.

```python
path = manager.get_raw_table_path("raw_events")
# Output: "raw.raw.raw_events"
```

### `get_dimension_table_path(dimension_name)`
Get dimension (curated layer) table path.

```python
path = manager.get_dimension_table_path("dim_customer")
# Output: "curated.dimensions.dim_customer"
```

### `get_fact_table_path(fact_name)`
Get fact (curated layer) table path.

```python
path = manager.get_fact_table_path("fact_sales")
# Output: "curated.facts.fact_sales"
```

### `from_pipeline_config(pipeline_config)`
Create manager from CentralizedPipelineConfig.

```python
from src.framework.config import (
    CentralizedPipelineConfig,
    CatalogSchemaManager
)

config = CentralizedPipelineConfig(...)
manager = CatalogSchemaManager.from_pipeline_config(config)
```

## Usage Examples

### Basic Path Construction
```python
manager = CatalogSchemaManager(
    catalogs={
        'landing': 'landing',
        'raw': 'raw',
        'base': 'base',
        'dimensions': 'curated',
        'facts': 'curated',
        'enriched': 'enriched',
    },
    schemas={
        'landing': 'landing',
        'raw': 'raw',
        'base': 'base',
        'dimensions': 'dimensions',
        'facts': 'facts',
        'enriched': 'enriched',
    }
)

# Get specific table path
table = manager.get_table_path("landing", "landing", "raw_events")
# Output: "landing.landing.raw_events"
```

### Layer-Specific Paths
```python
# Raw layer
raw_table = manager.get_raw_table_path("customer")
# Output: "raw.raw.customer"

# Base layer
base_table = manager.get_base_table_path("customer_clean")
# Output: "base.base.customer_clean"

# Dimension (curated layer)
dim = manager.get_dimension_table_path("dim_customer")
# Output: "curated.dimensions.dim_customer"

# Fact (curated layer)
fact = manager.get_fact_table_path("fact_sales")
# Output: "curated.facts.fact_sales"

# Enriched layer
enriched = manager.get_table_path("enriched", "enriched", "customer_360")
# Output: "enriched.enriched.customer_360"
```

### From Pipeline Configuration
```python
from src.framework.config import CentralizedPipelineConfig, CatalogSchemaManager

# Create pipeline config
config = CentralizedPipelineConfig(
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

# Create manager from config
manager = CatalogSchemaManager.from_pipeline_config(config)

# Use manager
raw_path = manager.get_raw_table_path("events")
```

### In Data Loading Code
```python
def load_raw_data(spark, table_name, manager):
    """Load raw data using manager for paths."""
    source_path = manager.get_raw_table_path(table_name)
    
    df = spark.read.format("delta").load(source_path)
    return df

def save_dimension(spark, dim_name, df, manager):
    """Save dimension using manager for paths."""
    target_path = manager.get_dimension_table_path(dim_name)
    
    df.write.format("delta").mode("overwrite").save(target_path)

# Usage
manager = CatalogSchemaManager.from_pipeline_config(config)
df = load_raw_data(spark, "customers", manager)
save_dimension(spark, "dim_customer", df, manager)
```

### In Configuration Building
```python
from src.framework.config import ConnectorConfigBuilderFactory

builder = ConnectorConfigBuilderFactory.create_builder(
    "rest_api_workflow_ds",
    connector_config,
    centralized_config
)

# Builder uses manager internally
config = builder.merge_shared_context().build()

# Manager can also be created separately for use in workflows
manager = CatalogSchemaManager.from_pipeline_config(centralized_config)
raw_table = manager.get_raw_table_path(model_name)
```

## Common Patterns

### Pattern: Dynamic Table Name Construction
```python
def get_layer_path(manager, layer, table_name):
    """Get table path for any layer."""
    methods = {
        'raw': manager.get_raw_table_path,
        'base': manager.get_base_table_path,
        'dimension': manager.get_dimension_table_path,
        'fact': manager.get_fact_table_path,
    }
    return methods[layer](table_name)

path = get_layer_path(manager, 'raw', 'events')
```

### Pattern: Batch Processing Multiple Tables
```python
tables = ['customer', 'order', 'product']
manager = CatalogSchemaManager.from_pipeline_config(config)

for table in tables:
    raw_path = manager.get_raw_table_path(table)
    base_path = manager.get_base_table_path(table)
    
    df = spark.read.format("delta").load(raw_path)
    df.write.format("delta").mode("overwrite").save(base_path)
```

## Layer Organization

The system organizes data in distinct layers:

| Layer | Catalog | Schema | Purpose |
|-------|---------|--------|---------|
| **Landing** | landing | landing | Raw files from source systems |
| **Raw** | raw | raw | Lightly transformed raw data |
| **Base** | base | base | Heavily cleaned, deduplicated data |
| **Dimensions** | curated | dimensions | Reference dimension tables |
| **Facts** | curated | facts | Fact tables for analysis |
| **Enriched** | enriched | enriched | Business-ready enriched data |

## Design Rationale

- **Centralized**: Single source for path construction
- **Consistent**: Ensures all tables follow same naming pattern
- **Type-Safe**: Layer methods enforce correct catalog/schema pairs
- **Maintainable**: Change catalogs/schemas in one place
- **Chainable**: Works well with builder pattern

## Integration Points

- **CentralizedPipelineConfig**: Source of catalog and schema definitions
- **Builders**: Used internally to build paths for specific layers
- **Data Loading**: Provides paths for spark.read and spark.write
- **Workflows**: Reference layer paths in pipeline steps

## Related Classes
- [CentralizedPipelineConfig](./centralized_config.md)
- [ConnectorConfig](./connector_config.md)

## See Also
- [Configuration Overview](./README.md)
- [Architecture Guide](./ARCHITECTURE.md)
