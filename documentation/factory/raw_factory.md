# Raw Factory - Ingestion Pipeline Creation

**File**: `src/framework/factory/raw_factory.py`

## Overview

The `RawPipelineFactory` creates raw layer ingestion pipelines dynamically. It abstracts away connector complexity, configuration management, and DLT table creation boilerplate.

## Class: RawPipelineFactory

### Purpose
Encapsulate raw layer table creation logic, supporting multiple connector types (REST API, JDBC, Volume, Autoloader, S3, etc.)

### Constructor

```python
class RawPipelineFactory:
    def __init__(self, spark: SparkSession):
        """Initialize the factory.
        
        Args:
            spark: Active SparkSession
        """
        self.spark = spark
```

---

## Main Methods

### create_pipeline(source_system_name: str)

**Signature**:
```python
def create_pipeline(self, source_system_name: str) -> None:
    """Create raw layer ingestion pipeline for a source system.
    
    Loads data contracts and creates DLT tables using appropriate connectors.
    Processes schemas in two passes:
    - Pass 1: Root call tables (independent)
    - Pass 2: Dependent call tables (depends on root being available)
    
    Args:
        source_system_name: Name of the source system (e.g., 'lakehouse', 'review')
    """
```

**Flow Diagram**:
```
create_pipeline(source_system_name)
├── Load CentralizedPipelineConfig
├── Create CatalogSchemaManager
├── Load data contract (YAML)
│
├─ PASS 1: Root Tables
│ ├── For each schema where is_root_call=true:
│ │   └── _process_schema(schema)
│ └── [All root tables materialized]
│
└─ PASS 2: Dependent Tables
  ├── For each schema where is_root_call=false:
  │   └── _process_schema(schema)
  └── [All tables complete]
```

**Example**:
```python
spark = SparkSession.getActiveSession()
factory = RawPipelineFactory(spark)
factory.create_pipeline("lakehouse")  # Creates all raw tables
```

---

### _process_schema(...)

**Signature**:
```python
def _process_schema(
    self,
    schema: Any,
    server_config: Any,
    centralized_config: CentralizedPipelineConfig,
    catalog_manager: CatalogSchemaManager
) -> None:
    """Process a single schema to create raw table.
    
    Args:
        schema: Schema object from data contract
        server_config: Server configuration (connector details)
        centralized_config: Centralized pipeline configuration
        catalog_manager: Catalog/schema path manager
    """
```

**Responsibilities**:
1. Extract model name from schema
2. Get server configuration (connector type, credentials, etc.)
3. Call `_create_raw_table`
4. Optionally create backfill table if configured

---

### _create_raw_table(...)

**Signature**:
```python
def _create_raw_table(
    self,
    model_name: str,
    schema: Any,
    server_config: Any,
    centralized_config: CentralizedPipelineConfig
) -> None:
    """Create raw layer DLT table using connector framework.
    
    Dynamically creates connector based on data contract configuration.
    
    Args:
        model_name: Name of the model/table
        schema: Schema object from data contract
        server_config: Server configuration from data contract
        centralized_config: Centralized pipeline configuration
    """
```

**Key Steps**:

```python
1. Extract connector type from server_config
   └─ e.g., "rest_api", "jdbc", "volume", "autoloader"

2. Create ConnectorConfig from server_config
   └─ Wraps configuration in typed object

3. Use ConnectorConfigBuilderFactory to create appropriate builder
   ├─ RestApiConfigBuilder
   ├─ JdbcConfigBuilder
   ├─ VolumeConfigBuilder
   ├─ AutoLoaderConfigBuilder
   └─ etc.

4. Build final configuration
   ├─ builder.merge_schema_overrides(schema)
   ├─ builder.build_spark_schema(model_name, schema)  # if applicable
   ├─ builder.resolve_secrets()
   └─ final_config = builder.build()

5. Create connector instance
   └─ connector = ConnectorFactory.create(connector_type, final_config)

6. Create DLT table
   └─ lakeflow_declarative_pipeline.ldp_table(
       name="raw.<schema>.<model>",
       connector=connector
   )
```

---

### _create_backfill_if_needed(...)

**Signature**:
```python
def _create_backfill_if_needed(
    self,
    model_name: str,
    validated_data_config: Any,
    centralized_config: CentralizedPipelineConfig
) -> None:
    """Create backfill append flow if configured.
    
    Checks if backfill is enabled in data contract and creates
    a separate backfill flow for historical data loading.
    
    Args:
        model_name: Name of the model/table
        validated_data_config: Validated data configuration
        centralized_config: Centralized pipeline configuration
    """
```

**When used**: 
- First-time data load needs historical data
- Backfill field set to `true` in data contract
- Typically used with REST APIs that have pagination

---

## Two-Pass Schema Processing

### Why Two Passes?

Some schemas have dependencies on others. Two passes ensure correct materialization order:

```yaml
# data_contracts/source_system/lakehouse.yml
schema:
  - name: customer           # Pass 1 (root_call: true)
    is_root_call: true
    
  - name: customer_booking   # Pass 2 (depends on customer)
    is_root_call: false
    depends_on: [customer]
```

### Pass 1: Root Tables
```
Process all schemas with is_root_call: true
├─ No dependencies on other schemas
├─ Can be created in any order
└─ All independent tables materialized
```

### Pass 2: Dependent Tables
```
Process all schemas with is_root_call: false
├─ May depend on tables from Pass 1
├─ Safe to process after Pass 1 completes
└─ Can reference Pass 1 tables in logic
```

---

## Connector Integration

### Connector Type Detection

```python
# From server_config, extract connector_type
connector_type = server_config.customProperties
    .find(p => p.property == "connector_type")
    .value

# Examples:
"rest_api"      → REST API connector
"jdbc"          → JDBC connector
"volume"        → Volume (file system) connector
"autoloader"    → Autoloader connector
"s3"            → S3 connector
```

### Configuration Building Pipeline

```python
# 1. Create base config from server_config
base_config = ConnectorConfig.from_server_config(server_config)

# 2. Get appropriate builder
# IMPORTANT: Pass model_name to builder for per-schema configuration
builder = ConnectorConfigBuilderFactory.create_builder(
    connector_type=base_config.connector_type,
    base_config=base_config,
    centralized_config=centralized_config,
    model_name=model_name  # Critical for AutoLoader per-schema volume mapping
)

# 3. Enhance configuration
builder = (
    builder
    .merge_schema_overrides(schema)
    .merge_shared_context()  # Uses model_name for per-schema logic
    .resolve_secrets()
)

# 4. Get final configuration
final_config = builder.build()

# 5. Create connector
connector = ConnectorFactory.create(connector_type, final_config)
```

**Key Feature**: When `model_name` is passed to the builder:
- **AutoLoader**: Sets volume to model_name (per-schema volume mapping)
- **Other connectors**: Parameter is ignored, configuration unchanged

---

## Data Contract Structure

### Example YAML

```yaml
# data_contracts/source_system/lakehouse.yml
---
catalog: source_system
object: lakehouse
version: 1.0

server:
  - property: connector_type
    value: rest_api
  - property: endpoint
    value: https://api.example.com/v1
  - property: auth_type
    value: bearer
  - property: auth_token
    value: "{{secrets/lakehouse/api_token}}"

schema:
  - name: customer
    type: object
    is_root_call: true
    
    customProperties:
      - property: source_type
        value: api
      - property: backfill
        value: "true"
    
    properties:
      - name: customer_id
        type: integer
        required: true
      - name: name
        type: string
      - name: email
        type: string

  - name: booking
    type: object
    is_root_call: false
    
    properties:
      - name: booking_id
        type: integer
      - name: customer_id
        type: integer
      - name: booked_at
        type: datetime
```

---

## Configuration Resolution

### Secret Resolution

Secrets are resolved automatically during config building:

```yaml
auth_token: "{{secrets/lakehouse/api_token}}"
```

Becomes:
```python
# At build time, resolver.resolve() is called
secret_value = dbutils.secrets.get(scope="lakehouse", key="api_token")
# auth_token: "<actual-secret-value>"
```

### Spark Config Resolution

```yaml
api_endpoint: "{{spark.api_endpoint}}"
```

Becomes:
```python
# At build time
endpoint = spark.conf.get("api.endpoint")
```

---

## Error Handling

### Per-Table Error Handling

```python
for schema in data_contract.schema_:
    try:
        self._process_schema(schema, ...)
    except Exception as e:
        logger.error(f"Error processing {schema.name}: {e}")
        continue  # Continue processing other schemas
```

**Benefit**: One failed table doesn't break entire pipeline

### Common Errors

| Error | Cause | Solution |
|-------|-------|----------|
| Secret not found | Invalid scope/key | Check Databricks secret store |
| Connector creation failed | Invalid config | Validate data contract YAML |
| Schema mismatch | Type incompatibility | Check schema definitions |
| Connection refused | Network/credentials | Check endpoint and auth |

---

## Logging

### Log Levels

```python
logger.info(f"Creating raw pipeline for {source_system_name}")
logger.info(f"Processing root call schema: {schema.name}")
logger.debug(f"Created connector: {type(connector).__name__}")
logger.error(f"Error creating table {model_name}: {e}")
```

### Sample Log Output

```
2024-01-15 10:30:45 INFO  Creating raw pipeline for: lakehouse
2024-01-15 10:30:45 INFO  === PASS 1: Creating root call tables ===
2024-01-15 10:30:46 INFO  Processing root call schema: customer
2024-01-15 10:30:46 DEBUG Created rest_api connector for customer
2024-01-15 10:30:47 INFO  Created raw table: customer using rest_api connector
2024-01-15 10:30:47 INFO  === PASS 2: Creating dependent call tables ===
2024-01-15 10:30:48 INFO  Processing root call schema: booking
2024-01-15 10:30:48 DEBUG Created rest_api connector for booking
2024-01-15 10:30:49 INFO  Created raw table: booking using rest_api connector
2024-01-15 10:30:49 INFO  Completed raw pipeline creation for lakehouse
```

---

## Usage Examples

### Basic Pipeline Creation

```python
from pyspark.sql import SparkSession
from src.framework.factory.raw_factory import RawPipelineFactory

spark = SparkSession.getActiveSession()
factory = RawPipelineFactory(spark)

# Create all raw tables for lakehouse
factory.create_pipeline("lakehouse")

# Creates:
# - raw.landing.customer
# - raw.landing.booking
# - etc. (all tables from data contract)
```

### In Solution Layer

```python
# src/solution/raw/raw_ingest_lakehouse.py
"""Raw layer pipeline for lakehouse."""

from src.framework.factory.raw_factory import create_raw_pipeline

# That's it! Everything else is in the factory
create_raw_pipeline("lakehouse")
```

### With Multiple Source Systems

```python
# src/solution/raw/raw_ingest_all.py
"""Create raw pipelines for all source systems."""

from src.framework.factory.raw_factory import create_raw_pipeline

# Create pipelines for multiple sources
for source_system in ["lakehouse", "review", "energydataservice"]:
    create_raw_pipeline(source_system)
```

---

## Design Decisions

### Why Two-Pass Processing?

**Problem**: Dependent schemas need other schemas to be materialized first

**Solution**: Process in two passes
- Pass 1: Independent schemas
- Pass 2: Dependent schemas (can reference Pass 1 results)

**Alternative Considered**: Topological sort - more complex, overkill for current needs

### Why Per-Table Error Handling?

**Problem**: One connector error shouldn't break entire pipeline

**Solution**: Try-catch each schema, log error, continue

**Alternative Considered**: Fail-fast - too risky in production

### Why Connector Factory Integration?

**Problem**: Raw factory shouldn't know about specific connector implementations

**Solution**: Use ConnectorFactory for abstraction
- Factory only orchestrates process
- Connectors handle specifics (REST, JDBC, etc.)

**Benefit**: Add new connector type without changing factory code

---

## Integration Points

### Depends On:
- **CentralizedPipelineConfig**: Catalog/schema names
- **CatalogSchemaManager**: Path construction
- **ConnectorConfigBuilderFactory**: Config building
- **ConnectorFactory**: Connector creation
- **DataContractHelper**: YAML parsing
- **SecretResolver**: Secret resolution

### Used By:
- Solution layer modules (e.g., `src/solution/raw/raw_ingest_lakehouse.py`)
- Data pipeline orchestration

### Related:
- **BasePipelineFactory**: Downstream processing (CDC, dedup)
- **Connector Framework**: Actual data loading implementation

---

## Performance Considerations

### Schema Processing
- Pass 1 processes all root tables sequentially
- Pass 2 processes all dependent tables sequentially
- No parallelization within passes (maintains order)

### Optimization Points
- Schema validation happens once
- Config resolution happens once per schema
- Connector reuse possible (future enhancement)

---

## Troubleshooting

### Pipeline Starts but No Tables Created
**Check**:
1. Is data contract file present? `data_contracts/source_system/lakehouse.yml`
2. Does contract have schema entries?
3. Check logs for errors

### Connector Creation Fails
**Check**:
1. Is connector type valid? (rest_api, jdbc, volume, autoloader, s3)
2. Are credentials correct? Check secrets
3. Is configuration complete?

### Tables Created But Empty
**Check**:
1. Is connector running? Check connector logs
2. Is backfill configured? May need separate load

---

## Extensions

### Adding New Connector Type

1. Create `ConnectorConfig` subclass
2. Create builder in `ConnectorConfigBuilderFactory`
3. Create connector in `ConnectorFactory`
4. Data contracts automatically work with new type

### Custom Transformation

Implement in connector or as post-processing:
```python
@dlt.table(name=...)
def custom_table():
    df = connector.read()
    # Custom transformations
    return df.filter(...).select(...)
```

---

## Next Steps

- [Base Factory](./base_factory.md) - Downstream CDC processing
- [Connector Framework](../connectors/README.md) - Connector implementations
- [Configuration System](../configuration/README.md) - Config details
