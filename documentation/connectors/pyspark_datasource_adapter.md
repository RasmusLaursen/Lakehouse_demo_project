# PySparkDatasourceAdapter

## Overview

`PySparkDatasourceAdapter` is an adapter that bridges custom data sources with PySpark's datasource framework. It enables seamless integration of custom connectors with Spark SQL and structured APIs.

**Location**: `src/framework/connectors/pyspark_datasource_adapter.py`

**Pattern**: Adapter Pattern

## Class Definition

```python
class PySparkDatasourceAdapter:
    """Adapter for PySpark datasource integration."""
```

## Key Responsibilities

1. **Datasource Registration**: Register custom datasource with PySpark
2. **Schema Mapping**: Map custom schemas to PySpark StructType
3. **Read Operations**: Handle read operations from datasource
4. **Configuration Translation**: Convert Connector configs to PySpark options
5. **Partition Handling**: Manage PySpark partitioning

## Constructor

```python
def __init__(self, connector_instance: BaseConnector):
    """
    Initialize adapter.
    
    Args:
        connector_instance: BaseConnector instance to adapt
    """
```

**Parameters**:
- `connector_instance` (BaseConnector): Connector to adapt for PySpark

## Core Methods

### register_datasource()

Register datasource with PySpark.

```python
def register_datasource(self, name: str) -> None:
    """
    Register datasource with PySpark.
    
    Args:
        name: Name to register datasource as
        
    Raises:
        AdapterException: If registration fails
    """
```

**Parameters**:
- `name` (str): Name to register datasource as (e.g., "my_datasource")

**Example**:
```python
adapter = PySparkDatasourceAdapter(rest_api_connector)
adapter.register_datasource("my_api_source")

# Now usable in Spark SQL
df = spark.read.format("my_api_source").load()
```

### get_struct_type()

Get PySpark schema for data.

```python
def get_struct_type(self) -> StructType:
    """
    Get PySpark StructType schema for datasource.
    
    Returns:
        PySpark StructType schema
        
    Raises:
        SchemaException: If schema cannot be determined
    """
```

**Returns**: `pyspark.sql.types.StructType` - Schema

### create_read_option()

Create PySpark read options.

```python
def create_read_option(self, 
                       config: Dict[str, Any]) -> Dict[str, str]:
    """
    Convert connector config to PySpark read options.
    
    Args:
        config: Connector configuration dict
        
    Returns:
        PySpark read options dict
    """
```

**Parameters**:
- `config` (Dict): Connector configuration

**Returns**: `Dict[str, str]` - PySpark read options

## Usage Examples

### Basic Adaptation

```python
from src.framework.connectors import ConnectorFactory
from src.framework.connectors.pyspark_datasource_adapter import PySparkDatasourceAdapter

# Create connector
connector = ConnectorFactory.create_connector(
    "rest_api",
    rest_api_config,
    centralized_config
)

# Adapt for PySpark
adapter = PySparkDatasourceAdapter(connector)

# Register datasource
adapter.register_datasource("my_api_data")

# Use via Spark SQL
df = spark.read.format("my_api_data").load()
```

### With Spark SQL

```python
# Register
adapter = PySparkDatasourceAdapter(connector)
adapter.register_datasource("sales_api")

# Use in SQL
spark.sql("""
    CREATE TABLE sales_source
    USING my_api_data
    LOCATION '/path/to/config'
""")

# Query
df = spark.sql("SELECT * FROM sales_source WHERE date > '2024-01-01'")
```

### With DataFrame API

```python
adapter = PySparkDatasourceAdapter(connector)
adapter.register_datasource("customer_source")

# Read using DataFrame API
df = spark.read.format("customer_source").option(
    "limit", "1000"
).load()

# Transform and write
df.filter("status = 'active'").write.mode("overwrite").parquet("/path/to/data")
```

## Schema Mapping

### Automatic Schema Detection

```python
adapter = PySparkDatasourceAdapter(connector)

# Get schema
schema = adapter.get_struct_type()

print(schema)
# Output: StructType([
#     StructField("id", IntegerType()),
#     StructField("name", StringType()),
#     StructField("email", StringType()),
#     ...
# ])
```

### Schema Application

```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Define schema
schema = StructType([
    StructField("id", IntegerType()),
    StructField("name", StringType()),
    StructField("email", StringType())
])

# Create adapter with schema
adapter = PySparkDatasourceAdapter(connector)

# Read with schema
df = spark.read.schema(schema).format("adapter_name").load()
```

## Configuration Translation

### Option Conversion

```python
connector_config = ConnectorConfig(
    connector_type="rest_api",
    config={
        "url": "https://api.example.com",
        "method": "GET",
        "page_size": 1000
    }
)

adapter = PySparkDatasourceAdapter(connector)

# Convert to PySpark options
pyspark_options = adapter.create_read_option(
    connector_config.config
)

# Use in read
df = spark.read.format("datasource_name").options(
    **pyspark_options
).load()
```

## Integration with Delta Live Tables

### DLT Datasource Usage

```python
# In Delta Live Tables pipeline
import dlt
from src.framework.connectors import ConnectorFactory
from src.framework.connectors.pyspark_datasource_adapter import PySparkDatasourceAdapter

connector = ConnectorFactory.create_connector(
    "rest_api",
    rest_api_config,
    centralized_config
)

adapter = PySparkDatasourceAdapter(connector)
adapter.register_datasource("my_api_source")

@dlt.table
def my_source_table():
    """Load data from REST API via adapter."""
    return spark.read.format("my_api_source").load()
```

## Partition Handling

### Partitioned Reads

```python
# Adapter handles partition mapping
adapter = PySparkDatasourceAdapter(connector)

# Read with partitions
df = spark.read.format("adapter_name").option(
    "partitions", "12"
).load()

# Spark distributes read across 12 partitions
```

## Error Handling

### Invalid Datasource

```python
try:
    adapter = PySparkDatasourceAdapter(connector)
    adapter.register_datasource("")  # Invalid name
except AdapterException as e:
    print(f"Registration failed: {e}")
```

### Schema Mismatch

```python
try:
    schema = adapter.get_struct_type()
except SchemaException as e:
    print(f"Cannot determine schema: {e}")
    # Provide explicit schema
    df = spark.read.schema(explicit_schema).format("adapter_name").load()
```

## Testing

### Test Patterns

```python
class TestPySparkDatasourceAdapter:
    def test_register_datasource(self):
        """Test datasource registration."""
        adapter = PySparkDatasourceAdapter(test_connector)
        adapter.register_datasource("test_source")
        
        # Verify registration
        df = spark.read.format("test_source").load()
        assert df is not None
    
    def test_schema_mapping(self):
        """Test schema mapping."""
        adapter = PySparkDatasourceAdapter(test_connector)
        
        schema = adapter.get_struct_type()
        
        assert isinstance(schema, StructType)
        assert len(schema.fields) > 0
    
    def test_option_conversion(self):
        """Test configuration to options."""
        adapter = PySparkDatasourceAdapter(test_connector)
        
        options = adapter.create_read_option({
            "url": "https://api.example.com",
            "timeout": 30
        })
        
        assert "url" in options
        assert options["url"] == "https://api.example.com"
```

## Best Practices

### 1. **Datasource Naming**

```python
# ✅ Descriptive names
adapter.register_datasource("customer_api_source")
adapter.register_datasource("sales_db_connector")

# ❌ Vague names
adapter.register_datasource("source1")
adapter.register_datasource("data")
```

### 2. **Schema Management**

```python
# ✅ Explicit schema
schema = StructType([
    StructField("id", IntegerType()),
    StructField("name", StringType())
])
df = spark.read.schema(schema).format("source").load()

# ❌ Inferred (if possible, specify for clarity)
df = spark.read.format("source").load()
```

### 3. **Option Setting**

```python
# ✅ Clear options
df = spark.read.format("source").option("timeout", "30").option(
    "retry_count", "3"
).load()

# ❌ Too many options
df = spark.read.format("source").options({
    "many": "options",
    "hard": "to",
    "read": "and",
    "maintain": "them"
}).load()
```

## Related Classes

- [BaseConnector](./base_connector.md) - Adapted connector
- [RestApiConnector](./datasources/rest_api_connector.md) - Example connector
- [RestApiDatasource](./datasources/rest_api_datasource.md) - DLT variant
- [ConnectorFactory](./connector_factory.md) - Connector creation

## See Also

- [QUICK_REFERENCE.md](./QUICK_REFERENCE.md) - Quick lookup
- [ARCHITECTURE.md](./ARCHITECTURE.md) - Design patterns
- [Delta Live Tables Documentation](https://docs.databricks.com/en/delta-live-tables/index.html)
