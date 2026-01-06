# Connectors Quick Reference

## Creating Connectors

### Basic Factory Usage

```python
from src.framework.connectors import ConnectorFactory
from src.framework.config import ConnectorConfig, CentralizedPipelineConfig

# Create config
connector_config = ConnectorConfig("rest_api", {...})
centralized = CentralizedPipelineConfig(...)

# Create connector
connector = ConnectorFactory.create_connector(
    "rest_api", connector_config, centralized
)
```

### Factory-Supported Types

| Type | Connector | Purpose |
|------|-----------|---------|
| `"dataframe"` | DataFrameConnector | Direct Spark DataFrame |
| `"rest_api"` | RestApiConnector | REST API endpoint |
| `"jdbc"` | JdbcConnector | JDBC database |
| `"autoloader"` | AutoLoaderConnector | Cloud files |
| `"rest_api_ds"` | RestApiDatasource | REST API for DLT |
| `"rest_api_workflow_ds"` | RestApiWorkflowDatasource | REST API workflow |

## Loading Data

### Standard Pattern

```python
try:
    connector.validate()
    df = connector.load(spark)
    # Use df...
finally:
    connector.close()
```

### With Schema

```python
from pyspark.sql.types import StructType, StructField, StringType

schema = StructType([
    StructField("name", StringType()),
    StructField("age", StringType())
])

df = connector.load(spark, schema=schema)
```

## Configuration Examples

### REST API Connector

```python
ConnectorConfig(
    connector_type="rest_api",
    config={
        "url": "https://api.example.com/data",
        "method": "GET",
        "auth_type": "oauth2",
        "auth_config": {
            "client_id": "YOUR_CLIENT_ID",
            "client_secret": "YOUR_CLIENT_SECRET",
            "token_url": "https://oauth.example.com/token"
        }
    }
)
```

### JDBC Connector

```python
ConnectorConfig(
    connector_type="jdbc",
    config={
        "url": "jdbc:mysql://localhost:3306/mydb",
        "username": "user",
        "password": "password",
        "query": "SELECT * FROM my_table"
    }
)
```

### AutoLoader Connector

```python
ConnectorConfig(
    connector_type="autoloader",
    config={
        "source_path": "s3://bucket/path/",
        "format": "csv",
        "schema_location": "s3://bucket/.schema"
    }
)
```

### DataFrame Connector

```python
ConnectorConfig(
    connector_type="dataframe",
    config={
        "dataframe": my_dataframe
    }
)
```

## Using Builders

### Create and Configure

```python
from src.framework.config import ConnectorConfigBuilderFactory

# Create builder
builder = ConnectorConfigBuilderFactory.create_builder("rest_api")

# Configure (fluent API)
config = (builder
    .with_url("https://api.example.com")
    .with_method("GET")
    .with_auth_type("oauth2")
    .with_auth_config({...})
    .build()
)

# Create connector
connector = ConnectorFactory.create_connector(
    "rest_api", config, centralized
)
```

## Partitioning Strategies

### Date Range Partitioning

```python
from src.framework.connectors.partition_strategies import DateRangePartitionStrategy

strategy = DateRangePartitionStrategy(
    start_date="2024-01-01",
    end_date="2024-12-31",
    partition_interval="month"
)

config = ConnectorConfig(
    connector_type="rest_api",
    config={
        "url": "...",
        "partition_strategy": strategy
    }
)
```

### Sequential ID Partitioning

```python
from src.framework.connectors.partition_strategies import SequentialPartitionStrategy

strategy = SequentialPartitionStrategy(
    min_id=1,
    max_id=1000000,
    partitions=10
)
```

## OAuth2 Authentication

### Configuration

```python
from src.framework.config import SecretResolver

secret_resolver = SecretResolver(centralized_config)

oauth_config = {
    "client_id": secret_resolver.resolve("oauth_client_id"),
    "client_secret": secret_resolver.resolve("oauth_client_secret"),
    "token_url": "https://oauth.provider.com/token"
}
```

### Manual Token Management

```python
from src.framework.connectors.oauth2_token_manager import OAuth2TokenManager

manager = OAuth2TokenManager(oauth_config)
token = manager.get_token()

# Use in headers
headers = {"Authorization": f"Bearer {token}"}
```

## PySpark Integration

### Using Adapters

```python
from src.framework.connectors.pyspark_datasource_adapter import PySparkDatasourceAdapter

# Create adapter
adapter = PySparkDatasourceAdapter(connector)

# Register datasource
adapter.register_datasource("my_source")

# Read via Spark
df = spark.read.format("my_source").load()
```

### Delta Live Tables

```python
import dlt
from src.framework.connectors import ConnectorFactory
from src.framework.connectors.pyspark_datasource_adapter import PySparkDatasourceAdapter

connector = ConnectorFactory.create_connector(...)
adapter = PySparkDatasourceAdapter(connector)
adapter.register_datasource("dlt_source")

@dlt.table
def my_table():
    return spark.read.format("dlt_source").load()
```

## Error Handling

### Common Exceptions

```python
from src.framework.exceptions import (
    ConfigurationException,
    ValidationException,
    ConnectorException,
    TokenException
)

try:
    connector.validate()
except ConfigurationException as e:
    print(f"Config error: {e}")
except ValidationException as e:
    print(f"Validation error: {e}")

try:
    df = connector.load(spark)
except ConnectorException as e:
    print(f"Load error: {e}")
except TokenException as e:
    print(f"Auth error: {e}")
```

## Testing Connectors

### Test Pattern

```python
import pytest
from src.framework.connectors import ConnectorFactory

def test_connector_loading():
    """Test connector loads data."""
    connector = ConnectorFactory.create_connector(
        "rest_api",
        config,
        centralized
    )
    
    connector.validate()
    df = connector.load(spark)
    
    assert df.count() > 0
    connector.close()
```

## Common Configuration Fields

### All Connectors

| Field | Type | Description |
|-------|------|-------------|
| connector_type | str | Connector type |
| timeout | int | Timeout in seconds (optional) |
| retry_count | int | Retry count (optional) |

### REST API Connector

| Field | Type | Required |
|-------|------|----------|
| url | str | ✓ |
| method | str | ✓ |
| headers | dict | ✗ |
| query_params | dict | ✗ |
| body | dict | ✗ |
| auth_type | str | ✗ |

### JDBC Connector

| Field | Type | Required |
|-------|------|----------|
| url | str | ✓ |
| username | str | ✓ |
| password | str | ✓ |
| query | str | ✓ |

### AutoLoader Connector

| Field | Type | Required |
|-------|------|----------|
| source_path | str | ✓ |
| format | str | ✓ |
| schema_location | str | ✓ |

## Schema Operations

### Schema Inference

```python
# Let connector infer schema
df = connector.load(spark)  # schema=None
```

### Explicit Schema

```python
from pyspark.sql.types import *

schema = StructType([
    StructField("id", IntegerType()),
    StructField("name", StringType()),
    StructField("email", StringType())
])

df = connector.load(spark, schema=schema)
```

## Lifecycle Management

### Full Lifecycle

```python
# 1. Create
connector = ConnectorFactory.create_connector(...)

# 2. Validate (optional but recommended)
connector.validate()

# 3. Load
df = connector.load(spark)

# 4. Use
transformed = df.filter("value > 100")

# 5. Close (always do this)
connector.close()
```

### Context Manager Pattern

```python
# Using try/finally for proper cleanup
try:
    df = connector.load(spark)
    # Process data
finally:
    connector.close()  # Guaranteed cleanup
```

## Performance Tips

### 1. Use Partitioning

```python
# Parallel loading with partitions
strategy = DateRangePartitionStrategy(...)
config = ConnectorConfig(
    ...,
    partition_strategy=strategy
)
```

### 2. Cache OAuth Tokens

```python
# Token manager auto-caches
manager = OAuth2TokenManager(config)  # Caches first token
token1 = manager.get_token()           # Returns cached
token2 = manager.get_token()           # Returns same cached
```

### 3. Reuse Connectors

```python
# Create once, use multiple times
connector = ConnectorFactory.create_connector(...)

df1 = connector.load(spark)
df2 = connector.load(spark)  # Reuses connector

connector.close()  # Clean up once
```

### 4. Schema Caching

```python
# Cache schema to avoid repeated inference
schema = connector.get_schema(spark)
df1 = connector.load(spark, schema=schema)
df2 = connector.load(spark, schema=schema)
```

## Documentation Links

- [BaseConnector](./base_connector.md) - Base class
- [ConnectorFactory](./connector_factory.md) - Creation
- [DataFrameConnector](./dataframe_connector.md) - DataFrame connector
- [OAuth2TokenManager](./oauth2_token_manager.md) - Authentication
- [PartitionStrategies](./partition_strategies.md) - Partitioning
- [PySparkDatasourceAdapter](./pyspark_datasource_adapter.md) - PySpark integration
- [Datasources](./datasources/README.md) - Connector variants
- [ARCHITECTURE.md](./ARCHITECTURE.md) - Design patterns
- [INDEX.md](./INDEX.md) - Full documentation index
