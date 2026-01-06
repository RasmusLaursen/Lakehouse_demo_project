# Datasources

## Overview

Datasources are specialized connector variants designed for specific use cases. This directory contains documentation for different datasource implementations and variants.

## Datasource Categories

### 1. **Core Connectors** (Parent Directory)

Standard connectors for direct data loading:
- **DataFrameConnector** - Use Spark DataFrames
- **RestApiConnector** - Load from REST APIs
- **JdbcConnector** - Load from JDBC databases
- **AutoLoaderConnector** - Load from cloud files

See: [../README.md](../README.md)

### 2. **Datasources** (This Directory)

Specialized variants for specific frameworks:
- **RestApiDatasource** - REST API for Delta Live Tables
- **RestApiWorkflowDatasource** - REST API workflow variant
- **JdbcDatasource** (if applicable)
- **AutoLoaderDatasource** (if applicable)

## Datasource Files

| File | Purpose |
|------|---------|
| [rest_api_connector.md](./rest_api_connector.md) | REST API data loading |
| [rest_api_datasource.md](./rest_api_datasource.md) | REST API for DLT |
| [rest_api_workflow_datasource.md](./rest_api_workflow_datasource.md) | REST API workflow variant |
| [jdbc_connector.md](./jdbc_connector.md) | JDBC database connector |
| [autoloader_connector.md](./autoloader_connector.md) | Cloud file loading with AutoLoader |

## Quick Reference

### REST API Loading

```python
from src.framework.connectors import ConnectorFactory
from src.framework.config import ConnectorConfig

config = ConnectorConfig(
    connector_type="rest_api",
    config={
        "url": "https://api.example.com",
        "method": "GET"
    }
)

connector = ConnectorFactory.create_connector(
    "rest_api", config, centralized
)

df = connector.load(spark)
```

### REST API with Delta Live Tables

```python
import dlt
from src.framework.connectors import ConnectorFactory
from src.framework.connectors.pyspark_datasource_adapter import PySparkDatasourceAdapter

@dlt.table
def my_api_data():
    connector = ConnectorFactory.create_connector(
        "rest_api",
        rest_api_config,
        centralized
    )
    
    adapter = PySparkDatasourceAdapter(connector)
    adapter.register_datasource("api_source")
    
    return spark.read.format("api_source").load()
```

### JDBC Database Loading

```python
config = ConnectorConfig(
    connector_type="jdbc",
    config={
        "url": "jdbc:mysql://localhost:3306/mydb",
        "username": "user",
        "password": "password",
        "query": "SELECT * FROM my_table"
    }
)

connector = ConnectorFactory.create_connector(
    "jdbc", config, centralized
)

df = connector.load(spark)
```

### AutoLoader Cloud Files

```python
config = ConnectorConfig(
    connector_type="autoloader",
    config={
        "source_path": "s3://bucket/path/",
        "format": "csv",
        "schema_location": "s3://bucket/.schema"
    }
)

connector = ConnectorFactory.create_connector(
    "autoloader", config, centralized
)

df = connector.load(spark)
```

## Connector Comparison

### By Framework

| Framework | Connector | Purpose |
|-----------|-----------|---------|
| **Any** | RestApiConnector | REST API endpoint |
| **Any** | JdbcConnector | JDBC database |
| **Any** | AutoLoaderConnector | Cloud files |
| **DLT** | RestApiDatasource | REST API for DLT |
| **DLT** | RestApiWorkflowDatasource | REST API workflow |

### By Data Source Type

| Source Type | Connector | Best For |
|-------------|-----------|----------|
| **REST API** | RestApiConnector | Direct REST calls |
| **REST API + DLT** | RestApiDatasource | DLT tables |
| **Database** | JdbcConnector | SQL queries |
| **Cloud Files** | AutoLoaderConnector | S3, Azure, GCS |
| **Spark DataFrame** | DataFrameConnector | In-memory data |

### By Performance Characteristics

| Characteristic | Recommended |
|---|---|
| **Parallel Loading** | RestApiConnector, JdbcConnector (with partitioning) |
| **Streaming** | AutoLoaderConnector, RestApiDatasource |
| **Small Data** | DataFrameConnector, RestApiConnector |
| **Large Data** | JdbcConnector (partitioned), AutoLoaderConnector |
| **Real-time** | RestApiDatasource, AutoLoaderConnector |

## Feature Comparison

### Supported Features by Connector

| Feature | REST API | JDBC | AutoLoader | DLT |
|---------|----------|------|-----------|-----|
| **Authentication** | ✓ (OAuth2) | ✓ | ✓ | ✓ |
| **Partitioning** | ✓ | ✓ | ✗ | ✓ |
| **Schema Inference** | ✓ | ✓ | ✓ | ✓ |
| **Incremental** | ✗ | ✗ | ✓ | ✓ |
| **Streaming** | ✗ | ✗ | ✓ | ✓ |

## Authentication Methods

### By Connector Type

| Connector | Methods |
|-----------|---------|
| **RestApiConnector** | API Key, OAuth2, Bearer Token, Basic Auth |
| **JdbcConnector** | Username/Password, Windows Auth |
| **AutoLoaderConnector** | IAM, Storage credentials |

## Configuration Details

### REST API Connector

See: [rest_api_connector.md](./rest_api_connector.md)

**Key Configuration**:
```python
{
    "url": str,              # API endpoint
    "method": str,           # GET, POST, etc.
    "headers": dict,         # Optional headers
    "auth_type": str,        # oauth2, bearer, basic
    "auth_config": dict      # Auth-specific config
}
```

### JDBC Connector

See: [jdbc_connector.md](./jdbc_connector.md)

**Key Configuration**:
```python
{
    "url": str,              # JDBC connection string
    "username": str,         # Database username
    "password": str,         # Database password
    "query": str             # SQL query
}
```

### AutoLoader Connector

See: [autoloader_connector.md](./autoloader_connector.md)

**Key Configuration**:
```python
{
    "source_path": str,      # Cloud storage path
    "format": str,           # File format
    "schema_location": str   # Schema evolution path
}
```

## Partitioning Strategies

### Supported by Connector

| Strategy | REST API | JDBC | AutoLoader |
|----------|----------|------|-----------|
| **DateRange** | ✓ | ✓ | - |
| **Sequential** | ✓ | ✓ | - |
| **NoPartition** | ✓ | ✓ | ✓ |

See: [../partition_strategies.md](../partition_strategies.md)

## OAuth2 Support

### Supported Connectors

| Connector | OAuth2 |
|-----------|--------|
| **RestApiConnector** | ✓ |
| **RestApiDatasource** | ✓ |
| **RestApiWorkflowDatasource** | ✓ |
| **JdbcConnector** | ✗ |
| **AutoLoaderConnector** | ✗ (uses IAM) |

See: [../oauth2_token_manager.md](../oauth2_token_manager.md)

## Delta Live Tables Integration

### DLT Datasources

| Datasource | Purpose | Location |
|-----------|---------|----------|
| **RestApiDatasource** | REST API for DLT tables | [rest_api_datasource.md](./rest_api_datasource.md) |
| **RestApiWorkflowDatasource** | REST API workflow variant | [rest_api_workflow_datasource.md](./rest_api_workflow_datasource.md) |

## Usage Patterns

### Pattern 1: One-Time Load

```python
connector = ConnectorFactory.create_connector("rest_api", config, centralized)
df = connector.load(spark)
df.write.mode("overwrite").parquet("/path")
connector.close()
```

### Pattern 2: Scheduled Pipeline

```python
@dlt.table
def api_data():
    connector = ConnectorFactory.create_connector("rest_api", config, centralized)
    return connector.load(spark)
```

### Pattern 3: Incremental Load

```python
@dlt.table
def autoloader_data():
    return spark.readStream.format("cloudFiles").load(...)
```

### Pattern 4: Partitioned Load

```python
strategy = DateRangePartitionStrategy(...)
config.partition_strategy = strategy
connector = ConnectorFactory.create_connector("rest_api", config, centralized)
df = connector.load(spark)  # Loads in parallel
```

## Performance Optimization

### Tips by Connector

#### REST API Connector
- Use partitioning for large datasets
- Enable token caching for repeated calls
- Set appropriate timeouts
- Implement exponential backoff for retries

#### JDBC Connector
- Use partitioning to parallelize reads
- Optimize query with WHERE clauses
- Consider connection pooling
- Monitor memory for large result sets

#### AutoLoader Connector
- Enable schema inference
- Use schema_location for evolution
- Leverage incremental reads
- Consider file partitioning in cloud storage

## Best Practices

### Configuration

✓ Use builders for type-safe configuration
✓ Store secrets in SecretResolver
✓ Validate configuration before loading
✓ Use appropriate timeouts

✗ Don't hardcode credentials
✗ Don't use overly large timeouts
✗ Don't skip validation
✗ Don't reuse token managers incorrectly

### Data Loading

✓ Always validate before loading
✓ Handle errors appropriately
✓ Clean up resources (close)
✓ Use partitioning for large data

✗ Don't load without schema when possible
✗ Don't ignore errors
✗ Don't forget to close connectors
✗ Don't skip validation

### Testing

✓ Use DataFrameConnector for test data
✓ Mock external connections in tests
✓ Test error cases
✓ Validate schema handling

✗ Don't test against live APIs
✗ Don't hardcode test data
✗ Don't skip error testing
✗ Don't use production credentials

## Related Documentation

### Core Connectors

- [../README.md](../README.md) - Connectors overview
- [../base_connector.md](../base_connector.md) - Base class
- [../connector_factory.md](../connector_factory.md) - Factory

### Support Classes

- [../oauth2_token_manager.md](../oauth2_token_manager.md) - OAuth2
- [../partition_strategies.md](../partition_strategies.md) - Partitioning
- [../pyspark_datasource_adapter.md](../pyspark_datasource_adapter.md) - PySpark integration

### Configuration

- [../../configuration/README.md](../../configuration/README.md) - Configuration system
- [../../configuration/connector_config.md](../../configuration/connector_config.md) - ConnectorConfig
- [../../configuration/builders/](../../configuration/builders/) - Builders

## Quick Links

- [REST API Connector](./rest_api_connector.md) - REST API loading
- [JDBC Connector](./jdbc_connector.md) - Database loading
- [AutoLoader Connector](./autoloader_connector.md) - Cloud files
- [REST API for DLT](./rest_api_datasource.md) - DLT integration

## See Also

- [../README.md](../README.md) - Main connectors documentation
- [../QUICK_REFERENCE.md](../QUICK_REFERENCE.md) - Quick examples
- [../ARCHITECTURE.md](../ARCHITECTURE.md) - Design patterns
- [../INDEX.md](../INDEX.md) - Full index
