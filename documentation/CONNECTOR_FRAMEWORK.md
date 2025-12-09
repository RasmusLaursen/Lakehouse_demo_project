# Connector Framework

## Overview

The connector framework provides a flexible, extensible architecture for ingesting data from various sources into the Lakehouse. Instead of hardcoding data source logic into pipelines, connectors encapsulate source-specific reading logic while maintaining a consistent interface.

## Architecture

### Core Components

1. **BaseConnector** (`base_connector.py`)
   - Abstract base class defining the connector interface
   - All connectors must implement: `read_stream()`, `read_batch()`, `validate_config()`

2. **ConnectorFactory** (`connector_factory.py`)
   - Registry pattern for creating connector instances
   - Maps connector type strings to connector classes
   - Auto-registration of connectors via `__init__.py`

3. **PipelineConfig** (`pipelines/config.py`)
   - Extended with `connector_type` and `connector_config` fields
   - `get_connector()` method creates connector instances
   - Parses connector config from data contracts

4. **Data Contracts** (`data_contracts/source_system/*.yml`)
   - Extended with connector configuration in `servers.customProperties`
   - Specifies connector type and connector-specific settings

## Available Connectors

### 1. VolumeConnector

Read data from Databricks Unity Catalog volumes.

**Type**: `volume`, `volume_autoloader`

**Configuration**:
```yaml
servers:
  - server: dev
    type: "databricks"
    environment: "development"
    format: "parquet"
    customProperties:
      - property: connector_type
        value: "volume"
      - property: connector_config
        value:
          add_audit_columns: true
```

**Features**:
- Streaming via cloudFiles/Auto Loader
- Batch reading
- Automatic audit column injection
- Support for all file formats (parquet, json, csv, avro, etc.)

**Required Config**:
- `catalog`: Unity Catalog catalog name
- `schema`: Schema name
- `volume`: Volume name
- `format`: File format

**Optional Config**:
- `add_audit_columns`: Add audit metadata (default: false)
- `path`: Subfolder within volume (default: "")
- `options`: Additional Spark reader options

### 2. RestApiConnector

Read data from REST APIs with authentication, pagination, and rate limiting.

**Type**: `rest_api`, `http`, `https`

**Configuration**:
```yaml
servers:
  - server: dev
    type: "rest_api"
    environment: "development"
    customProperties:
      - property: connector_type
        value: "rest_api"
      - property: connector_config
        value:
          endpoint: "https://api.example.com/v1/customers"
          method: "GET"
          auth_type: "bearer"
          auth_token: "${secrets.api_token}"
          pagination_type: "offset"
          pagination_config:
            limit: 100
            offset_param: "offset"
            limit_param: "limit"
          data_path: "data.items"
          rate_limit_delay: 0.5
```

**Features**:
- Multiple authentication methods (Bearer, API Key, OAuth, Basic)
- Pagination support (offset-based, cursor-based, page-based)
- Rate limiting
- JSON path extraction for nested data
- Custom headers and query parameters

**Required Config**:
- `endpoint`: API base URL
- `method`: HTTP method (GET, POST, PUT, PATCH)

**Optional Config**:
- `auth_type`: bearer, api_key, oauth, basic, none
- `auth_token`: Token for Bearer/API Key auth
- `auth_header`: Header name for API Key (default: "X-API-Key")
- `headers`: Custom HTTP headers
- `params`: Query parameters
- `pagination_type`: offset, cursor, page, none
- `pagination_config`: Pagination settings
- `data_path`: JSON path to data (e.g., "data.items")
- `rate_limit_delay`: Delay between requests in seconds
- `timeout`: Request timeout (default: 30)
- `add_audit_columns`: Add audit metadata

**Pagination Types**:

1. **Offset-based**:
```yaml
pagination_type: "offset"
pagination_config:
  start_offset: 0
  limit: 100
  offset_param: "offset"
  limit_param: "limit"
```

2. **Cursor-based**:
```yaml
pagination_type: "cursor"
pagination_config:
  cursor_param: "cursor"
  cursor_path: "pagination.next_cursor"
```

3. **Page-based**:
```yaml
pagination_type: "page"
pagination_config:
  start_page: 1
  page_size: 100
  page_param: "page"
  size_param: "size"
```

### 3. JdbcConnector

Read data from relational databases.

**Type**: `jdbc`, `database`

**Configuration**:
```yaml
servers:
  - server: dev
    type: "jdbc"
    environment: "development"
    customProperties:
      - property: connector_type
        value: "jdbc"
      - property: connector_config
        value:
          url: "jdbc:postgresql://db.example.com:5432/mydb"
          table: "customers"
          user: "${secrets.db_user}"
          password: "${secrets.db_password}"
          partition_column: "customer_id"
          lower_bound: 1
          upper_bound: 1000000
          num_partitions: 10
```

**Features**:
- Auto-detection of JDBC drivers (PostgreSQL, MySQL, SQL Server, Oracle, DB2)
- Partitioned reading for large tables
- Incremental loading with watermark columns
- Custom SQL queries
- Connection pooling options

**Required Config**:
- `url`: JDBC connection URL
- `table`: Table name or SQL query (wrap queries in parentheses)

**Optional Config**:
- `driver`: JDBC driver class (auto-detected if not provided)
- `user`: Database username
- `password`: Database password
- `properties`: Additional JDBC connection properties
- `partition_column`: Column for parallel reading
- `lower_bound`: Lower bound for partition column
- `upper_bound`: Upper bound for partition column
- `num_partitions`: Number of partitions
- `fetch_size`: JDBC fetch size
- `query_timeout`: Query timeout in seconds
- `incremental_column`: Column for incremental loading (e.g., updated_at)
- `incremental_value`: Last processed value
- `add_audit_columns`: Add audit metadata

**Supported Databases**:
- PostgreSQL: `jdbc:postgresql://host:port/database`
- MySQL: `jdbc:mysql://host:port/database`
- SQL Server: `jdbc:sqlserver://host:port;database=dbname`
- Oracle: `jdbc:oracle:thin:@host:port:SID`
- DB2: `jdbc:db2://host:port/database`

## Usage

### 1. Define Connector in Data Contract

Update `data_contracts/source_system/<system>.yml`:

```yaml
servers:
  - server: dev
    type: "databricks"
    environment: "development"
    format: "parquet"
    customProperties:
      - property: connector_type
        value: "volume"  # or "rest_api", "jdbc", etc.
      - property: connector_config
        value:
          # Connector-specific configuration
          add_audit_columns: true
```

### 2. Automatic Pipeline Integration

The `RawPipelineFactory` automatically:
1. Loads connector config from data contract
2. Creates connector instance via `config.get_connector()`
3. Passes connector to `ldp_table()` for data reading

No code changes needed in existing pipelines!

### 3. Manual Connector Usage

For custom pipelines:

```python
from pyspark.sql import SparkSession
from src.framework.connectors import ConnectorFactory

# Create connector
config = {
    "catalog": "landing",
    "schema": "my_schema",
    "volume": "my_data",
    "format": "json"
}
connector = ConnectorFactory.create("volume", config)

# Read data
spark = SparkSession.builder.getOrCreate()
df = connector.read_stream(spark)  # or read_batch(spark)
```

## Extending with New Connectors

### 1. Create Connector Class

Create `src/framework/connectors/my_connector.py`:

```python
from typing import Dict, Any
from pyspark.sql import SparkSession, DataFrame
from src.framework.connectors.base_connector import BaseConnector
from src.framework.helper import logging_helper

logger = logging_helper.get_logger(__name__)

class MyConnector(BaseConnector):
    """Connector for my custom data source."""
    
    def validate_config(self, config: Dict[str, Any]) -> None:
        """Validate configuration."""
        required = ["field1", "field2"]
        missing = [f for f in required if f not in config]
        if missing:
            raise ValueError(f"Missing required fields: {', '.join(missing)}")
    
    def read_stream(self, spark: SparkSession) -> DataFrame:
        """Read streaming data."""
        # Implement streaming logic
        pass
    
    def read_batch(self, spark: SparkSession) -> DataFrame:
        """Read batch data."""
        # Implement batch reading logic
        pass
```

### 2. Register Connector

Add to `src/framework/connectors/__init__.py`:

```python
from src.framework.connectors.my_connector import MyConnector

# Register
ConnectorFactory.register("my_type", MyConnector)

# Update __all__
__all__ = [
    # ... existing exports ...
    "MyConnector",
]
```

### 3. Use in Data Contracts

```yaml
customProperties:
  - property: connector_type
    value: "my_type"
  - property: connector_config
    value:
      field1: "value1"
      field2: "value2"
```

## Testing

Run connector tests:

```bash
python tests/unit/test_connectors.py
```

Tests validate:
- ✓ Connector registration
- ✓ Configuration validation
- ✓ Factory pattern
- ✓ Type aliases
- ✓ PipelineConfig integration

## Migration Guide

### From Legacy Volume Ingestion

**Before** (hardcoded in pipeline):
```python
df = read.read_volume_autoloader(
    source_catalog="landing",
    source_schema="lakehouse_landing",
    objectname="customer_contract",
    filetype="parquet",
    add_audit_column=True
)
```

**After** (connector-based):
```yaml
# In data contract
customProperties:
  - property: connector_type
    value: "volume"
  - property: connector_config
    value:
      add_audit_columns: true
```

Pipeline code unchanged - factory handles it automatically!

### Adding REST API Source

1. Add data contract:
```yaml
servers:
  - server: dev
    type: "rest_api"
    customProperties:
      - property: connector_type
        value: "rest_api"
      - property: connector_config
        value:
          endpoint: "https://api.external.com/customers"
          method: "GET"
          auth_type: "bearer"
          auth_token: "${secrets.external_api_token}"
          pagination_type: "offset"
```

2. No pipeline code changes needed!

## Best Practices

1. **Use Data Contracts**: Always configure connectors via data contracts, not hardcoded in pipelines
2. **Secret Management**: Use `${secrets.key}` for credentials in YAML
3. **Partition Large Tables**: Use JDBC partitioning for tables >1M rows
4. **Rate Limiting**: Configure API rate limits to respect service quotas
5. **Incremental Loading**: Use watermark columns for large datasets
6. **Test Connectors**: Validate configuration before deployment
7. **Error Handling**: Connectors log errors - monitor logs for issues

## Troubleshooting

### "Unknown connector type" Error
- Check connector is registered in `__init__.py`
- Verify spelling of connector_type in data contract

### "Missing required config fields" Error
- Review connector documentation for required fields
- Check data contract YAML syntax (proper indentation)

### JDBC Connection Failures
- Verify JDBC driver is available in Spark classpath
- Check network connectivity to database
- Validate credentials

### API Rate Limiting
- Increase `rate_limit_delay` in connector_config
- Implement exponential backoff for production

## Future Enhancements

Planned connectors:
- **KafkaConnector**: Real-time streaming from Kafka topics
- **S3Connector**: Direct S3 reading (alternative to volumes)
- **DeltaSharingConnector**: Read from Delta Sharing providers
- **EventHubConnector**: Azure Event Hubs streaming

## Summary

The connector framework:
✅ Separates data source logic from business logic  
✅ Enables easy addition of new data sources  
✅ Maintains backward compatibility  
✅ Supports streaming and batch ingestion  
✅ Configured via data contracts (infrastructure as code)  
✅ Tested and production-ready
