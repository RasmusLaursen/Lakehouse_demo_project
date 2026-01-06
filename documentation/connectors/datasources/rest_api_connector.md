# RestApiConnector

## Overview

`RestApiConnector` is a connector for loading data directly from REST API endpoints. It supports various authentication methods, custom headers, query parameters, and partitioning strategies for large datasets.

**Location**: `src/framework/connectors/rest_api_connector.py`

**Extends**: `BaseConnector`

**Type**: `"rest_api"`

## Key Features

- **Multiple Auth Methods**: API Key, OAuth2, Bearer Token, Basic Auth
- **Custom Headers**: Add any HTTP headers needed
- **Query Parameters**: Support for parameterized requests
- **Request Body**: Support for POST/PUT with body data
- **Pagination**: Handle paginated responses
- **Partitioning**: Parallel data loading via date or sequential ranges
- **Error Handling**: Automatic retries with exponential backoff
- **Rate Limiting**: Respect API rate limits

## Class Definition

```python
class RestApiConnector(BaseConnector):
    """Connector for REST API data sources."""
```

## Configuration

### Required Fields

| Field | Type | Description |
|-------|------|-------------|
| url | str | API endpoint URL |
| method | str | HTTP method (GET, POST, etc.) |

### Optional Fields

| Field | Type | Description |
|-------|------|-------------|
| headers | dict | Custom HTTP headers |
| query_params | dict | URL query parameters |
| body | dict/str | Request body for POST/PUT |
| auth_type | str | Authentication type (oauth2, bearer, basic, apikey) |
| auth_config | dict | Authentication configuration |
| timeout | int | Request timeout in seconds (default: 30) |
| retry_count | int | Number of retries (default: 3) |
| batch_size | int | Records per request (default: 1000) |
| partition_strategy | PartitionStrategy | Partitioning strategy |

## Core Methods

### validate()

Validate connector configuration.

```python
def validate(self) -> None:
    """
    Validate REST API connector configuration.
    
    Raises:
        ConfigurationException: If configuration is invalid
    """
```

**Validation Checks**:
- URL format valid
- HTTP method valid
- Authentication config complete
- Required parameters present

### load()

Load data from REST API.

```python
def load(self, spark: SparkSession, schema=None) -> DataFrame:
    """
    Load data from REST API.
    
    Args:
        spark: SparkSession instance
        schema: Optional schema for data
        
    Returns:
        DataFrame with API response data
        
    Raises:
        ConnectorException: If load fails
    """
```

**Parameters**:
- `spark` (SparkSession): Active Spark session
- `schema` (optional): StructType schema

**Returns**: `DataFrame` - API response data

### close()

Clean up resources.

```python
def close(self) -> None:
    """Close connector and clean up resources."""
```

## Usage Examples

### Basic REST API Call

```python
from src.framework.connectors import ConnectorFactory
from src.framework.config import ConnectorConfig, CentralizedPipelineConfig

# Create configuration
connector_config = ConnectorConfig(
    connector_type="rest_api",
    config={
        "url": "https://api.example.com/data",
        "method": "GET"
    }
)

centralized = CentralizedPipelineConfig(
    source_name="example_api",
    target_schema="bronze"
)

# Create connector
connector = ConnectorFactory.create_connector(
    "rest_api",
    connector_config,
    centralized
)

# Load data
try:
    connector.validate()
    df = connector.load(spark)
    df.show()
finally:
    connector.close()
```

### With Query Parameters

```python
connector_config = ConnectorConfig(
    connector_type="rest_api",
    config={
        "url": "https://api.example.com/data",
        "method": "GET",
        "query_params": {
            "limit": 1000,
            "offset": 0,
            "filter": "status=active"
        }
    }
)
```

### With Custom Headers

```python
connector_config = ConnectorConfig(
    connector_type="rest_api",
    config={
        "url": "https://api.example.com/data",
        "method": "GET",
        "headers": {
            "User-Agent": "Lakehouse/1.0",
            "Accept": "application/json",
            "X-Custom-Header": "value"
        }
    }
)
```

### With OAuth2 Authentication

```python
connector_config = ConnectorConfig(
    connector_type="rest_api",
    config={
        "url": "https://api.example.com/data",
        "method": "GET",
        "auth_type": "oauth2",
        "auth_config": {
            "client_id": "YOUR_CLIENT_ID",
            "client_secret": "YOUR_CLIENT_SECRET",
            "token_url": "https://oauth.example.com/token",
            "scopes": ["data:read"]
        }
    }
)
```

### With API Key Authentication

```python
connector_config = ConnectorConfig(
    connector_type="rest_api",
    config={
        "url": "https://api.example.com/data",
        "method": "GET",
        "auth_type": "apikey",
        "auth_config": {
            "header_name": "X-API-Key",
            "api_key": "YOUR_API_KEY"
        }
    }
)
```

### With Basic Authentication

```python
connector_config = ConnectorConfig(
    connector_type="rest_api",
    config={
        "url": "https://api.example.com/data",
        "method": "GET",
        "auth_type": "basic",
        "auth_config": {
            "username": "user",
            "password": "password"
        }
    }
)
```

### With POST Request

```python
connector_config = ConnectorConfig(
    connector_type="rest_api",
    config={
        "url": "https://api.example.com/query",
        "method": "POST",
        "headers": {
            "Content-Type": "application/json"
        },
        "body": {
            "query": "SELECT * FROM users WHERE active = true"
        }
    }
)
```

### With Partitioning

```python
from src.framework.connectors.partition_strategies import DateRangePartitionStrategy

strategy = DateRangePartitionStrategy(
    start_date="2024-01-01",
    end_date="2024-12-31",
    partition_interval="month"
)

connector_config = ConnectorConfig(
    connector_type="rest_api",
    config={
        "url": "https://api.example.com/data",
        "method": "GET",
        "query_params": {
            "date_from": "{partition_start}",
            "date_to": "{partition_end}"
        },
        "partition_strategy": strategy
    }
)
```

## Configuration Builder

### Using Builder Pattern

```python
from src.framework.config import ConnectorConfigBuilderFactory

# Create builder
builder = ConnectorConfigBuilderFactory.create_builder("rest_api")

# Configure
connector_config = (builder
    .with_url("https://api.example.com/data")
    .with_method("GET")
    .with_auth_type("oauth2")
    .with_auth_config({
        "client_id": "...",
        "client_secret": "...",
        "token_url": "..."
    })
    .with_query_params({"limit": 1000})
    .build()
)

# Create connector
connector = ConnectorFactory.create_connector(
    "rest_api",
    connector_config,
    centralized
)
```

## Authentication Types

### OAuth2

```python
"auth_type": "oauth2",
"auth_config": {
    "client_id": "client_id",
    "client_secret": "client_secret",
    "token_url": "https://oauth.provider.com/token",
    "scopes": ["read", "data"]
}
```

### Bearer Token

```python
"auth_type": "bearer",
"auth_config": {
    "token": "YOUR_BEARER_TOKEN"
}
```

### Basic Auth

```python
"auth_type": "basic",
"auth_config": {
    "username": "user",
    "password": "password"
}
```

### API Key

```python
"auth_type": "apikey",
"auth_config": {
    "header_name": "X-API-Key",
    "api_key": "YOUR_API_KEY"
}
```

## Error Handling

### Connection Errors

```python
try:
    df = connector.load(spark)
except ConnectorException as e:
    if "connection" in str(e).lower():
        print("Failed to connect to API")
```

### Authentication Errors

```python
try:
    connector.validate()
except ConfigurationException as e:
    if "auth" in str(e).lower():
        print("Authentication configuration invalid")
```

### Rate Limiting

```python
# Connector handles rate limiting with exponential backoff
# Retries automatically up to retry_count
connector_config = ConnectorConfig(
    connector_type="rest_api",
    config={
        "url": "...",
        "method": "GET",
        "retry_count": 5,  # Retries for rate limits
        "timeout": 30
    }
)
```

## Performance Considerations

### 1. **Batch Size**

```python
# Adjust batch size based on API response size
connector_config = ConnectorConfig(
    connector_type="rest_api",
    config={
        "url": "...",
        "batch_size": 1000  # Records per request
    }
)
```

### 2. **Timeout Configuration**

```python
# Set appropriate timeout for API response time
connector_config = ConnectorConfig(
    connector_type="rest_api",
    config={
        "url": "...",
        "timeout": 60  # Seconds
    }
)
```

### 3. **Partitioning Strategy**

```python
# Use partitioning for parallel loads
strategy = DateRangePartitionStrategy(
    start_date="2024-01-01",
    end_date="2024-12-31",
    partition_interval="month"
)

connector_config = ConnectorConfig(
    connector_type="rest_api",
    config={
        "url": "https://api.example.com/data",
        "partition_strategy": strategy
    }
)
```

### 4. **Token Caching**

```python
# OAuth2 tokens are automatically cached
# Manager refreshes before expiry
# This reduces authentication overhead
```

## Testing

### Test Patterns

```python
class TestRestApiConnector:
    def test_load_returns_dataframe(self):
        """Test connector returns DataFrame."""
        connector = ConnectorFactory.create_connector(
            "rest_api",
            rest_api_config,
            centralized
        )
        
        df = connector.load(spark)
        
        assert df is not None
        assert isinstance(df, DataFrame)
    
    def test_validate_fails_with_invalid_url(self):
        """Test validation fails with invalid URL."""
        invalid_config = ConnectorConfig(
            "rest_api",
            {"url": "not_a_url", "method": "GET"}
        )
        
        connector = ConnectorFactory.create_connector(
            "rest_api", invalid_config, centralized
        )
        
        with pytest.raises(ConfigurationException):
            connector.validate()
    
    def test_auth_configuration(self):
        """Test authentication is configured correctly."""
        oauth_config = ConnectorConfig(
            "rest_api",
            {
                "url": "...",
                "method": "GET",
                "auth_type": "oauth2",
                "auth_config": {...}
            }
        )
        
        connector = ConnectorFactory.create_connector(
            "rest_api", oauth_config, centralized
        )
        
        connector.validate()  # Should not raise
```

## Using Secrets

### Store Credentials in SecretResolver

```python
from src.framework.config import SecretResolver

secret_resolver = SecretResolver(centralized_config)

connector_config = ConnectorConfig(
    connector_type="rest_api",
    config={
        "url": "https://api.example.com/data",
        "auth_type": "oauth2",
        "auth_config": {
            "client_id": secret_resolver.resolve("oauth_client_id"),
            "client_secret": secret_resolver.resolve("oauth_client_secret"),
            "token_url": "https://oauth.example.com/token"
        }
    }
)
```

See: [../../configuration/secret_resolver.md](../../configuration/secret_resolver.md)

## Related Classes

- [BaseConnector](../base_connector.md) - Abstract base class
- [ConnectorFactory](../connector_factory.md) - Factory for creation
- [OAuth2TokenManager](../oauth2_token_manager.md) - Token management
- [PartitionStrategies](../partition_strategies.md) - Partitioning
- [RestApiDatasource](./rest_api_datasource.md) - DLT variant

## See Also

- [../QUICK_REFERENCE.md](../QUICK_REFERENCE.md) - Quick examples
- [../ARCHITECTURE.md](../ARCHITECTURE.md) - Design patterns
- [../README.md](../README.md) - Connectors overview
- [README.md](./README.md) - Datasources overview
