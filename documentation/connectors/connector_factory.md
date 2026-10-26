# ConnectorFactory

## Overview

`ConnectorFactory` is the factory for creating connector instances. It uses the Factory Pattern to instantiate the appropriate connector based on connector type and configuration.

**Location**: `src/framework/connectors/connector_factory.py`

**Pattern**: Factory Pattern

## Class Definition

```python
class ConnectorFactory:
    """Factory for creating connector instances."""
```

## Key Responsibilities

1. **Connector Instantiation**: Create appropriate connector based on type
2. **Type Validation**: Verify supported connector type
3. **Configuration Passing**: Pass configs to connector constructor
4. **Error Handling**: Clear error messages for unsupported types

## Factory Method

### create_connector()

Primary factory method for creating connectors.

```python
@staticmethod
def create_connector(
    connector_type: str,
    connector_config: ConnectorConfig,
    centralized_config: CentralizedPipelineConfig
) -> BaseConnector:
    """
    Create connector of specified type.
    
    Args:
        connector_type: Type of connector ("rest_api", "jdbc", etc.)
        connector_config: Connector-specific configuration
        centralized_config: Shared pipeline configuration
        
    Returns:
        Instantiated connector
        
    Raises:
        ValueError: If connector type is not supported
    """
```

**Parameters**:
- `connector_type` (str): Connector type identifier
- `connector_config` (ConnectorConfig): Type-specific config
- `centralized_config` (CentralizedPipelineConfig): Shared metadata

**Returns**: `BaseConnector` - Instantiated connector subclass

**Raises**: `ValueError` - Unsupported connector type

## Supported Connector Types

| Type | Connector Class | Purpose |
|------|-----------------|---------|
| **dataframe** | DataFrameConnector | Direct Spark DataFrame |
| **rest_api** | RestApiConnector | REST API endpoint |
| **jdbc** | JdbcConnector | JDBC database |
| **autoloader** | AutoLoaderConnector | Databricks AutoLoader |
| **rest_api_ds** | RestApiDatasource | REST API for DLT |
| **rest_api_workflow_ds** | RestApiWorkflowDatasource | REST API workflow for DLT |

## Usage Examples

### Basic Usage

```python
from src.framework.connectors import ConnectorFactory
from src.framework.config import ConnectorConfig, CentralizedPipelineConfig

# Setup configurations
centralized = CentralizedPipelineConfig(
    source_name="my_source",
    target_schema="bronze"
)

connector_config = ConnectorConfig(
    connector_type="rest_api",
    config={
        "url": "https://api.example.com",
        "method": "GET"
    }
)

# Create connector via factory
connector = ConnectorFactory.create_connector(
    "rest_api",
    connector_config,
    centralized
)
```

### With Configuration Builder

```python
from src.framework.config import ConnectorConfigBuilderFactory

# Build config first
builder = ConnectorConfigBuilderFactory.create_builder("rest_api")
connector_config = (builder
    .with_url("https://api.example.com")
    .with_method("GET")
    .build()
)

# Then create connector
connector = ConnectorFactory.create_connector(
    "rest_api",
    connector_config,
    centralized
)
```

### Loading Data

```python
try:
    # Validate
    connector.validate()
    
    # Load
    df = connector.load(spark)
    
    # Use data
    df.show()
    
finally:
    # Cleanup
    connector.close()
```

## Connector Mapping

### How the Factory Maps Types

The factory maintains internal mapping:

```python
CONNECTOR_TYPES = {
    "dataframe": DataFrameConnector,
    "rest_api": RestApiConnector,
    "jdbc": JdbcConnector,
    "autoloader": AutoLoaderConnector,
    "rest_api_ds": RestApiDatasource,
    "rest_api_workflow_ds": RestApiWorkflowDatasource,
}
```

The factory:
1. Validates connector_type exists in mapping
2. Gets connector class from mapping
3. Instantiates class with both configs
4. Returns instantiated connector

## Error Handling

### Unsupported Type

```python
try:
    connector = ConnectorFactory.create_connector(
        "invalid_type",  # Not in supported types
        connector_config,
        centralized
    )
except ValueError as e:
    print(f"Error: {e}")
    # Output: "Unsupported connector type: invalid_type"
```

### Getting Supported Types

```python
supported = ConnectorFactory.get_supported_types()
# Returns: ["dataframe", "rest_api", "jdbc", "autoloader", "rest_api_ds", "rest_api_workflow_ds"]
```

## Integration with Configuration System

### Builder + Factory Pattern

```python
# Step 1: Create builder
builder = ConnectorConfigBuilderFactory.create_builder("rest_api")

# Step 2: Configure via builder
connector_config = (builder
    .with_url("https://api.example.com")
    .with_auth("oauth2")
    .with_method("GET")
    .build()
)

# Step 3: Create connector via factory
connector = ConnectorFactory.create_connector(
    "rest_api",
    connector_config,
    centralized_config
)

# Step 4: Use connector
df = connector.load(spark)
```

This pattern ensures:
- Type-safe configuration building
- Validated connector creation
- Clean separation of concerns

## Design Pattern Details

### Factory Pattern Benefits

1. **Encapsulation**: Connector creation logic centralized
2. **Extensibility**: Easy to add new connector types
3. **Type Safety**: Validates type before instantiation
4. **Decoupling**: Clients don't depend on specific connector classes

### Extension Pattern

To add a new connector type:

```python
# 1. Create connector class
class MyCustomConnector(BaseConnector):
    def validate(self): ...
    def load(self, spark, schema=None): ...
    def close(self): ...

# 2. Register with factory
ConnectorFactory.register_connector_type(
    "my_custom",
    MyCustomConnector
)

# 3. Create via factory
connector = ConnectorFactory.create_connector(
    "my_custom",
    connector_config,
    centralized
)
```

## Testing Patterns

### Test Factory

```python
class TestConnectorFactory:
    def test_create_rest_api_connector(self):
        """Test creating REST API connector."""
        connector = ConnectorFactory.create_connector(
            "rest_api",
            rest_api_config,
            centralized
        )
        assert isinstance(connector, RestApiConnector)
    
    def test_unsupported_type_raises_error(self):
        """Test unsupported type raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            ConnectorFactory.create_connector(
                "unsupported",
                config,
                centralized
            )
        assert "Unsupported connector type" in str(exc_info.value)
```

## Supported Connector Types

### 1. DataFrameConnector

Direct Spark DataFrame loading.

```python
connector = ConnectorFactory.create_connector(
    "dataframe",
    config,
    centralized
)
```

See: [dataframe_connector.md](./dataframe_connector.md)

### 2. RestApiConnector

REST API endpoint data loading.

```python
connector = ConnectorFactory.create_connector(
    "rest_api",
    config,
    centralized
)
```

See: [datasources/rest_api_connector.md](./datasources/rest_api_connector.md)

### 3. JdbcConnector

JDBC-based database connections.

```python
connector = ConnectorFactory.create_connector(
    "jdbc",
    config,
    centralized
)
```

See: [datasources/jdbc_connector.md](./datasources/jdbc_connector.md)

### 4. AutoLoaderConnector

Databricks AutoLoader for cloud files.

```python
connector = ConnectorFactory.create_connector(
    "autoloader",
    config,
    centralized
)
```

See: [datasources/autoloader_connector.md](./datasources/autoloader_connector.md)

### 5. RestApiDatasource

REST API datasource for Delta Live Tables.

```python
connector = ConnectorFactory.create_connector(
    "rest_api_ds",
    config,
    centralized
)
```

See: [datasources/rest_api_datasource.md](./datasources/rest_api_datasource.md)

### 6. RestApiWorkflowDatasource

REST API workflow datasource for DLT.

```python
connector = ConnectorFactory.create_connector(
    "rest_api_workflow_ds",
    config,
    centralized
)
```

See: [datasources/rest_api_workflow_datasource.md](./datasources/rest_api_workflow_datasource.md)

## Related Classes

- [BaseConnector](./base_connector.md) - Abstract base class
- [ConnectorConfig](../configuration/connector_config.md) - Type-specific configuration
- [CentralizedPipelineConfig](../configuration/centralized_config.md) - Shared configuration
- [ConnectorConfigBuilderFactory](../configuration/builders/builder_factory.md) - Builder factory

## See Also

- [ARCHITECTURE.md](./ARCHITECTURE.md) - Design patterns
- [QUICK_REFERENCE.md](./QUICK_REFERENCE.md) - Quick lookup
- [datasources/README.md](./datasources/README.md) - Datasource variants
