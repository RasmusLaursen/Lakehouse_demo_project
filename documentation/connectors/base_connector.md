# BaseConnector

## Overview

`BaseConnector` is the abstract base class that all connectors extend. It defines the standard interface and contracts that all data source connectors must follow within the Lakehouse framework.

**Location**: `src/framework/connectors/base_connector.py`

**Pattern**: Template Method Pattern (structure defined, subclasses implement details)

## Class Definition

```python
class BaseConnector(ABC):
    """Abstract base class for all connectors."""
```

## Key Responsibilities

1. **Interface Definition**: Define contract for all connectors
2. **Lifecycle Management**: Load, validate, close operations
3. **Configuration Handling**: Store connector and centralized configs
4. **Error Handling**: Standardized exception handling

## Constructor

```python
def __init__(self, connector_config: ConnectorConfig, 
             centralized_config: CentralizedPipelineConfig):
    """
    Initialize connector.
    
    Args:
        connector_config: Connector-specific configuration
        centralized_config: Shared pipeline configuration
    """
    self.connector_config = connector_config
    self.centralized_config = centralized_config
```

**Parameters**:
- `connector_config` (ConnectorConfig): Type-specific configuration
- `centralized_config` (CentralizedPipelineConfig): Shared metadata

## Abstract Methods

### load()

Load data from source.

```python
@abstractmethod
def load(self, spark: SparkSession, schema=None) -> DataFrame:
    """
    Load data from source into DataFrame.
    
    Args:
        spark: SparkSession instance
        schema: Optional schema for data
        
    Returns:
        PySpark DataFrame
        
    Raises:
        ConnectorException: If load fails
    """
    pass
```

**Purpose**: Main data loading method - subclasses implement source-specific logic

**Parameters**:
- `spark` (SparkSession): Active Spark session
- `schema` (optional): StructType schema for data validation

**Returns**: `pyspark.sql.DataFrame` - Loaded data

**Raises**: `ConnectorException` - Loading failed

### validate()

Validate connector configuration.

```python
@abstractmethod
def validate(self) -> None:
    """
    Validate connector configuration.
    
    Raises:
        ConfigurationException: If validation fails
    """
    pass
```

**Purpose**: Pre-load validation of connector setup

**Raises**: `ConfigurationException` - Config invalid

### close()

Clean up connector resources.

```python
@abstractmethod
def close(self) -> None:
    """
    Close connector and clean up resources.
    
    Raises:
        ConnectorException: If cleanup fails
    """
    pass
```

**Purpose**: Resource cleanup, connection closing

**Raises**: `ConnectorException` - Cleanup failed

## Provided Methods

### get_connector_type()

```python
def get_connector_type(self) -> str:
    """Get connector type."""
    return self.connector_config.connector_type
```

**Returns**: Connector type string (e.g., "rest_api")

### get_source_name()

```python
def get_source_name(self) -> str:
    """Get source name from configuration."""
    return self.centralized_config.source_name
```

**Returns**: Source system name

## Usage Example

### Implementing a New Connector

```python
from src.framework.connectors import BaseConnector
from pyspark.sql import DataFrame, SparkSession

class MyConnector(BaseConnector):
    """Custom connector implementation."""
    
    def validate(self) -> None:
        """Validate configuration."""
        if not self.connector_config.get("my_required_field"):
            raise ConfigurationException(
                "my_required_field is required"
            )
    
    def load(self, spark: SparkSession, schema=None) -> DataFrame:
        """Load data from custom source."""
        # Implementation specific to source
        data = self._fetch_data()
        return spark.createDataFrame(data, schema=schema)
    
    def close(self) -> None:
        """Clean up resources."""
        # Close connections
        pass
```

### Using a Connector

```python
from src.framework.connectors import ConnectorFactory

# Create via factory
connector = ConnectorFactory.create_connector(
    "my_type",
    connector_config,
    centralized_config
)

# Validate
connector.validate()

# Load data
df = connector.load(spark)

# Cleanup
connector.close()
```

## Configuration Access

Connectors access two configuration objects:

### 1. Connector-Specific Config

```python
# Access connector-specific settings
connection_string = self.connector_config.get("connection_string")
username = self.connector_config.get("username")
```

See: [ConnectorConfig](../configuration/connector_config.md)

### 2. Centralized Config

```python
# Access shared metadata
source_name = self.centralized_config.source_name
target_schema = self.centralized_config.target_schema
```

See: [CentralizedPipelineConfig](../configuration/centralized_config.md)

## Error Handling

Connectors should raise appropriate exceptions:

```python
from src.framework.exceptions import (
    ConnectorException,
    ConfigurationException,
    ValidationException
)

# Configuration error
raise ConfigurationException("Missing required field: api_key")

# Validation error
raise ValidationException("Invalid schema definition")

# Runtime error
raise ConnectorException("Failed to connect to source")
```

## Connector Lifecycle

```
create → validate → load → close
         ↓
    (if validation fails)
         ↓
      Exception
```

## Lifecycle Example

```python
try:
    connector = ConnectorFactory.create_connector(
        connector_type,
        connector_config,
        centralized_config
    )
    
    # Validate before loading
    connector.validate()
    
    # Load data
    df = connector.load(spark, schema)
    
    # Process data...
    
finally:
    # Always cleanup
    connector.close()
```

## Built-in Subclasses

| Subclass | Location | Purpose |
|----------|----------|---------|
| DataFrameConnector | `dataframe_connector.py` | Direct DataFrame |
| RestApiConnector | `rest_api_connector.py` | REST API |
| JdbcConnector | `jdbc_connector.py` | JDBC database |
| AutoLoaderConnector | `autoloader_connector.py` | Cloud files |

See individual class documentation for details.

## Design Considerations

### 1. **Abstract Methods**
All subclasses must implement load, validate, close.

### 2. **Configuration Injection**
Both configs injected at construction - immutable after.

### 3. **Spark Session Passed at Load**
Allows multiple Spark sessions if needed.

### 4. **Schema Optional**
Supports both schema-defined and schema-inferred loading.

### 5. **Resource Management**
Close method handles cleanup - encourage try/finally usage.

## Testing

Base test patterns:

```python
class TestBaseConnector:
    def test_must_implement_load(self):
        """Verify abstract method enforcement."""
        with pytest.raises(TypeError):
            BaseConnector(config, centralized)
    
    def test_subclass_implementation(self):
        """Verify subclass properly extends base."""
        connector = MyConnector(config, centralized)
        assert isinstance(connector, BaseConnector)
```

## Related Classes

- [ConnectorConfig](../configuration/connector_config.md) - Type-specific config
- [CentralizedPipelineConfig](../configuration/centralized_config.md) - Shared config
- [ConnectorFactory](./connector_factory.md) - Factory for creation

## See Also

- [ARCHITECTURE.md](./ARCHITECTURE.md) - Design patterns
- [QUICK_REFERENCE.md](./QUICK_REFERENCE.md) - Quick lookup
- [dataframe_connector.md](./dataframe_connector.md) - Example implementation
