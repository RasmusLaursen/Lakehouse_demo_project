# Connectors

## Overview

The connector framework provides abstracted interfaces for loading data from various sources into the Lakehouse. Each connector encapsulates source-specific logic while conforming to a standard interface.

## Structure

```
src/framework/connectors/
├── __init__.py                              # Main exports
├── base_connector.py                        # BaseConnector abstract class
├── connector_factory.py                     # ConnectorFactory for instantiation
├── dataframe_connector.py                   # DataFrame connector
├── autoloader_connector.py                  # Databricks AutoLoader
├── jdbc_connector.py                        # JDBC database connector
├── rest_api_connector.py                    # REST API connector (direct)
├── oauth2_token_manager.py                  # OAuth2 token management
├── partition_strategies.py                  # Partition strategy implementations
├── pyspark_datasource_adapter.py           # PySpark datasource adapter
├── rest_api_datasource.py                   # REST API datasource (DLT)
├── rest_api_workflow_datasource.py         # REST API workflow datasource
└── datasources/                             # PySpark datasource implementations
    └── (various datasource files)
```

## Core Components

### 1. **BaseConnector** (`base_connector.py`)
Abstract base class defining the connector interface.

- **Purpose**: Establishes contract all connectors must follow
- **Key Methods**: load, validate, close
- **Responsibility**: Define standardized data loading interface

See: [base_connector.md](./base_connector.md)

### 2. **ConnectorFactory** (`connector_factory.py`)
Factory for creating appropriate connector instances.

- **Purpose**: Instantiate correct connector based on type
- **Responsibility**: Centralize connector instantiation
- **Pattern**: Factory Pattern

See: [connector_factory.md](./connector_factory.md)

### 3. **Specialized Connectors**

#### DataFrameConnector
Direct Spark DataFrame loading.
See: [dataframe_connector.md](./dataframe_connector.md)

#### AutoLoaderConnector
Databricks AutoLoader for cloud file ingestion.
See: [autoloader_connector.md](./autoloader_connector.md)

#### JdbcConnector
JDBC-based database connections.
See: [jdbc_connector.md](./jdbc_connector.md)

#### RestApiConnector
Direct REST API data loading.
See: [rest_api_connector.md](./rest_api_connector.md)

### 4. **Support Classes**

#### OAuth2TokenManager
Handles OAuth2 token management for REST APIs.
See: [oauth2_token_manager.md](./oauth2_token_manager.md)

#### PartitionStrategies
Various partition strategy implementations.
See: [partition_strategies.md](./partition_strategies.md)

#### PySparkDatasourceAdapter
Adapter for PySpark datasource integration.
See: [pyspark_datasource_adapter.md](./pyspark_datasource_adapter.md)

### 5. **Datasources** (DLT Integration)

REST API datasources for Delta Live Tables:
- RestApiDatasource
- RestApiWorkflowDatasource

See: [datasources/README.md](./datasources/README.md)

## Connector Types

| Type | Class | Purpose |
|------|-------|---------|
| **dataframe** | DataFrameConnector | Direct Spark DataFrame |
| **autoloader** | AutoLoaderConnector | Cloud file ingestion |
| **jdbc** | JdbcConnector | Database via JDBC |
| **rest_api** | RestApiConnector | REST API direct loading |
| **rest_api_ds** | RestApiDatasource | REST API for DLT |
| **rest_api_workflow_ds** | RestApiWorkflowDatasource | REST API workflow for DLT |

## Design Patterns

### Factory Pattern
`ConnectorFactory` creates appropriate connector based on type.

### Template Method Pattern
`BaseConnector` defines algorithm structure, subclasses implement specifics.

### Strategy Pattern
Different connectors implement different loading strategies.

## Usage Example

```python
from src.framework.connectors import ConnectorFactory
from src.framework.config import ConnectorConfig, CentralizedPipelineConfig

# Create configurations
centralized = CentralizedPipelineConfig(...)
connector_config = ConnectorConfig("rest_api", {...})

# Create connector via factory
connector = ConnectorFactory.create_connector(
    "rest_api",
    connector_config,
    centralized
)

# Load data
df = connector.load(spark, schema)
```

## Integration with Configuration

Connectors work with the configuration system:

1. **Configuration Building**: Uses `ConnectorConfigBuilderFactory`
2. **Final Config**: Passes to `ConnectorFactory`
3. **Connector Creation**: Factory instantiates correct connector
4. **Data Loading**: Connector loads data using configuration

## Common Methods

All connectors inherit from `BaseConnector`:

```python
class BaseConnector:
    def load(self, spark, schema=None):
        """Load data from source."""
        pass
    
    def validate(self):
        """Validate connector is properly configured."""
        pass
    
    def close(self):
        """Close connector and clean up resources."""
        pass
```

## Documentation Files

### Core Classes
- [base_connector.md](./base_connector.md)
- [connector_factory.md](./connector_factory.md)
- [dataframe_connector.md](./dataframe_connector.md)
- [autoloader_connector.md](./autoloader_connector.md)
- [jdbc_connector.md](./jdbc_connector.md)
- [rest_api_connector.md](./rest_api_connector.md)

### Support Classes
- [oauth2_token_manager.md](./oauth2_token_manager.md)
- [partition_strategies.md](./partition_strategies.md)
- [pyspark_datasource_adapter.md](./pyspark_datasource_adapter.md)

### Datasources (DLT)
- [datasources/README.md](./datasources/README.md)
- [datasources/rest_api_datasource.md](./datasources/rest_api_datasource.md)
- [datasources/rest_api_workflow_datasource.md](./datasources/rest_api_workflow_datasource.md)

### Guides
- [ARCHITECTURE.md](./ARCHITECTURE.md)
- [QUICK_REFERENCE.md](./QUICK_REFERENCE.md)

## Extension Points

### Adding New Connector Type

1. Create class extending `BaseConnector`
2. Implement `load()` method
3. Implement `validate()` method
4. Register with `ConnectorFactory`
5. Write tests

See: [ADDING_NEW_CONNECTOR_TYPE.md](./ADDING_NEW_CONNECTOR_TYPE.md)

## Testing

See: `tests/unit/test_connectors.py`

```bash
# Run connector tests
python -m pytest tests/unit/test_connectors.py -v
```

## Related Documentation

- [Configuration System](../configuration/README.md)
- [Connector Framework](../CONNECTOR_FRAMEWORK.md)
- [Connector Quickstart](../CONNECTOR_QUICKSTART.md)

## See Also

- [INDEX.md](./INDEX.md) - Navigation guide
- [QUICK_REFERENCE.md](./QUICK_REFERENCE.md) - Quick lookup
- [ARCHITECTURE.md](./ARCHITECTURE.md) - Design patterns
