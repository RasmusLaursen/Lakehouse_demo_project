# JdbcConfigBuilder

## Location
`src/framework/config/builders/jdbc_config_builder.py`

## Purpose
Builder for JDBC connector configuration. Provides foundation for connecting to JDBC-based databases.

## Extends
`BaseConfigBuilder`

## Status
Currently a placeholder for future JDBC connector implementation.

## Methods

### `merge_shared_context()`
Add JDBC-specific context from centralized configuration.

```python
builder.merge_shared_context()
```

**Planned to Add**:
- Database host
- Database port
- Database name
- JDBC driver
- Connection credentials

**Future Behavior**:
Will extract values from centralized configuration (when implemented) and merge into connector configuration.

**Returns**: Self for method chaining

## Planned Usage

### Future JDBC Configuration
```python
from src.framework.config import (
    ConnectorConfig,
    CentralizedPipelineConfig,
    JdbcConfigBuilder,
)

builder = JdbcConfigBuilder(
    ConnectorConfig("jdbc", {
        "table": "customers"
    }),
    CentralizedPipelineConfig(
        jdbc_host="db.example.com",
        jdbc_port=5432,
        jdbc_database="lakehouse",
        jdbc_driver="org.postgresql.Driver"
    )
)

config = (builder
    .merge_shared_context()
    .merge_schema_overrides(schema)
    .resolve_secrets()
    .build())
```

### Via Factory
```python
from src.framework.config import ConnectorConfigBuilderFactory

builder = ConnectorConfigBuilderFactory.create_builder(
    "jdbc",
    connector_config,
    centralized_config
)

config = builder.merge_shared_context().build()
```

## Planned Design

### Expected Context Additions
```python
{
    "host": self.pipeline_config.jdbc_host,
    "port": self.pipeline_config.jdbc_port,
    "database": self.pipeline_config.jdbc_database,
    "driver": self.pipeline_config.jdbc_driver,
    "user": self.pipeline_config.jdbc_user,
    "password": self.pipeline_config.jdbc_password,
}
```

### Expected Configuration Properties
- `host`: Database hostname
- `port`: Database port (5432, 3306, etc.)
- `database`: Database name
- `driver`: JDBC driver class
- `user`: Database username
- `password`: Database password (as secret)
- `table`: Table name for data loading
- `query`: Optional query for data extraction

## Supported Databases (Planned)

- PostgreSQL
- MySQL
- Oracle
- SQL Server
- DB2
- Other JDBC-compatible databases

## Integration Points

- **ConnectorConfigBuilderFactory**: Creates this builder for "jdbc" type
- **BaseConfigBuilder**: Inherits common methods
- **CentralizedPipelineConfig**: Source of JDBC configuration (when added)

## Notes

- This builder is currently a placeholder
- Implementation will follow same pattern as VolumeConfigBuilder and RestApiConfigBuilder
- Credentials should be stored as secrets and resolved via SecretResolver
- Same fluent API pattern as other builders

## Related Classes
- [BaseConfigBuilder](./base_config_builder.md)
- [VolumeConfigBuilder](./volume_config_builder.md)
- [RestApiConfigBuilder](./rest_api_config_builder.md)
- [ConnectorConfig](../connector_config.md)
- [CentralizedPipelineConfig](../centralized_config.md)

## See Also
- [Builders Overview](./README.md)
- [Configuration Overview](../README.md)
- [Adding New Connectors](../ADDING_NEW_CONNECTOR.md)
