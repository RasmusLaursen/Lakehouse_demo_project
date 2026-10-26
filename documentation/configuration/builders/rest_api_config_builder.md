# RestApiConfigBuilder

## Location
`src/framework/config/builders/rest_api_config_builder.py`

## Purpose
Builder for REST API connector configuration. Adds REST API-specific metadata and handles schema building and OAuth2 token management.

## Extends
`BaseConfigBuilder`

## Responsibilities
- Add raw catalog and schema (for workflow datasources)
- Build Spark schema from data contract properties
- Pre-load OAuth2 access tokens
- Handle both standard REST API and workflow datasource scenarios

## Methods

### `merge_shared_context()`
Add REST API-specific context from centralized configuration.

```python
builder.merge_shared_context()
```

**For Standard REST API** (`rest_api`):
- Does not add shared context (uses only connector config)

**For Workflow Datasources** (`rest_api_workflow_ds`):
- Adds `raw_catalog`: raw_catalog from centralized config
- Adds `raw_schema`: raw_schema from centralized config

**Behavior**:
- Checks connector type
- For workflow datasources, extracts raw catalog/schema
- Only adds if not already configured
- Logs number of items added
- Returns self for method chaining

**Example**:
```python
builder = RestApiConfigBuilder(
    ConnectorConfig("rest_api_workflow_ds", {...}),
    CentralizedPipelineConfig(raw_catalog="raw", raw_schema="raw")
)

builder.merge_shared_context()
# Adds: raw_catalog="raw", raw_schema="raw"
```

### `build_spark_schema(model_name, schema)`
Build Spark schema from data contract properties.

```python
builder.build_spark_schema("customer_api", schema_object)
```

**Behavior**:
- Extracts properties from data contract schema
- Converts to Spark StructType
- Caches schema in configuration
- Used for workflow datasources

**Returns**: Self for method chaining

**Example**:
```python
builder = RestApiConfigBuilder(...)

# Build schema for REST API model
builder.build_spark_schema("metering_points", schema)

# Schema now in config
schema_config = builder.config.get("schema")
```

### `pre_load_oauth2_token(model_name)`
Pre-load OAuth2 access token by exchanging refresh token.

```python
builder.pre_load_oauth2_token("customer_api")
```

**Behavior**:
- Checks if OAuth2 is configured
- Gets refresh token from configuration
- Creates OAuth2TokenManager
- Exchanges refresh token for access token
- Updates configuration with new access token
- Avoids repeated token exchanges during workflow

**Returns**: Self for method chaining

**Example**:
```python
builder = RestApiConfigBuilder(
    ConnectorConfig("rest_api", {
        "endpoint": "https://api.example.com",
        "auth_type": "oauth2_refresh",
        "refresh_token": "{{secrets/scope/refresh_token}}"
    }),
    ...
)

# Pre-load token early (after secrets resolved)
builder.resolve_secrets().pre_load_oauth2_token("my_model")

# Access token now cached
access_token = builder.config.get("auth_token")
```

## Usage Examples

### Standard REST API
```python
from src.framework.config import (
    ConnectorConfig,
    CentralizedPipelineConfig,
    RestApiConfigBuilder,
)

builder = RestApiConfigBuilder(
    ConnectorConfig("rest_api", {
        "endpoint": "https://api.example.com",
        "auth_type": "bearer",
        "auth_token": "{{secrets/scope/token}}"
    }),
    CentralizedPipelineConfig(...)
)

config = (builder
    .merge_shared_context()
    .resolve_secrets()
    .build())
```

### Workflow Datasource
```python
builder = RestApiConfigBuilder(
    ConnectorConfig("rest_api_workflow_ds", {
        "endpoint": "https://api.example.com/v2",
        "method": "POST",
    }),
    CentralizedPipelineConfig(
        raw_catalog="raw",
        raw_schema="raw"
    )
)

config = (builder
    .merge_shared_context()           # Adds raw catalog/schema
    .merge_schema_overrides(schema)
    .build_spark_schema("model", schema)
    .resolve_secrets()
    .build())
```

### With OAuth2
```python
builder = RestApiConfigBuilder(
    ConnectorConfig("rest_api_workflow_ds", {
        "endpoint": "https://oauth-api.example.com",
        "auth_type": "oauth2_refresh",
        "refresh_token": "{{secrets/oauth/refresh}}"
    }),
    centralized_config
)

config = (builder
    .merge_shared_context()
    .merge_schema_overrides(schema)
    .resolve_secrets()              # Resolve refresh token
    .pre_load_oauth2_token("api")   # Exchange for access token
    .build())
```

### Via Factory
```python
from src.framework.config import ConnectorConfigBuilderFactory

builder = ConnectorConfigBuilderFactory.create_builder(
    "rest_api_workflow_ds",
    connector_config,
    centralized_config
)

config = (builder
    .merge_shared_context()
    .resolve_secrets()
    .build())
```

## Connector Type Variants

### `rest_api`
Standard REST API connector.
- No shared context added
- Uses only connector config

### `rest_api_ds`
REST API as workflow datasource.
- No shared context added (legacy)

### `rest_api_workflow_ds`
REST API as modern workflow datasource.
- Adds raw catalog and schema
- Supports schema building
- Supports OAuth2 token management

## Workflows

### Standard REST API Workflow
```
ConnectorConfig("rest_api")
    ↓
merge_shared_context()      # No-op for standard
    ↓
resolve_secrets()
    ↓
build()
    ↓
Configuration ready
```

### Workflow Datasource with OAuth2
```
ConnectorConfig("rest_api_workflow_ds")
    ↓
merge_shared_context()      # Add raw_catalog, raw_schema
    ↓
merge_schema_overrides()    # Apply schema properties
    ↓
build_spark_schema()        # Build from contract
    ↓
resolve_secrets()           # Resolve refresh_token
    ↓
pre_load_oauth2_token()     # Exchange for access_token
    ↓
build()
    ↓
Configuration ready for workflow
```

## Design Rationale

- **Flexible**: Handles multiple REST API connector types
- **Schema Support**: Can build schemas from contracts
- **OAuth2 Ready**: Handles token management
- **Workflow Integration**: Designed for workflow datasources

## Integration Points

- **ConnectorConfigBuilderFactory**: Creates this builder for "rest_api*" types
- **BaseConfigBuilder**: Inherits common methods
- **OAuth2TokenManager**: Used for token exchange
- **Data Contracts**: Extracts schema for schema building

## Related Classes
- [BaseConfigBuilder](./base_config_builder.md)
- [ConnectorConfig](../connector_config.md)
- [CentralizedPipelineConfig](../centralized_config.md)

## See Also
- [Builders Overview](./README.md)
- [Configuration Overview](../README.md)
