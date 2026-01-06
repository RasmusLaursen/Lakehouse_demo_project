# ConnectorConfig

## Location
`src/framework/config/connector_config.py`

## Purpose
Wraps and manages connector-specific configuration dictionary. Provides a clean API for accessing and manipulating configuration values.

## Responsibilities
- Store connector type and configuration parameters
- Provide clean get/set API for configuration access
- Support configuration merging
- Extract secret references for resolution
- Initialize from server configuration

## Properties

### connector_type
The type of connector: `"volume"`, `"rest_api"`, `"jdbc"`, `"autoloader"`, etc.

### config_dict
Internal dictionary storing all configuration parameters.

## Methods

### `__init__(connector_type, config_dict=None)`
Initialize configuration with connector type and optional configuration dictionary.

```python
config = ConnectorConfig("rest_api", {
    "endpoint": "https://api.example.com",
    "auth_type": "bearer",
    "auth_token": "token_123"
})
```

### `get(key, default=None)`
Get a configuration value by key.

```python
endpoint = config.get("endpoint")
timeout = config.get("timeout", 30)  # with default
```

### `set(key, value)`
Set a configuration value.

```python
config.set("endpoint", "https://api.example.com")
config.set("timeout", 60)
```

### `merge(other_dict)`
Merge another dictionary into configuration, returning a new instance.

```python
extended_config = config.merge({
    "extra_param": "value"
})
```

**Note**: Returns a new ConnectorConfig instance without modifying the original.

### `extract_secrets()`
Extract all keys that contain secret references.

```python
secret_keys = config.extract_secrets()
# Output: ["auth_token", "api_key", ...]
```

**Returns**: List of keys that have values containing secret references (e.g., `{{secrets/*}}`).

### `to_dict()`
Export configuration as dictionary.

```python
config_dict = config.to_dict()
print(config_dict)
# Output: {'endpoint': '...', 'auth_type': '...', ...}
```

### `from_server_config(server_config)`
Create ConnectorConfig from server configuration object.

```python
from src.framework.config import ConnectorConfig

server_config = data_contract.servers[0]
connector_config = ConnectorConfig.from_server_config(server_config)
```

**Expected server_config attributes**:
- `connector_type`: Type of connector
- Other attributes become configuration parameters

### `source_type` (Property)
Alias for connector_type. Used for backward compatibility.

```python
source_type = config.source_type  # Returns connector_type
```

## Usage Examples

### Basic Get/Set
```python
config = ConnectorConfig("rest_api", {
    "endpoint": "https://api.example.com"
})

# Get value
endpoint = config.get("endpoint")

# Set value
config.set("timeout", 30)

# Get with default
retries = config.get("retries", 3)
```

### Configuration Merging
```python
base_config = ConnectorConfig("rest_api", {
    "endpoint": "https://api.example.com",
    "method": "GET",
})

# Merge adds new keys
extended = base_config.merge({
    "timeout": 60,
    "retries": 3
})

# Original unchanged
print(base_config.to_dict())   # 2 keys
print(extended.to_dict())      # 5 keys
```

### Secret Extraction
```python
config = ConnectorConfig("rest_api", {
    "endpoint": "https://api.example.com",
    "auth_token": "{{secrets/scope/api_key}}",
    "api_key": "{{secrets/scope/api_key}}"
})

secret_keys = config.extract_secrets()
# Output: ["auth_token", "api_key"]

# Use with SecretResolver
resolver = SecretResolver()
for key in secret_keys:
    value = config.get(key)
    resolved = resolver.resolve(value)
    config.set(key, resolved)
```

### From Server Configuration
```python
from src.framework.config import ConnectorConfig
from src.framework.helper import get_data_contract

# Get data contract
contract = get_data_contract("lakehouse")
server_config = contract.servers[0]

# Create ConnectorConfig
connector_config = ConnectorConfig.from_server_config(server_config)

print(connector_config.connector_type)  # e.g., "rest_api"
print(connector_config.get("endpoint"))
```

### With Builder Pattern
```python
from src.framework.config import ConnectorConfigBuilderFactory

# Create config
config = ConnectorConfig("volume", {
    "path": "/Volumes/landing/data"
})

# Use with builder
builder = ConnectorConfigBuilderFactory.create_builder(
    "volume",
    config,
    centralized_config
)

final_config = builder.merge_shared_context().build()
```

## Common Patterns

### Pattern: Conditional Update
```python
# Update only if not already set
if not config.get("timeout"):
    config.set("timeout", 30)
```

### Pattern: Extract and Resolve Secrets
```python
secret_keys = config.extract_secrets()
for key in secret_keys:
    original_value = config.get(key)
    resolved_value = resolver.resolve(original_value)
    config.set(key, resolved_value)
```

### Pattern: Convert to Other Format
```python
# Export as dictionary for external API
config_dict = config.to_dict()
spark_options = {"header": "true", **config_dict}
```

## Design Rationale

- **Wrapper Pattern**: Encapsulates dictionary while providing clean API
- **Immutable Merge**: `merge()` returns new instance, preserves original
- **Simple API**: Common operations (get, set, merge) are straightforward
- **Secret-Aware**: Built-in secret extraction support
- **Type Agnostic**: Works with any connector type

## Integration Points

- **ConnectorConfigBuilderFactory**: Creates appropriate builder for this config
- **BaseConfigBuilder**: Uses config for building final configuration
- **SecretResolver**: Resolves secrets extracted from this config
- **Data Contracts**: Server configuration converted to ConnectorConfig

## Related Classes
- [CentralizedPipelineConfig](./centralized_config.md)
- [SecretResolver](./secret_resolver.md)
- [BaseConfigBuilder](./builders/base_config_builder.md)

## See Also
- [Configuration Overview](./README.md)
- [Architecture Guide](./ARCHITECTURE.md)
