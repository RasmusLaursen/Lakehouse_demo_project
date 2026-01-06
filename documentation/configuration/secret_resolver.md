# SecretResolver

## Location
`src/framework/config/secret_resolver.py`

## Purpose
Resolves secret references in configuration values to actual secrets stored in Databricks.

## Responsibilities
- Detect and parse secret references
- Support multiple secret formats
- Integrate with Databricks secret management
- Fail fast on unresolvable secrets
- Provide logging for debugging

## Supported Secret Formats

### 1. Spark Configuration Format
```
{{spark.config-key}}
```

Resolves from `spark.conf.get("spark.config-key")`.

**Use Case**: DLT pipelines where configuration is injected via Spark config.

**Example**:
```python
resolver.resolve("{{spark.api-token}}")
# Looks for: spark.conf.get("spark.api-token")
```

### 2. Databricks Secrets Format
```
{{secrets/scope/key}}
```

Resolves via `dbutils.secrets.get("scope", "key")`.

**Use Case**: Standard Databricks secret store access.

**Example**:
```python
resolver.resolve("{{secrets/aws/access_key}}")
# Looks for: dbutils.secrets.get("aws", "access_key")
```

### 3. Secret Protocol Format
```
secret://scope/key
```

Resolves via `dbutils.secrets.get("scope", "key")`.

**Use Case**: URL-style secret references.

**Example**:
```python
resolver.resolve("secret://aws/secret_key")
# Looks for: dbutils.secrets.get("aws", "secret_key")
```

### 4. Plain Values
```
plain_value_123
```

Returned as-is (not a secret reference).

**Use Case**: Non-sensitive configuration values.

**Example**:
```python
resolver.resolve("plain_text")
# Output: "plain_text" (unchanged)
```

## Methods

### `__init__(spark=None)`
Initialize resolver with optional Spark session.

```python
from src.framework.config import SecretResolver

resolver = SecretResolver()
# OR with explicit Spark session
resolver = SecretResolver(spark=spark)
```

### `resolve(value)`
Resolve a secret reference to its actual value.

```python
token = resolver.resolve("{{spark.api-token}}")
secret = resolver.resolve("{{secrets/scope/key}}")
protocol = resolver.resolve("secret://scope/key")
plain = resolver.resolve("plain_value")
```

**Returns**: Resolved value as string.

**Raises**: 
- `ValueError`: If secret format is invalid or secret not found
- `Exception`: If Databricks secret access fails

### `_is_secret_reference(value)`
Check if value contains a secret reference.

```python
if resolver._is_secret_reference("{{secrets/scope/key}}"):
    # Process as secret
```

**Returns**: Boolean

### `_resolve_spark_config(key)`
Resolve Spark configuration format.

```python
token = resolver._resolve_spark_config("api-token")
# Gets: spark.conf.get("api-token")
```

**Internal method** - use `resolve()` instead.

### `_resolve_databricks_secret(scope, key)`
Resolve Databricks secret format.

```python
secret = resolver._resolve_databricks_secret("aws", "access_key")
# Gets: dbutils.secrets.get("aws", "access_key")
```

**Internal method** - use `resolve()` instead.

### `_extract_scope_and_key(reference)`
Extract scope and key from secret reference.

```python
scope, key = resolver._extract_scope_and_key("{{secrets/aws/access_key}}")
# Output: ("aws", "access_key")
```

**Internal method** - use `resolve()` instead.

## Usage Examples

### Resolving Configuration Secrets
```python
from src.framework.config import SecretResolver, ConnectorConfig

resolver = SecretResolver()

config = ConnectorConfig("rest_api", {
    "endpoint": "https://api.example.com",
    "auth_type": "bearer",
    "auth_token": "{{secrets/scope/api_key}}"
})

# Resolve secret
token = config.get("auth_token")
resolved_token = resolver.resolve(token)
config.set("auth_token", resolved_token)

print(config.get("auth_token"))  # Actual token value
```

### With Multiple Secret Formats
```python
resolver = SecretResolver()

# Spark config format
spark_token = resolver.resolve("{{spark.databricks-token}}")

# Databricks secrets format
db_secret = resolver.resolve("{{secrets/aws/access_key}}")

# Protocol format
proto_secret = resolver.resolve("secret://azure/connection_string")

# Plain value
plain = resolver.resolve("production")
```

### Batch Secret Resolution
```python
from src.framework.config import SecretResolver, ConnectorConfig

config = ConnectorConfig("rest_api", {
    "endpoint": "https://api.example.com",
    "auth_token": "{{secrets/scope/token}}",
    "api_key": "{{secrets/scope/key}}",
    "timeout": "30"  # Not a secret
})

resolver = SecretResolver()
secret_keys = config.extract_secrets()

# Resolve all secrets
for key in secret_keys:
    original_value = config.get(key)
    resolved_value = resolver.resolve(original_value)
    config.set(key, resolved_value)
```

### With Builder Pattern
```python
from src.framework.config import ConnectorConfigBuilderFactory

builder = ConnectorConfigBuilderFactory.create_builder(
    "rest_api",
    connector_config,
    centralized_config
)

# Builder automatically resolves secrets
config = (builder
    .merge_shared_context()
    .resolve_secrets()      # <-- Resolves all secrets
    .build())
```

### Error Handling
```python
resolver = SecretResolver()

try:
    # Attempt to resolve non-existent secret
    value = resolver.resolve("{{secrets/nonexistent/key}}")
except ValueError as e:
    print(f"Secret resolution failed: {e}")
```

## Common Patterns

### Pattern: Conditional Resolution
```python
resolver = SecretResolver()

value = config.get("auth_token")

# Resolve only if it looks like a secret
if "{{" in value or value.startswith("secret://"):
    resolved = resolver.resolve(value)
else:
    resolved = value

config.set("auth_token", resolved)
```

### Pattern: Pre-Flight Validation
```python
resolver = SecretResolver()

# Check if all secrets are resolvable before processing
for key in config.extract_secrets():
    try:
        value = config.get(key)
        resolver.resolve(value)
    except Exception as e:
        raise ValueError(f"Secret {key} not available: {e}")
```

### Pattern: Selective Secret Resolution
```python
resolver = SecretResolver()

# Resolve only specific keys
secret_keys_to_resolve = ["auth_token", "api_key"]

for key in secret_keys_to_resolve:
    if key in config_dict:
        value = config_dict[key]
        if resolver._is_secret_reference(value):
            config_dict[key] = resolver.resolve(value)
```

## Debugging

Enable logging to see secret resolution details:

```python
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("src.framework.config.secret_resolver")

# Now you'll see messages like:
# INFO - Resolving {{spark.api-token}} from Spark config
# INFO - Successfully resolved {{spark.api-token}} (length: 16)
```

## Security Considerations

✅ **Do**:
- Store sensitive values as secrets, never hardcode
- Use format `{{secrets/scope/key}}` for persistent secrets
- Use `{{spark.*}}` for DLT environment variables
- Validate secrets are resolvable before use

❌ **Don't**:
- Log actual secret values (resolver logs lengths only)
- Store secrets in code or configuration files
- Pass secrets as command-line arguments
- Commit credentials to version control

## Design Rationale

- **Multiple Formats**: Supports various secret storage patterns
- **Fail Fast**: Errors on unresolvable secrets prevent silent failures
- **Logging**: Provides visibility without exposing secrets
- **Flexible**: Handles mixed plain and secret values
- **Integrated**: Works seamlessly with builder pattern

## Integration Points

- **ConnectorConfig**: Used with `extract_secrets()`
- **BaseConfigBuilder**: Called during `resolve_secrets()` step
- **Configuration Workflow**: Final step before configuration is ready

## Related Classes
- [ConnectorConfig](./connector_config.md)
- [BaseConfigBuilder](./builders/base_config_builder.md)

## See Also
- [Configuration Overview](./README.md)
- [Architecture Guide](./ARCHITECTURE.md)
- [Databricks Secrets Documentation](https://docs.databricks.com/security/secrets/)
