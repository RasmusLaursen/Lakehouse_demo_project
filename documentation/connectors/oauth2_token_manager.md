# OAuth2TokenManager

## Overview

`OAuth2TokenManager` handles OAuth2 token management for REST API connectors. It manages token acquisition, caching, and refresh to ensure authentication remains valid across multiple API calls.

**Location**: `src/framework/connectors/oauth2_token_manager.py`

**Pattern**: Token Caching Strategy

## Class Definition

```python
class OAuth2TokenManager:
    """Manages OAuth2 tokens for REST API authentication."""
```

## Key Responsibilities

1. **Token Acquisition**: Request tokens from OAuth2 provider
2. **Token Caching**: Store tokens and reuse when valid
3. **Token Refresh**: Automatically refresh expired tokens
4. **Credential Management**: Store and use credentials securely
5. **Token Validation**: Check token expiration

## Configuration

### Required Fields

| Field | Type | Description |
|-------|------|-------------|
| client_id | str | OAuth2 client ID |
| client_secret | str | OAuth2 client secret |
| token_url | str | Token endpoint URL |

### Optional Fields

| Field | Type | Description |
|-------|------|-------------|
| refresh_threshold_seconds | int | Seconds before expiry to refresh (default: 300) |
| cache_path | str | Path to cache tokens |
| scopes | list | OAuth2 scopes to request |

## Methods

### __init__()

Initialize token manager.

```python
def __init__(self, config: Dict[str, Any]):
    """
    Initialize OAuth2 token manager.
    
    Args:
        config: Configuration dict with oauth2 settings
    """
```

**Parameters**:
- `config` (Dict): OAuth2 configuration

**Raises**: `ConfigurationException` - Missing required fields

### get_token()

Get valid OAuth2 token.

```python
def get_token(self) -> str:
    """
    Get valid OAuth2 token.
    
    Automatically refreshes if expired.
    
    Returns:
        Valid OAuth2 token (bearer token)
        
    Raises:
        TokenException: If token acquisition fails
    """
```

**Returns**: `str` - Valid OAuth2 bearer token

**Raises**: `TokenException` - Token acquisition/refresh failed

### is_token_valid()

Check if current token is valid.

```python
def is_token_valid(self) -> bool:
    """
    Check if cached token is still valid.
    
    Returns:
        True if token is valid, False if expired
    """
```

**Returns**: `bool` - Token validity status

### refresh_token()

Manually refresh token.

```python
def refresh_token(self) -> None:
    """
    Manually refresh OAuth2 token.
    
    Raises:
        TokenException: If refresh fails
    """
```

**Raises**: `TokenException` - Token refresh failed

## Usage Examples

### Basic Usage

```python
from src.framework.connectors.oauth2_token_manager import OAuth2TokenManager

# Configure OAuth2
oauth_config = {
    "client_id": "my_client_id",
    "client_secret": "my_client_secret",
    "token_url": "https://provider.com/oauth/token"
}

# Create manager
token_manager = OAuth2TokenManager(oauth_config)

# Get token
token = token_manager.get_token()

# Use in REST API calls
headers = {
    "Authorization": f"Bearer {token}"
}
```

### With REST API Connector

```python
from src.framework.connectors import RestApiConnector
from src.framework.config import ConnectorConfig

# Create connector config with OAuth2
connector_config = ConnectorConfig(
    connector_type="rest_api",
    config={
        "url": "https://api.example.com/data",
        "auth_type": "oauth2",
        "auth_config": {
            "client_id": "my_client_id",
            "client_secret": "my_client_secret",
            "token_url": "https://provider.com/oauth/token"
        }
    }
)

# Connector uses token manager internally
connector = ConnectorFactory.create_connector(
    "rest_api",
    connector_config,
    centralized
)
```

### Token Refresh Handling

```python
# Get token (auto-refreshes if expired)
token = token_manager.get_token()

# Check if valid
if token_manager.is_token_valid():
    # Use token
    pass
else:
    # Refresh before use
    token_manager.refresh_token()
    token = token_manager.get_token()
```

## Configuration in ConnectorConfig

### OAuth2 in ConnectorConfig

```python
connector_config = ConnectorConfig(
    connector_type="rest_api",
    config={
        "url": "https://api.example.com",
        "method": "GET",
        "auth_type": "oauth2",
        "auth_config": {
            "client_id": "your_client_id",
            "client_secret": "your_client_secret",
            "token_url": "https://oauth.provider.com/token",
            "scopes": ["read", "data"],
            "refresh_threshold_seconds": 600
        }
    }
)
```

### Using Secrets

```python
from src.framework.config import SecretResolver

# Resolve secrets
secret_resolver = SecretResolver(centralized_config)

connector_config = ConnectorConfig(
    connector_type="rest_api",
    config={
        "url": "https://api.example.com",
        "auth_type": "oauth2",
        "auth_config": {
            "client_id": secret_resolver.resolve("oauth2_client_id"),
            "client_secret": secret_resolver.resolve("oauth2_client_secret"),
            "token_url": "https://oauth.provider.com/token"
        }
    }
)
```

## Token Caching Strategy

### How Caching Works

1. **First Request**: Token manager fetches from OAuth2 provider
2. **Storage**: Stores token with expiration time
3. **Subsequent Requests**: Returns cached token if valid
4. **Auto-Refresh**: Automatically refreshes if near expiration
5. **Fallback**: Fetches new token if cache miss

### Cache Location

```python
# Default: In-memory cache
# Optional: Persistent cache at specified path
oauth_config = {
    "client_id": "...",
    "client_secret": "...",
    "token_url": "...",
    "cache_path": "/tmp/oauth_cache"  # Optional
}
```

## Error Handling

### Invalid Credentials

```python
try:
    token_manager = OAuth2TokenManager({
        "client_id": "invalid",
        "client_secret": "invalid",
        "token_url": "https://provider.com/token"
    })
    token = token_manager.get_token()
except TokenException as e:
    print(f"Authentication failed: {e}")
```

### Network Errors

```python
try:
    token = token_manager.get_token()
except TokenException as e:
    if "connection" in str(e).lower():
        print("Network error - retrying...")
```

### Token Expiration

```python
# Auto-handled by manager
if not token_manager.is_token_valid():
    # Automatically refresh on next get_token() call
    token = token_manager.get_token()
```

## Security Considerations

### 1. **Secret Storage**

Never store credentials directly:

```python
# ❌ Don't do this
config = {
    "client_id": "my_client_id",
    "client_secret": "my_secret"  # EXPOSED!
}

# ✅ Do this
config = {
    "client_id": secret_resolver.resolve("oauth_client_id"),
    "client_secret": secret_resolver.resolve("oauth_client_secret")
}
```

See: [SecretResolver](../configuration/secret_resolver.md)

### 2. **Token Scope Limitation**

Request only needed scopes:

```python
oauth_config = {
    "scopes": ["data:read"],  # Minimal scope
    # Don't request "admin" if not needed
}
```

### 3. **Token Refresh Threshold**

Refresh before expiration to avoid failures:

```python
oauth_config = {
    "refresh_threshold_seconds": 600  # Refresh 10 min before expiry
}
```

## Testing

### Test Patterns

```python
class TestOAuth2TokenManager:
    def test_get_token_returns_valid_token(self):
        """Test token manager returns valid token."""
        manager = OAuth2TokenManager({
            "client_id": "test_id",
            "client_secret": "test_secret",
            "token_url": "https://test.provider.com/token"
        })
        
        token = manager.get_token()
        assert token is not None
        assert isinstance(token, str)
    
    def test_token_validation(self):
        """Test token validity check."""
        manager = OAuth2TokenManager(config)
        
        # Get token
        token = manager.get_token()
        
        # Should be valid
        assert manager.is_token_valid()
    
    def test_automatic_refresh_on_expiry(self):
        """Test token auto-refresh on expiration."""
        manager = OAuth2TokenManager(config)
        
        # Get initial token
        token1 = manager.get_token()
        
        # Simulate expiration
        manager._token_expiration = time.time() - 1
        
        # Next call should refresh
        token2 = manager.get_token()
        
        # Should be a new token
        assert token1 != token2
```

## Integration with REST API Connector

```python
# RestApiConnector uses OAuth2TokenManager internally
connector = ConnectorFactory.create_connector(
    "rest_api",
    ConnectorConfig("rest_api", {
        "url": "https://api.example.com",
        "auth_type": "oauth2",
        "auth_config": oauth_config
    }),
    centralized
)

# Connector automatically manages tokens
df = connector.load(spark)
```

## Related Classes

- [RestApiConnector](./datasources/rest_api_connector.md) - Uses token manager
- [SecretResolver](../configuration/secret_resolver.md) - Stores credentials
- [ConnectorConfig](../configuration/connector_config.md) - Configuration

## See Also

- [QUICK_REFERENCE.md](./QUICK_REFERENCE.md) - Quick lookup
- [ARCHITECTURE.md](./ARCHITECTURE.md) - Design patterns
- [SecretResolver](../configuration/secret_resolver.md) - Credential management
