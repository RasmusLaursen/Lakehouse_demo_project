# Secret Integration - Deployment Guide

This guide walks you through deploying and testing the secret integration framework with the REST API connector.

## Prerequisites

- Databricks workspace with Unity Catalog enabled
- Databricks CLI configured
- Secret scope permissions (READ access)
- DLT pipeline configured

## Step 1: Create Secret Scope

Create a secret scope to store your API credentials:

```bash
# Create scope (one-time setup)
databricks secrets create-scope --scope api-secrets

# Verify scope created
databricks secrets list-scopes
```

## Step 2: Store Secrets

Add your API credentials to the scope:

```bash
# Store API token
databricks secrets put --scope api-secrets --key my-api-token

# You'll be prompted to enter the token value in an editor
# Or use --string-value flag:
databricks secrets put --scope api-secrets --key my-api-token --string-value "your-token-here"

# List secrets in scope (values are hidden)
databricks secrets list --scope api-secrets
```

## Step 3: Configure Data Contract

Update your data contract to use secret references:

```yaml
# data_contracts/source_system/my_api.yml
servers:
  - server: dev
    type: "rest_api"
    customProperties:
      - property: connector_config
        value:
          endpoint: "https://api.example.com/v1"
          auth_type: "bearer"
          auth_token: "secret://api-secrets/my-api-token"  # Secret reference
```

## Step 4: Configure DLT Pipeline

For DLT pipelines, add secret configuration to pipeline settings:

### Option A: Using Databricks Asset Bundle

Edit `resources/pipelines/your_pipeline.yml`:

```yaml
resources:
  pipelines:
    my_pipeline:
      name: my_pipeline
      target: raw
      catalog: ${bundle.target}_lakehouse
      
      # Add secret configuration
      configuration:
        spark.secrets.api-secrets.token: "{{secrets/api-secrets/my-api-token}}"
      
      libraries:
        - notebook:
            path: src/solution/raw/raw_pipeline.py
```

### Option B: Using Databricks UI

1. Navigate to **Workflows** > **Delta Live Tables**
2. Select your pipeline
3. Click **Settings**
4. Under **Advanced** > **Configuration**
5. Add key-value pairs:
   - Key: `spark.secrets.api-secrets.token`
   - Value: `{{secrets/api-secrets/my-api-token}}`

### Option C: Using API/CLI

```json
{
  "configuration": {
    "spark.secrets.api-secrets.token": "{{secrets/api-secrets/my-api-token}}"
  }
}
```

## Step 5: Deploy Bundle

Deploy the updated configuration:

```bash
# Validate bundle
databricks bundle validate -t developer

# Deploy to developer environment
databricks bundle deploy -t developer

# Run the pipeline
databricks bundle run -t developer energidataservice_pipeline
```

## Step 6: Verify Secret Resolution

Check the DLT pipeline logs to verify secrets are being resolved:

```
INFO - SecretManager initialized in DLT pipeline context
INFO - Successfully retrieved secret from scope: api-*** (key: to***)
INFO - API Call [GET]: https://api.example.com/v1/data with params: {...}
```

Note: Secret names and values are automatically masked in logs.

## Testing in Notebook

For testing in a notebook (non-DLT context):

```python
from src.framework.secret_integration import get_secret

# This will use dbutils.secrets in notebook context
api_token = get_secret("api-secrets", "my-api-token")
print(f"Token retrieved: {api_token[:4]}***")  # Show first 4 chars only
```

## Troubleshooting

### Secret Not Found in DLT

**Error**: `SecretNotFoundError: Secret 'my-api-token' not found in scope 'api-secrets'`

**Solution**: 
1. Verify secret exists: `databricks secrets list --scope api-secrets`
2. Check pipeline configuration includes: `spark.secrets.api-secrets.token: "{{secrets/api-secrets/my-api-token}}"`
3. Redeploy bundle: `databricks bundle deploy -t developer`

### Permission Denied

**Error**: `SecretAccessDeniedError: Access denied to secret scope 'api-secrets'`

**Solution**:
1. Check scope permissions: `databricks secrets list-acls --scope api-secrets`
2. Grant READ access: `databricks secrets put-acl --scope api-secrets --principal <user@example.com> --permission READ`
3. For service principals: `databricks secrets put-acl --scope api-secrets --principal <service-principal-id> --permission READ`

### Wrong Context Detection

**Error**: Secrets work in notebook but not in DLT

**Solution**: This is expected behavior:
- Notebooks use `dbutils.secrets` (automatic)
- DLT uses Spark configuration (requires pipeline settings)
- Verify DLT pipeline has configuration section with secret mappings

### Secret Not Resolved

**Error**: API returns 401 Unauthorized

**Solution**:
1. Check logs for "Could not resolve secret reference" warning
2. Verify format: `secret://scope/key` or `{{secrets/scope/key}}`
3. Test secret directly: `get_secret("api-secrets", "my-api-token")`
4. Check token is valid (not expired)

## Validation Checklist

- [ ] Secret scope created
- [ ] Secrets stored in scope
- [ ] Data contract uses secret reference format
- [ ] DLT pipeline configuration includes secret mappings
- [ ] Bundle deploys successfully
- [ ] Pipeline logs show "Successfully retrieved secret"
- [ ] API requests include authentication header
- [ ] No secret values visible in logs

## Secret Reference Formats

Both formats are supported:

```yaml
# Format 1: secret:// protocol (recommended)
auth_token: "secret://api-secrets/my-api-token"

# Format 2: DLT style interpolation
auth_token: "{{secrets/api-secrets/my-api-token}}"
```

For DLT pipelines, you must configure both:
1. Data contract: `secret://scope/key`
2. Pipeline settings: `spark.secrets.scope.key: "{{secrets/scope/key}}"`

## Best Practices

1. **Separate scopes by environment**:
   - `dev-api-secrets`
   - `test-api-secrets`
   - `prod-api-secrets`

2. **Use descriptive key names**:
   - Good: `energidataservice-api-token`
   - Bad: `token1`

3. **Rotate secrets regularly**:
   ```python
   from src.framework.secret_integration import clear_secret_cache
   clear_secret_cache()
   ```

4. **Limit scope permissions**:
   - Grant READ only
   - Use service principals for pipelines
   - Individual users for notebooks

5. **Monitor secret usage**:
   - Check audit logs
   - Track secret access patterns
   - Alert on unauthorized access attempts

## Next Steps

After successful deployment:

1. **Test incremental loading**: Verify checkpoint-based streaming
2. **Monitor API calls**: Check comprehensive logging
3. **Validate data quality**: Confirm data freshness and accuracy
4. **Set up alerts**: Monitor pipeline failures and API errors
5. **Document secrets**: Maintain secret inventory and rotation schedule
