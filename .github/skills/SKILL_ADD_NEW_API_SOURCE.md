# SKILL: Adding a New API Source for Ingestion

## Overview
This comprehensive guide walks you through adding a complete new API source to the lakehouse framework, including data contracts, schemas in Databricks Asset Bundles (DABs), pipelines, and Python ingestion files.

## Prerequisites
Before starting, gather the following information:

### 1. Source System Information
- [ ] **Source system name**: CamelCase identifier (e.g., `WeatherAPI`, `PaymentGateway`)
- [ ] **Source system description**: Business purpose and data scope
- [ ] **Base URL**: API endpoint base (e.g., `https://api.example.com/`)
- [ ] **Authentication type**: none, oauth2_refresh, bearer token, API key
- [ ] **Rate limits**: Requests per second/minute to avoid throttling

### 2. API Endpoint Details
- [ ] **Endpoint path(s)**: Specific paths for each entity (e.g., `v1/transactions`, `data/weather`)
- [ ] **HTTP Method**: GET, POST
- [ ] **Query parameters**: Required and optional parameters
- [ ] **Response format**: JSON structure and data path
- [ ] **Pagination**: Strategy (offset, cursor, none)

### 3. Data Requirements
- [ ] **Entity names**: List of all entities to ingest
- [ ] **Sample responses**: Actual API responses for each entity
- [ ] **Primary keys**: Unique identifiers per entity
- [ ] **Timestamp fields**: For incremental loading
- [ ] **Schema definitions**: All columns with types and descriptions

## Complete Step-by-Step Process

---

## STEP 1: Create Data Contract YAML

Create a new data contract file in `data_contracts/source_system/` for your source system.

### File Naming Convention
- Use lowercase with underscores: `{source_system_name}.yml`
- Examples: `weatherapi.yml`, `payment_gateway.yml`

### Data Contract Template

```yaml
kind: DataContract
apiVersion: v3.0.0
id: "urn:datacontract:{source_system}:declaration"
status: active

name: "{Source System Name} - {Purpose}"
version: "1.0.0"
description:
  purpose: "Data contract for {describe what data this contains}"
  limitations: "{Any known limitations, rate limits, data freshness}"
  usage: "{How this data should be used in the lakehouse}"

servers:
  - server: dev
    type: "rest_api"
    environment: "development"
    format: "json"
    customProperties:
      - property: loadtype
        value: "rest_api"
      - property: connector_type
        value: "rest_api"
      - property: connector_config
        value:
          endpoint: "https://api.example.com/"
          method: "GET"
          auth_type: "none"  # or "bearer", "oauth2_refresh"
          pagination_type: "none"  # or "offset", "cursor"
          add_audit_columns: true
          timeout: 60
          
          # Rate limiting (optional)
          rate_limit_delay: 0.5  # Delay in seconds between requests

schema:
  - name: EntityName  # CamelCase, no spaces
    description: "Clear description of the entity"
    type: table
    customProperties:
      # SCD Type: 1 = append-only, 2 = track changes
      - property: scd_type
        value: 1
      
      # Primary key columns
      - property: keys
        value: ["id", "timestamp"]
      
      # Column for ordering/incremental processing
      - property: sequence_column
        value: "timestamp"
      
      # Connector type (use "eloverblik_api" for REST APIs)
      - property: loadtype
        value: "eloverblik_api"
      
      # Processing mode
      - property: mode
        value: "streaming"  # or "batch"
      
      # Timestamp field for incremental loading
      - property: timestamp_field
        value: "timestamp"
      
      # Initial load start date
      - property: initial_timestamp
        value: "2026-01-01"
      
      # Days per micro-batch (1=daily, 7=weekly, 30=monthly)
      - property: days_per_batch
        value: 1
      
      # API endpoint path (relative to base URL)
      - property: table_name
        value: "v1/endpoint/path"
      
      # HTTP method
      - property: method
        value: "GET"
      
      # URL parameters template
      - property: url_params_template
        value:
          start: "2026-01-01"
          end: "2026-12-31"
          limit: 1000
      
      # JSON path to data array in response
      - property: data_path
        value: "records"  # or "data.results", "items", etc.
    
    properties:
      - name: id
        description: "Unique identifier"
        type: string
        required: true
        tags:
          - "primary_key"
          - "identifier"
        classification: "internal"
        example: "12345"
      
      - name: timestamp
        description: "Event timestamp"
        type: timestamp
        required: true
        tags:
          - "temporal"
          - "primary_key"
        classification: "public"
        example: "2026-02-16T10:00:00"
      
      - name: value
        description: "Measured value"
        type: double
        required: false
        tags:
          - "measure"
        classification: "public"
        example: "42.5"
      
      # Add more properties as needed...
```

**Save as:** `data_contracts/source_system/{source_system}.yml`

---

## STEP 2: Create Unity Catalog Schema Definition

Create Unity Catalog schema definitions in `resources/unity_catalog/source_systems/`.

### File Naming Convention
- Use lowercase with underscores: `{source_system}.yml`

### Schema Template

```yaml
variables:
  source_system_{source_system}:
    default: {source_system}

resources:
  schemas:
    {source_system}_raw_schema:
      name: ${var.source_system_{source_system}}
      catalog_name: ${var.raw_catalog}
      comment: Raw schema for {Source System Name} API data
    
    {source_system}_base_schema:
      name: ${var.source_system_{source_system}}
      catalog_name: ${var.base_catalog}
      comment: Base schema for {Source System Name} with CDC
```

**Example for "weatherapi":**
```yaml
variables:
  source_system_weatherapi:
    default: weatherapi

resources:
  schemas:
    weatherapi_raw_schema:
      name: ${var.source_system_weatherapi}
      catalog_name: ${var.raw_catalog}
      comment: Raw schema for Weather API data
    
    weatherapi_base_schema:
      name: ${var.source_system_weatherapi}
      catalog_name: ${var.base_catalog}
      comment: Base schema for Weather API with CDC
```

**Save as:** `resources/unity_catalog/source_systems/{source_system}.yml`

---

## STEP 3: Create Pipeline Configuration

Create a DLT pipeline configuration in `resources/pipelines/`.

### File Naming Convention
- Use lowercase with underscores: `{source_system}.pipeline.yml`

### Pipeline Template

```yaml
variables:
  environment:
    default: dev

resources:
  pipelines:
    {source_system}_pipeline:
      name: {source_system}_pipeline_${var.environment}
      catalog: ${resources.schemas.{source_system}_raw_schema.catalog_name}
      schema: ${resources.schemas.{source_system}_raw_schema.name}
      
      libraries:
        - glob:
            include: ../../src/solution/raw/raw_ingest_{source_system}.py
        - glob:
            include: ../../src/solution/base/base_ingest_{source_system}.py

      environment:
        dependencies:
          - ../../dist/*.whl

      configuration:
        bundle.sourcePath: ${workspace.file_path}/src
        environment: ${var.environment}

        catalogs: | 
          {
            "raw_catalog": "${resources.schemas.{source_system}_raw_schema.catalog_name}",
            "base_catalog": "${resources.schemas.{source_system}_base_schema.catalog_name}"
          }
        
        schemas: | 
          {
            "{source_system}_raw_schema": "${resources.schemas.{source_system}_raw_schema.name}",
            "{source_system}_base_schema": "${resources.schemas.{source_system}_base_schema.name}"
          }

      serverless: true
      channel: CURRENT
      photon: false
      continuous: false
      
      development: true
```

**Example for "weatherapi":**
```yaml
variables:
  environment:
    default: dev

resources:
  pipelines:
    weatherapi_pipeline:
      name: weatherapi_pipeline_${var.environment}
      catalog: ${resources.schemas.weatherapi_raw_schema.catalog_name}
      schema: ${resources.schemas.weatherapi_raw_schema.name}
      
      libraries:
        - glob:
            include: ../../src/solution/raw/raw_ingest_weatherapi.py
        - glob:
            include: ../../src/solution/base/base_ingest_weatherapi.py

      environment:
        dependencies:
          - ../../dist/*.whl

      configuration:
        bundle.sourcePath: ${workspace.file_path}/src
        environment: ${var.environment}

        catalogs: | 
          {
            "raw_catalog": "${resources.schemas.weatherapi_raw_schema.catalog_name}",
            "base_catalog": "${resources.schemas.weatherapi_base_schema.catalog_name}"
          }
        
        schemas: | 
          {
            "weatherapi_raw_schema": "${resources.schemas.weatherapi_raw_schema.name}",
            "weatherapi_base_schema": "${resources.schemas.weatherapi_base_schema.name}"
          }

      serverless: true
      channel: CURRENT
      photon: false
      continuous: false
      
      development: true
```

**Save as:** `resources/pipelines/{source_system}.pipeline.yml`

---

## STEP 4: Create Raw Layer Python File

Create the raw layer ingestion Python file in `src/solution/raw/`.

### File Naming Convention
- Use lowercase with underscores: `raw_ingest_{source_system}.py`

### Raw Layer Template

```python
"""Raw layer ingestion pipeline for {source_system} source system.

This module uses the factory pattern to create DLT tables dynamically.
All logic has been consolidated into the RawPipelineFactory.
"""
from src.framework.factory.raw_factory import create_raw_pipeline

# Create the raw pipeline for {source_system} source system
create_raw_pipeline("{source_system}")
```

**Example for "weatherapi":**
```python
"""Raw layer ingestion pipeline for weatherapi source system.

This module uses the factory pattern to create DLT tables dynamically.
All logic has been consolidated into the RawPipelineFactory.
"""
from src.framework.factory.raw_factory import create_raw_pipeline

# Create the raw pipeline for weatherapi source system
create_raw_pipeline("weatherapi")
```

**Save as:** `src/solution/raw/raw_ingest_{source_system}.py`

---

## STEP 5: Create Base Layer Python File

Create the base layer CDC Python file in `src/solution/base/`.

### File Naming Convention
- Use lowercase with underscores: `base_ingest_{source_system}.py`

### Base Layer Template

```python
"""Base layer CDC pipeline for {source_system} source system.

This module uses the factory pattern to create DLT tables dynamically.
All logic has been consolidated into the BasePipelineFactory.

The factory properly handles:
- Data contract loading
- Configuration management
- Optional data quality validation
- Change Data Capture (CDC) processing
- Proper closure variable capture for DLT decorators
"""
from src.framework.factory.base_factory import create_base_pipeline

# Create the base pipeline for {source_system} source system
create_base_pipeline("{source_system}")
```

**Example for "weatherapi":**
```python
"""Base layer CDC pipeline for weatherapi source system.

This module uses the factory pattern to create DLT tables dynamically.
All logic has been consolidated into the BasePipelineFactory.

The factory properly handles:
- Data contract loading
- Configuration management
- Optional data quality validation
- Change Data Capture (CDC) processing
- Proper closure variable capture for DLT decorators
"""
from src.framework.factory.base_factory import create_base_pipeline

# Create the base pipeline for weatherapi source system
create_base_pipeline("weatherapi")
```

**Save as:** `src/solution/base/base_ingest_{source_system}.py`

---

## STEP 6: Deploy and Test

### 6.1 Deploy to Databricks

```powershell
# Deploy the bundle to your environment
databricks bundle deploy -p {profile} -t {target}
```

**Example:**
```powershell
databricks bundle deploy -p privat-free -t developer
```

### 6.2 Verify Schema Creation

Check in Databricks Unity Catalog that schemas were created:
- `{raw_catalog}.{source_system}` - Raw schema
- `{base_catalog}.{source_system}` - Base schema

### 6.3 Run the Pipeline

```powershell
# Run the pipeline
databricks bundle run {source_system}_pipeline -p {profile} -t {target}
```

**Example:**
```powershell
databricks bundle run weatherapi_pipeline -p privat-free -t developer
```

### 6.4 Validate Data Flow

1. **Check Raw Layer**: Verify tables were created and data loaded in raw schema
2. **Check Base Layer**: Verify CDC processing in base schema
3. **Monitor Logs**: Review DLT pipeline logs for errors or warnings
4. **Data Quality**: Check row counts, null values, and data types

---

## Complete File Checklist

After completing all steps, you should have created these files:

- [ ] `data_contracts/source_system/{source_system}.yml` - Data contract with schemas
- [ ] `resources/unity_catalog/source_systems/{source_system}.yml` - Schema definitions
- [ ] `resources/pipelines/{source_system}.pipeline.yml` - DLT pipeline configuration
- [ ] `src/solution/raw/raw_ingest_{source_system}.py` - Raw layer ingestion
- [ ] `src/solution/base/base_ingest_{source_system}.py` - Base layer CDC

---

## Common Issues and Solutions

### Issue 1: Schema Not Found
**Problem:** Pipeline fails with "Schema not found" error
**Solution:** Ensure Unity Catalog schema YAML is properly configured and deployed

### Issue 2: Authentication Failures
**Problem:** API returns 401/403 errors
**Solution:** 
- Verify `auth_type` in data contract
- Add secrets to Databricks Secret Scope if needed
- Update connector_config with proper authentication

### Issue 3: No Data Loaded
**Problem:** Pipeline runs but no data appears
**Solution:**
- Check `data_path` in data contract matches API response structure
- Verify `initial_timestamp` is appropriate
- Check API endpoint URL and parameters

### Issue 4: Schema Mismatch
**Problem:** Data types don't match or columns missing
**Solution:**
- Test API manually and review actual response
- Update schema in data contract to match reality
- Use string types initially, cast later if needed

### Issue 5: Rate Limiting
**Problem:** API returns 429 Too Many Requests
**Solution:**
- Add `rate_limit_delay` in connector_config
- Reduce `days_per_batch` to make smaller requests
- Check API rate limit documentation

---

## Advanced Configurations

### Adding Multiple Entities
Add multiple schema entries in the same data contract file:

```yaml
schema:
  - name: EntityOne
    # ... entity one configuration
  
  - name: EntityTwo
    # ... entity two configuration
```

### Authentication with Secrets
For APIs requiring secrets (API keys, tokens):

```yaml
customProperties:
  - property: connector_config
    value:
      endpoint: "https://api.example.com/"
      auth_type: "bearer"
      secret_scope: "my-scope"
      secret_key: "api-token"
```

### Custom Headers
Add custom HTTP headers:

```yaml
customProperties:
  - property: connector_config
    value:
      endpoint: "https://api.example.com/"
      headers:
        X-API-Version: "v2"
        X-Custom-Header: "value"
```

### POST Requests with Body
For POST requests with JSON body:

```yaml
customProperties:
  - property: method
    value: "POST"
  - property: body_template
    value:
      query: "SELECT * FROM data"
      format: "json"
```

---

## Best Practices

1. **Start Small**: Begin with one entity, validate, then add more
2. **Test API First**: Always test the API manually before creating contracts
3. **Document Everything**: Add clear descriptions to all entities and fields
4. **Use Appropriate Batch Sizes**: Match `days_per_batch` to data granularity
5. **Monitor Rate Limits**: Add delays to avoid overwhelming APIs
6. **Version Control**: Commit each step and test before moving forward
7. **Naming Consistency**: Use consistent naming across all files
8. **Security**: Never hardcode secrets, use Databricks Secret Scope

---

## Quick Reference: File Locations

```
project_root/
├── data_contracts/
│   └── source_system/
│       └── {source_system}.yml              # Data contract
├── resources/
│   ├── unity_catalog/
│   │   └── source_systems/
│   │       └── {source_system}.yml          # Schema definitions
│   └── pipelines/
│       └── {source_system}.pipeline.yml     # Pipeline config
└── src/
    └── solution/
        ├── raw/
        │   └── raw_ingest_{source_system}.py    # Raw layer
        └── base/
            └── base_ingest_{source_system}.py   # Base layer
```

---

## Example: Complete Weather API Implementation

Here's a complete example for adding "weatherapi" source:

**1. Data Contract:** `data_contracts/source_system/weatherapi.yml`
**2. Schema:** `resources/unity_catalog/source_systems/weatherapi.yml`
**3. Pipeline:** `resources/pipelines/weatherapi.pipeline.yml`
**4. Raw:** `src/solution/raw/raw_ingest_weatherapi.py`
**5. Base:** `src/solution/base/base_ingest_weatherapi.py`

Deploy and run:
```powershell
databricks bundle deploy -p privat-free -t developer
databricks bundle run weatherapi_pipeline -p privat-free -t developer
```

---

## Summary

You now have a complete new API source integrated into the lakehouse framework with:
✅ Data contract defining API and schema
✅ Unity Catalog schemas in raw and base layers
✅ DLT pipeline configuration
✅ Raw layer ingestion using factory pattern
✅ Base layer CDC processing using factory pattern

The framework handles all the heavy lifting - you just define the configuration!
