# SKILL: Adding New Entities to Data Contracts

## Overview
This guide helps you add new data sources/entities to YAML data contracts for ingestion through the Eloverblik/Energy Data Service connector framework.

## Prerequisites
Before adding a new entity, gather the following information:

### 1. API Information
- [ ] **Base URL**: e.g., `https://api.energidataservice.dk/`
- [ ] **Endpoint path**: e.g., `dataset/DayAheadPrices`
- [ ] **HTTP Method**: GET or POST
- [ ] **Authentication**: none, oauth2_refresh, bearer token
- [ ] **Rate limits**: Requests per second/minute
- [ ] **Query parameters**: start, end, limit, etc.

### 2. Data Structure
- [ ] **Response format**: JSON structure
- [ ] **Data path**: Location of records in response (e.g., `records`, `result.result`)
- [ ] **Sample response**: Get actual API response with 2-3 records
- [ ] **Nested structures**: Any arrays or objects that need special handling

### 3. Schema Information
- [ ] **Entity name**: CamelCase without spaces (e.g., `DayAheadPrices`)
- [ ] **Description**: Clear purpose of the data
- [ ] **Primary keys**: Unique identifier columns
- [ ] **Timestamp field**: Main temporal column for incremental loading
- [ ] **All columns**: Name, type, description, required/optional
- [ ] **SCD Type**: 1 (append-only) or 2 (track changes)

### 4. Operational Parameters
- [ ] **Initial timestamp**: Where to start ingestion (e.g., `2025-01-01`)
- [ ] **Batch size**: Days per micro-batch (1 for hourly data, 7 for daily, 30 for monthly)
- [ ] **Mode**: streaming
- [ ] **Dependencies**: Does it require data from another endpoint first?

## Step-by-Step Process

### Step 1: Test the API
Test the API endpoint manually to understand its behavior:

```powershell
# Using curl or Invoke-RestMethod
Invoke-RestMethod -Uri "https://api.energidataservice.dk/dataset/DayAheadPrices?start=2025-01-01&end=2025-01-02&limit=10" | ConvertTo-Json -Depth 10
```

Document:
- Response structure
- Available fields
- Data types
- Timestamp formats
- Pagination behavior

### Step 2: Choose the Appropriate Contract File

**For API** → `data_contracts/source_system/eloverblik.yml`
- Customer data
- Metering points
- Time series consumption
- Charges

**For Energy Data Service** → `data_contracts/source_system/energidataservice.yml`
- Public datasets
- Production data
- Price data
- Market data

### Step 3: Add Schema Definition

Add a new schema entry to the appropriate YAML file:

```yaml
  - name: YourEntityName  # CamelCase, no spaces
    description: "Clear description of what this data represents"
    type: table
    customProperties:
      # SCD Type: 1 = append-only, 2 = track changes
      - property: scd_type
        value: 1
      
      # Primary key(s) - must uniquely identify a record
      - property: keys
        value: ["Timestamp", "IdColumn"]
      
      # Column used for incremental ordering
      - property: sequence_column
        value: "Timestamp"
      
      # Connector type (must be "eloverblik_api" for this framework)
      - property: loadtype
        value: "eloverblik_api"
      
      # Streaming vs batch mode
      - property: mode
        value: "streaming"  # or "batch"
      
      # Timestamp field for incremental loading
      - property: timestamp_field
        value: "Timestamp"
      
      # Where to start ingestion
      - property: initial_timestamp
        value: "2025-01-01"
      
      # How many days per micro-batch (smaller = less memory)
      - property: days_per_batch
        value: 1  # 1 for hourly data, 7 for daily, 30 for monthly
      
      # API endpoint path
      - property: table_name
        value: "dataset/YourEndpoint"
      
      # HTTP method
      - property: method
        value: "GET"  # or "POST"
      
      # URL query parameters (for GET requests)
      - property: url_params_template
        value:
          start: "2025-01-01"
          end: "2026-12-31"
          limit: 0
      
      # For POST requests - body template
      # - property: body_params_template
      #   value:
      #     meteringPoints:
      #       meteringPoint: "body_params"  # Replaced with actual values
      
      # Path to records in JSON response
      - property: data_path
        value: "records"  # or "result", "result.result", etc.
```

### Step 4: Define Schema Properties

Add all columns with complete metadata:

```yaml
    properties:
      - name: ColumnName
        description: "What this column contains"
        type: string  # string, timestamp, decimal, integer, boolean
        required: true  # or false
        tags:
          - "category"  # identifier, temporal, primary_key, price, etc.
        classification: "public"  # or "sensitive", "pii"
        example: "Sample value"
```

**Data Types Mapping:**
- `string` → text, codes, names
- `timestamp` → dates, datetimes
- `decimal` → prices, measurements, percentages
- `integer` → counts, IDs
- `boolean` → flags, yes/no

**Important Tags:**
- `primary_key` → Part of unique identifier
- `temporal` → Time-related fields
- `identifier` → ID/code fields
- `price` → Monetary values
- `geography` → Location fields
- `measurement` → Numeric measurements

### Step 5: Configure Memory Management

Choose `days_per_batch` based on data volume:

| Data Frequency | Recommended Batch Size | Memory Impact |
|----------------|------------------------|---------------|
| Sub-hourly (15min) | 1 day | Low |
| Hourly | 1-3 days | Low-Medium |
| Daily | 7-14 days | Medium |
| Weekly | 30-60 days | High |
| Monthly | 365 days | High |

**Rule of thumb**: Start small (1 day) and increase if performance is acceptable.

### Step 6: Handle Special Cases

#### Nested Arrays/Objects
If response has nested structures (e.g., arrays inside objects):

```yaml
      # Store as JSON strings instead of exploding
      - property: data_path
        value: "result.result"
      
      # In schema, define nested columns as strings:
      - name: nestedArray
        type: string  # Will contain JSON array as string
```

#### Dependencies on Other Endpoints
If endpoint requires data from another endpoint first (e.g., TimeSeriesData needs MeteringPoints):

```yaml
      - property: dependency_table
        value: "api/meteringpoints/meteringpoints"
      
      - property: is_root_call
        value: false
      
      - property: body_params_template
        value:
          meteringPoints:
            meteringPoint: "body_params"  # Replaced with dependency data
```

#### Authentication for POST Requests
For endpoints requiring OAuth2:

```yaml
      - property: method
        value: "POST"
      
      - property: body_params_template
        value:
          # Your request structure here
```

Ensure `auth_type: "oauth2_refresh"` is set at the server level.

### Step 7: Validate Configuration

Run validation checks:

```yaml
# Check YAML syntax
# Check required properties are present
# Verify data_path matches actual response structure
# Confirm timestamp_field exists in schema properties
# Ensure primary keys are marked with primary_key tag
# Verify initial_timestamp format is YYYY-MM-DD
```

### Step 8: Deploy and Test

1. **Deploy to Databricks:**
   ```powershell
   databricks bundle deploy -p privat-free -t developer
   ```

2. **Monitor first micro-batch:**
   - Check memory usage (should be < 1024 MB for serverless)
   - Verify record count
   - Inspect data quality

3. **Common Issues:**
   - **OOM Error**: Reduce `days_per_batch`
   - **No data**: Check `initial_timestamp` and API date range
   - **Schema mismatch**: Verify column names match API response exactly
   - **URL not working**: Check `table_name` and `url_params_template`

## Examples

### Example 1: Simple GET Endpoint (Hourly Data)

```yaml
  - name: DayAheadPrices
    description: "Day-ahead electricity spot prices"
    type: table
    customProperties:
      - property: scd_type
        value: 1
      - property: keys
        value: ["TimeUTC", "PriceArea"]
      - property: sequence_column
        value: "TimeUTC"
      - property: loadtype
        value: "eloverblik_api"
      - property: mode
        value: "streaming"
      - property: timestamp_field
        value: "TimeUTC"
      - property: initial_timestamp
        value: "2025-01-01"
      - property: days_per_batch
        value: 7
      - property: table_name
        value: "dataset/DayAheadPrices"
      - property: method
        value: "GET"
      - property: url_params_template
        value:
          start: "2025-01-01"
          end: "2026-12-31"
          limit: 0
      - property: data_path
        value: "records"
    properties:
      - name: TimeUTC
        type: timestamp
        required: true
        tags: ["temporal", "primary_key"]
      - name: PriceArea
        type: string
        required: true
        tags: ["identifier", "primary_key"]
      - name: DayAheadPriceEUR
        type: decimal
        tags: ["price"]
      - name: DayAheadPriceDKK
        type: decimal
        tags: ["price"]
```

### Example 2: POST with Dependencies

```yaml
  - name: TimeSeriesData
    description: "Historical consumption data"
    type: table
    customProperties:
      - property: scd_type
        value: 1
      - property: keys
        value: ["position", "out_Quantity_Quality"]
      - property: loadtype
        value: "eloverblik_api"
      - property: mode
        value: "streaming"
      - property: timestamp_field
        value: "position"
      - property: initial_timestamp
        value: "2025-01-01"
      - property: days_per_batch
        value: 1
      - property: table_name
        value: "api/meterdata/gettimeseries/{dateFrom}/{dateTo}"
      - property: method
        value: "POST"
      - property: dependency_table
        value: "api/meteringpoints/meteringpoints"
      - property: is_root_call
        value: false
      - property: body_params_template
        value:
          meteringPoints:
            meteringPoint: "body_params"
      - property: url_params_template
        value:
          aggregation: "Actual"
      - property: data_path
        value: "result"
    properties:
      # ... define all columns
```

### Example 3: Nested Array as JSON String

```yaml
  - name: Charges
    description: "Subscription, tariff, and fee charges"
    customProperties:
      # ... standard config
      - property: data_path
        value: "result.result"
    properties:
      - name: subscriptions
        type: string  # Stores JSON array as string
        description: "Subscription charges (JSON array)"
      - name: tariffs
        type: string  # Stores JSON array as string
        description: "Tariff charges (JSON array)"
      - name: fees
        type: string  # Stores JSON array as string
        description: "Fee charges (JSON array)"
```

## Troubleshooting Guide

### Memory Issues (OOM)
**Symptom**: `MEMORY_LIMIT_SERVERLESS exceeded 1024 MB`

**Solutions**:
1. Reduce `days_per_batch` to 1
2. Reduce number of columns if possible
3. Check for memory leaks in connector code
4. Consider batch mode for historical backfill

### No Data Ingested
**Symptom**: Zero records in target table

**Check**:
1. API returns data for date range: Test with curl
2. `initial_timestamp` is within available data range
3. `timestamp_field` exists in schema and API response
4. `data_path` correctly navigates to records array
5. URL parameters are properly replaced

### Schema Mismatch
**Symptom**: Errors about missing/unexpected columns

**Check**:
1. Column names match API response exactly (case-sensitive)
2. `data_path` points to correct level in response
3. Nested structures are handled (JSON strings vs exploding)
4. All API fields are defined in schema or handled by `_raw_json`

### URL Not Found (404)
**Symptom**: API returns 404 error

**Check**:
1. `table_name` path is correct
2. Query parameters in `url_params_template`, not in `table_name`
3. Path parameters use `{placeholder}` syntax
4. Base endpoint URL is correct in server config

### Authentication Failures
**Symptom**: 401/403 errors

**Check**:
1. `auth_type` is correctly set
2. Token refresh endpoint is accessible
3. Secrets are properly configured
4. OAuth2 scopes are sufficient

## Checklist Before Committing

- [ ] API tested manually with sample dates
- [ ] All required customProperties defined
- [ ] Primary keys identified and tagged
- [ ] Timestamp field exists in schema
- [ ] days_per_batch appropriate for data volume
- [ ] data_path tested with actual API response
- [ ] Column types match API data types
- [ ] Descriptions are clear and complete
- [ ] Tags are appropriate and consistent
- [ ] Examples are realistic
- [ ] Deployed and tested with small date range
- [ ] Memory usage < 1024 MB per batch
- [ ] Data quality validated in target table

## Best Practices

1. **Start Conservative**: Use small `days_per_batch` initially (1 day)
2. **Test Incrementally**: Test with 2-3 days of data before full backfill
3. **Document Edge Cases**: Note any special handling in descriptions
4. **Consistent Naming**: Follow CamelCase for entity names
5. **Complete Metadata**: Every column needs description, type, tags
6. **Monitor First Run**: Watch memory and execution time
7. **Version Control**: Commit contract changes separately from code changes

## Related Documentation

- [Connector Architecture](./connectors/ARCHITECTURE.md)
- [Configuration Guide](./configuration/centralized_config.md)
- [Data Contract Parser](./contract_parser/)
- [Eloverblik Datasource](./connectors/datasources/eloverblik_datasource.md)

## Contact & Support

For questions or issues:
1. Check existing entities for similar patterns
2. Review error logs in Databricks
3. Test API manually to isolate issues
4. Consult framework documentation
