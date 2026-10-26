# RestApiDatasource

## Overview

`RestApiDatasource` is a variant of the REST API connector optimized for Delta Live Tables (DLT). It enables seamless integration of REST API data sources within DLT pipelines using a standardized datasource format.

**Location**: `src/framework/connectors/rest_api_datasource.py`

**Extends**: `BaseConnector`

**Type**: `"rest_api_ds"`

**Framework**: Delta Live Tables

## Key Features

- **DLT Integration**: Native support for Delta Live Tables
- **Schema Evolution**: Automatic schema tracking
- **Incremental Loading**: Support for incremental data updates
- **Workflow Optimization**: Designed for pipeline workflows
- **Standard Datasource Format**: Works with spark.read.format()
- **Partition Support**: Partitioned data loading

## Class Definition

```python
class RestApiDatasource(BaseConnector):
    """REST API datasource for Delta Live Tables."""
```

## Configuration

### Required Fields

| Field | Type | Description |
|-------|------|-------------|
| url | str | API endpoint URL |
| method | str | HTTP method |

### Optional Fields

| Field | Type | Description |
|-------|------|-------------|
| auth_type | str | Authentication type |
| auth_config | dict | Authentication configuration |
| headers | dict | Custom HTTP headers |
| query_params | dict | URL query parameters |
| batch_size | int | Records per request |
| checkpoint_location | str | DLT checkpoint location |
| schema_location | str | DLT schema location |

## Core Methods

### validate()

Validate datasource configuration.

```python
def validate(self) -> None:
    """Validate REST API datasource configuration."""
```

### load()

Load data as DLT-compatible source.

```python
def load(self, spark: SparkSession, schema=None) -> DataFrame:
    """Load data from REST API for DLT."""
```

### close()

Clean up resources.

```python
def close(self) -> None:
    """Close datasource."""
```

## Usage Examples

### Basic DLT Integration

```python
import dlt
from src.framework.connectors import ConnectorFactory
from src.framework.config import ConnectorConfig, CentralizedPipelineConfig

@dlt.table
def api_source_bronze():
    """Load data from REST API."""
    connector_config = ConnectorConfig(
        connector_type="rest_api_ds",
        config={
            "url": "https://api.example.com/data",
            "method": "GET"
        }
    )
    
    centralized = CentralizedPipelineConfig(
        source_name="api_source",
        target_schema="bronze"
    )
    
    connector = ConnectorFactory.create_connector(
        "rest_api_ds",
        connector_config,
        centralized
    )
    
    return connector.load(spark)
```

### With Schema Evolution

```python
@dlt.table(
    schema_location="s3://bucket/.schema/api_source"
)
def api_source_bronze():
    """Load with schema evolution tracking."""
    connector_config = ConnectorConfig(
        connector_type="rest_api_ds",
        config={
            "url": "https://api.example.com/data",
            "method": "GET",
            "schema_location": "s3://bucket/.schema/api_source"
        }
    )
    
    connector = ConnectorFactory.create_connector(
        "rest_api_ds",
        connector_config,
        centralized
    )
    
    return connector.load(spark)
```

### With Authentication

```python
@dlt.table
def secure_api_source():
    """Load from authenticated API."""
    connector_config = ConnectorConfig(
        connector_type="rest_api_ds",
        config={
            "url": "https://api.example.com/data",
            "method": "GET",
            "auth_type": "oauth2",
            "auth_config": {
                "client_id": secret_resolver.resolve("api_client_id"),
                "client_secret": secret_resolver.resolve("api_client_secret"),
                "token_url": "https://oauth.example.com/token"
            }
        }
    )
    
    connector = ConnectorFactory.create_connector(
        "rest_api_ds",
        connector_config,
        centralized
    )
    
    return connector.load(spark)
```

### With Query Parameters

```python
@dlt.table
def filtered_api_data():
    """Load filtered data."""
    connector_config = ConnectorConfig(
        connector_type="rest_api_ds",
        config={
            "url": "https://api.example.com/data",
            "method": "GET",
            "query_params": {
                "filter": "status=active",
                "limit": 1000
            }
        }
    )
    
    connector = ConnectorFactory.create_connector(
        "rest_api_ds",
        connector_config,
        centralized
    )
    
    return connector.load(spark)
```

### Multi-Step Pipeline

```python
import dlt

@dlt.table
def api_bronze():
    """Load raw data."""
    return load_via_datasource("rest_api_ds", config)

@dlt.view
def api_silver():
    """Cleanse data."""
    return spark.sql("""
        SELECT 
            id, name, email,
            CAST(created_at AS TIMESTAMP) as created_at
        FROM LIVE.api_bronze
        WHERE id IS NOT NULL
    """)

@dlt.table
def api_gold():
    """Aggregate data."""
    return spark.sql("""
        SELECT 
            COUNT(*) as total_records,
            COUNT(DISTINCT id) as unique_ids,
            MAX(created_at) as latest_date
        FROM LIVE.api_silver
    """)
```

## DLT Integration

### DLT Workflow Configuration

```python
# In Databricks DLT workflow YAML
clusters:
  - job_cluster_key: dlt_cluster
    new_cluster:
      spark_version: "14.3.x-scala2.12"
      node_type_id: "i3.xlarge"
      num_workers: 2

development: true

target: "api_datasets"
notebook_path: "/path/to/dlt_notebook"

pipelines:
  - id: "api_ingestion"
    name: "API Data Ingestion"
    clusters:
      - job_cluster_key: dlt_cluster
```

### Materialization Strategy

```python
import dlt

# Bronze - Raw data from API
@dlt.table(
    comment="Raw API data",
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
def api_bronze():
    """Raw data directly from API."""
    connector = create_api_datasource_connector()
    return connector.load(spark)

# Silver - Cleaned data
@dlt.table(
    comment="Cleaned API data",
    table_properties={"delta.timeTravelRetentionInDays": "30"}
)
def api_silver():
    """Cleaned and validated data."""
    return spark.sql("""
        SELECT * FROM LIVE.api_bronze
        WHERE _corrupt_record IS NULL
    """)

# Gold - Aggregated data
@dlt.table(
    comment="Aggregated metrics",
    table_properties={"delta.timeTravelRetentionInDays": "90"}
)
def api_gold():
    """Business metrics."""
    return spark.sql("SELECT ... FROM LIVE.api_silver")
```

## Schema Management

### Schema Tracking

```python
@dlt.table(
    schema_location="s3://bucket/.schema"
)
def api_data():
    """Schema automatically tracked and evolved."""
    return connector.load(spark)
```

### Explicit Schema Definition

```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([
    StructField("id", IntegerType()),
    StructField("name", StringType()),
    StructField("email", StringType())
])

@dlt.table
def api_data_typed():
    """Data with explicit schema."""
    df = connector.load(spark, schema=schema)
    return df
```

## Incremental Loading

### Tracking Incremental Updates

```python
@dlt.table
def api_incremental():
    """Incremental data loading."""
    connector_config = ConnectorConfig(
        connector_type="rest_api_ds",
        config={
            "url": "https://api.example.com/data",
            "query_params": {
                "updated_since": dlt.current_timestamp() - timedelta(hours=1)
            }
        }
    )
    
    connector = ConnectorFactory.create_connector(
        "rest_api_ds",
        connector_config,
        centralized
    )
    
    return connector.load(spark)
```

## Error Handling

### Handling Failed Loads

```python
@dlt.table
def api_data_with_retry():
    """Handle transient failures."""
    try:
        connector = create_connector()
        return connector.load(spark)
    except ConnectorException as e:
        if "timeout" in str(e).lower():
            # Log and retry
            print(f"Retry loading: {e}")
            time.sleep(5)
            return connector.load(spark)
        else:
            raise
```

### Quarantine Pattern

```python
@dlt.table
def api_data_bronze():
    """Load with error handling."""
    return connector.load(spark)

@dlt.view
def api_data_silver_valid():
    """Valid records."""
    return spark.sql("""
        SELECT * FROM LIVE.api_data_bronze
        WHERE _corrupt_record IS NULL
    """)

@dlt.view
def api_data_silver_invalid():
    """Invalid records for review."""
    return spark.sql("""
        SELECT * FROM LIVE.api_data_bronze
        WHERE _corrupt_record IS NOT NULL
    """)
```

## Performance Optimization

### Partitioned Loading

```python
from src.framework.connectors.partition_strategies import DateRangePartitionStrategy

@dlt.table
def api_data_partitioned():
    """Load with partitioning for performance."""
    strategy = DateRangePartitionStrategy(
        start_date="2024-01-01",
        end_date="2024-12-31",
        partition_interval="month"
    )
    
    connector_config = ConnectorConfig(
        connector_type="rest_api_ds",
        config={
            "url": "https://api.example.com/data",
            "partition_strategy": strategy
        }
    )
    
    connector = ConnectorFactory.create_connector(
        "rest_api_ds",
        connector_config,
        centralized
    )
    
    return connector.load(spark)
```

## Configuration Builder

```python
from src.framework.config import ConnectorConfigBuilderFactory

builder = ConnectorConfigBuilderFactory.create_builder("rest_api_ds")

connector_config = (builder
    .with_url("https://api.example.com/data")
    .with_method("GET")
    .with_auth_type("oauth2")
    .with_auth_config({...})
    .build()
)
```

## Monitoring and Logging

### DLT Table Metrics

```python
@dlt.table(
    comment="API data with monitoring",
    table_properties={
        "dlt.table_type": "APPEND"
    }
)
def api_data_monitored():
    """Load with DLT monitoring."""
    df = connector.load(spark)
    
    # Log metrics
    print(f"Loaded {df.count()} records")
    
    return df
```

## Best Practices

### 1. **Layer Strategy**

✓ Use bronze/silver/gold layers
✓ Validate in silver layer
✓ Aggregate in gold layer

### 2. **Error Handling**

✓ Quarantine invalid records
✓ Monitor load failures
✓ Implement retry logic

### 3. **Schema Evolution**

✓ Enable schema tracking
✓ Use schema_location
✓ Version schemas

### 4. **Performance**

✓ Use partitioning for large datasets
✓ Set appropriate batch sizes
✓ Cache token credentials

## Related Classes

- [BaseConnector](../base_connector.md) - Base class
- [RestApiConnector](./rest_api_connector.md) - Direct REST API connector
- [RestApiWorkflowDatasource](./rest_api_workflow_datasource.md) - Workflow variant

## See Also

- [../QUICK_REFERENCE.md](../QUICK_REFERENCE.md) - Quick examples
- [../README.md](../README.md) - Connectors overview
- [README.md](./README.md) - Datasources overview
- [Delta Live Tables Docs](https://docs.databricks.com/en/delta-live-tables/)
