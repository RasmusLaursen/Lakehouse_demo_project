# RestApiWorkflowDatasource

## Overview

`RestApiWorkflowDatasource` is a variant of the REST API datasource optimized for workflow-specific use cases in Delta Live Tables. It provides additional features for workflow orchestration and monitoring.

**Location**: `src/framework/connectors/rest_api_workflow_datasource.py`

**Extends**: `BaseConnector`

**Type**: `"rest_api_workflow_ds"`

**Framework**: Delta Live Tables (Workflow variant)

## Key Features

- **Workflow Optimization**: Designed for DLT workflow patterns
- **Scheduling Support**: Integration with workflow scheduling
- **Dependency Management**: Support for task dependencies
- **Checkpointing**: Automatic checkpoint management
- **Monitoring**: Built-in monitoring and alerting
- **Retry Logic**: Workflow-level retry support

## Class Definition

```python
class RestApiWorkflowDatasource(BaseConnector):
    """REST API datasource optimized for workflows."""
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
| workflow_id | str | Workflow identifier |
| task_id | str | Task identifier |
| checkpoint_location | str | Checkpoint path |
| max_retries | int | Maximum retries |
| dependencies | list | Task dependencies |

## Core Methods

### validate()

Validate workflow datasource configuration.

```python
def validate(self) -> None:
    """Validate REST API workflow datasource configuration."""
```

### load()

Load data within workflow context.

```python
def load(self, spark: SparkSession, schema=None) -> DataFrame:
    """Load data with workflow context."""
```

### close()

Clean up workflow resources.

```python
def close(self) -> None:
    """Close workflow datasource."""
```

## Usage Examples

### Basic Workflow Integration

```python
import dlt
from src.framework.connectors import ConnectorFactory
from src.framework.config import ConnectorConfig, CentralizedPipelineConfig

@dlt.table
def api_data_workflow():
    """Load via workflow-optimized datasource."""
    connector_config = ConnectorConfig(
        connector_type="rest_api_workflow_ds",
        config={
            "url": "https://api.example.com/data",
            "method": "GET",
            "workflow_id": "api_ingestion_workflow"
        }
    )
    
    connector = ConnectorFactory.create_connector(
        "rest_api_workflow_ds",
        connector_config,
        centralized
    )
    
    return connector.load(spark)
```

### With Task Dependencies

```python
@dlt.table
def api_bronze():
    """Bronze layer - load raw data."""
    connector_config = ConnectorConfig(
        connector_type="rest_api_workflow_ds",
        config={
            "url": "https://api.example.com/data",
            "workflow_id": "api_pipeline",
            "task_id": "load_bronze",
            "dependencies": ["validate_connection"]
        }
    )
    
    connector = ConnectorFactory.create_connector(
        "rest_api_workflow_ds",
        connector_config,
        centralized
    )
    
    return connector.load(spark)
```

### With Retry Configuration

```python
@dlt.table
def api_data_with_retry():
    """Load with workflow retry logic."""
    connector_config = ConnectorConfig(
        connector_type="rest_api_workflow_ds",
        config={
            "url": "https://api.example.com/data",
            "method": "GET",
            "max_retries": 5,
            "workflow_id": "api_ingestion"
        }
    )
    
    connector = ConnectorFactory.create_connector(
        "rest_api_workflow_ds",
        connector_config,
        centralized
    )
    
    return connector.load(spark)
```

## Workflow Orchestration

### Multi-Table Pipeline

```python
import dlt

# Task 1: Validate connection
@dlt.view
def validate_connection():
    """Validate API connectivity."""
    # Validation logic
    return spark.createDataFrame([(True,)])

# Task 2: Load bronze data
@dlt.table
def api_bronze():
    """Load raw API data."""
    connector = create_workflow_datasource_connector(
        task_id="load_bronze",
        dependencies=["validate_connection"]
    )
    return connector.load(spark)

# Task 3: Clean silver data
@dlt.table
def api_silver():
    """Clean and validate data."""
    return spark.sql("""
        SELECT * FROM LIVE.api_bronze
        WHERE _corrupt_record IS NULL
    """)

# Task 4: Create gold aggregates
@dlt.table
def api_gold():
    """Aggregate metrics."""
    return spark.sql("""
        SELECT 
            DATE(created_at) as date,
            COUNT(*) as record_count
        FROM LIVE.api_silver
        GROUP BY DATE(created_at)
    """)
```

### Workflow Definition (YAML)

```yaml
name: "API Data Ingestion Pipeline"

job_clusters:
  - job_cluster_key: "dlt_cluster"
    new_cluster:
      spark_version: "14.3.x-scala2.12"
      node_type_id: "i3.xlarge"
      num_workers: 2

tasks:
  - task_key: "validate_connection"
    notebook_task:
      notebook_path: "/validate_api"
    job_cluster_key: "dlt_cluster"

  - task_key: "load_bronze"
    depends_on:
      - task_key: "validate_connection"
    pipeline_task:
      pipeline_id: "api_ingestion_pipeline"
    job_cluster_key: "dlt_cluster"

  - task_key: "monitor_quality"
    depends_on:
      - task_key: "load_bronze"
    notebook_task:
      notebook_path: "/monitor_quality"
    job_cluster_key: "dlt_cluster"
```

## Checkpointing

### Automatic Checkpointing

```python
@dlt.table
def api_data_checkpointed():
    """Load with automatic checkpointing."""
    connector_config = ConnectorConfig(
        connector_type="rest_api_workflow_ds",
        config={
            "url": "https://api.example.com/data",
            "checkpoint_location": "s3://bucket/.checkpoints/api_data"
        }
    )
    
    connector = ConnectorFactory.create_connector(
        "rest_api_workflow_ds",
        connector_config,
        centralized
    )
    
    return connector.load(spark)
```

## Error Handling and Retries

### Workflow-Level Retries

```python
@dlt.table
def api_data_resilient():
    """Load with workflow retry strategy."""
    connector_config = ConnectorConfig(
        connector_type="rest_api_workflow_ds",
        config={
            "url": "https://api.example.com/data",
            "max_retries": 3,
            "workflow_id": "api_ingestion"
        }
    )
    
    try:
        connector = ConnectorFactory.create_connector(
            "rest_api_workflow_ds",
            connector_config,
            centralized
        )
        return connector.load(spark)
    except Exception as e:
        print(f"Error in workflow task: {e}")
        raise
```

### Task Failure Notifications

```python
import json

def log_task_status(task_id, status, error=None):
    """Log task status for monitoring."""
    status_data = {
        "task_id": task_id,
        "status": status,
        "timestamp": datetime.now().isoformat()
    }
    
    if error:
        status_data["error"] = str(error)
    
    print(json.dumps(status_data))

@dlt.table
def api_data_monitored():
    """Load with status monitoring."""
    task_id = "load_api_data"
    
    try:
        log_task_status(task_id, "started")
        
        connector = create_workflow_datasource_connector()
        df = connector.load(spark)
        
        log_task_status(task_id, "completed", None)
        return df
        
    except Exception as e:
        log_task_status(task_id, "failed", e)
        raise
```

## Performance Optimization

### Workflow-Aware Batching

```python
@dlt.table
def api_data_optimized():
    """Load with workflow optimization."""
    connector_config = ConnectorConfig(
        connector_type="rest_api_workflow_ds",
        config={
            "url": "https://api.example.com/data",
            "batch_size": 5000,  # Optimized for workflow
            "parallel_partitions": 4
        }
    )
    
    connector = ConnectorFactory.create_connector(
        "rest_api_workflow_ds",
        connector_config,
        centralized
    )
    
    return connector.load(spark)
```

## Monitoring and Observability

### Metrics Collection

```python
@dlt.table
def api_data_with_metrics():
    """Load with metrics collection."""
    connector = create_workflow_datasource_connector()
    df = connector.load(spark)
    
    # Collect metrics
    metrics = {
        "row_count": df.count(),
        "columns": len(df.columns),
        "null_counts": {col: df.filter(col.isNull()).count() 
                        for col in df.columns}
    }
    
    print(f"Metrics: {metrics}")
    
    return df
```

### Quality Metrics

```python
@dlt.table
def api_quality_metrics():
    """Compute quality metrics."""
    return spark.sql("""
        SELECT 
            COUNT(*) as total_records,
            COUNT(*) - COUNT(id) as missing_ids,
            COUNT(DISTINCT id) as unique_ids,
            ROUND(100.0 * (COUNT(*) - COUNT(id)) / COUNT(*), 2) as null_percentage
        FROM LIVE.api_bronze
    """)
```

## Integration with Job Schedules

### Scheduled Workflow

```python
# In Databricks Job Config
{
    "name": "api_ingestion_daily",
    "type": "pipeline",
    "pipeline_id": "api_ingestion_pipeline",
    "schedule": {
        "quartz_cron_expression": "0 0 * * * ?",  # Daily at midnight
        "timezone_id": "UTC"
    },
    "max_concurrent_runs": 1
}
```

## Best Practices

### 1. **Task Organization**

✓ Separate concerns into distinct tasks
✓ Define clear task dependencies
✓ Use meaningful task IDs

### 2. **Error Handling**

✓ Implement retry logic at workflow level
✓ Log errors comprehensively
✓ Set up alerting for failures

### 3. **Performance**

✓ Use checkpointing for large datasets
✓ Optimize batch sizes for API limits
✓ Monitor task execution time

### 4. **Monitoring**

✓ Log all task statuses
✓ Collect performance metrics
✓ Track data quality metrics

## Configuration Builder

```python
from src.framework.config import ConnectorConfigBuilderFactory

builder = ConnectorConfigBuilderFactory.create_builder("rest_api_workflow_ds")

connector_config = (builder
    .with_url("https://api.example.com/data")
    .with_method("GET")
    .with_workflow_id("api_ingestion")
    .build()
)
```

## Related Classes

- [BaseConnector](../base_connector.md) - Base class
- [RestApiConnector](./rest_api_connector.md) - Direct REST API connector
- [RestApiDatasource](./rest_api_datasource.md) - DLT datasource variant

## See Also

- [../QUICK_REFERENCE.md](../QUICK_REFERENCE.md) - Quick examples
- [../README.md](../README.md) - Connectors overview
- [README.md](./README.md) - Datasources overview
- [Delta Live Tables Docs](https://docs.databricks.com/en/delta-live-tables/)
