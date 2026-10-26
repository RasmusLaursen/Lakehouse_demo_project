# AutoLoaderConnector

## Overview

`AutoLoaderConnector` is a connector for loading files from cloud storage using Databricks AutoLoader. It supports automatic schema inference, schema evolution, and incremental data loading from S3, Azure Storage, and Google Cloud Storage.

**Location**: `src/framework/connectors/autoloader_connector.py`

**Extends**: `BaseConnector`

**Type**: `"autoloader"`

## Key Features

- **Cloud Storage Support**: S3, Azure Blob Storage, Google Cloud Storage
- **Automatic Schema Inference**: Automatically infer schema from files
- **Schema Evolution**: Track and handle schema changes
- **Incremental Loading**: Load only new/modified files
- **File Format Support**: CSV, JSON, Parquet, Delta
- **Streaming Support**: Real-time file ingestion
- **Checkpoint Management**: State management for incremental loads

## Class Definition

```python
class AutoLoaderConnector(BaseConnector):
    """Connector for cloud file loading via AutoLoader."""
```

## Configuration

### Required Fields

| Field | Type | Description |
|-------|------|-------------|
| source_path | str | Cloud storage path (s3://, abfss://, gs://) |
| format | str | File format (csv, json, parquet) |
| schema_location | str | Schema tracking location |

### Optional Fields

| Field | Type | Description |
|-------|------|-------------|
| schema_hint | str | Suggested schema (optional) |
| recursively_walk_dir | bool | Recursively load subdirectories |
| cloudFiles_schemaEvolutionMode | str | "addNewColumns", "failOnNewColumns" |
| cloudFiles_format | str | Format-specific configuration |
| max_files_per_trigger | int | Files per batch for streaming |

## Core Methods

### validate()

Validate AutoLoader configuration.

```python
def validate(self) -> None:
    """Validate AutoLoader connector configuration."""
```

### load()

Load files from cloud storage.

```python
def load(self, spark: SparkSession, schema=None) -> DataFrame:
    """
    Load files from cloud storage.
    
    Args:
        spark: SparkSession instance
        schema: Optional schema for data
        
    Returns:
        DataFrame with file data
    """
```

### close()

Clean up connector resources.

```python
def close(self) -> None:
    """Close AutoLoader connector."""
```

## Usage Examples

### Basic CSV Loading from S3

```python
from src.framework.connectors import ConnectorFactory
from src.framework.config import ConnectorConfig, CentralizedPipelineConfig

connector_config = ConnectorConfig(
    connector_type="autoloader",
    config={
        "source_path": "s3://my-bucket/data/csv/",
        "format": "csv",
        "schema_location": "s3://my-bucket/.schema/csv_schema"
    }
)

centralized = CentralizedPipelineConfig(
    source_name="csv_files",
    target_schema="bronze"
)

connector = ConnectorFactory.create_connector(
    "autoloader",
    connector_config,
    centralized
)

try:
    connector.validate()
    df = connector.load(spark)
    df.show()
finally:
    connector.close()
```

### JSON Files from S3

```python
connector_config = ConnectorConfig(
    connector_type="autoloader",
    config={
        "source_path": "s3://my-bucket/data/json/",
        "format": "json",
        "schema_location": "s3://my-bucket/.schema/json_schema",
        "cloudFiles_schemaEvolutionMode": "addNewColumns"
    }
)
```

### Parquet Files from Azure

```python
connector_config = ConnectorConfig(
    connector_type="autoloader",
    config={
        "source_path": "abfss://container@account.dfs.core.windows.net/data/parquet/",
        "format": "parquet",
        "schema_location": "abfss://container@account.dfs.core.windows.net/.schema/"
    }
)
```

### CSV Files from Google Cloud Storage

```python
connector_config = ConnectorConfig(
    connector_type="autoloader",
    config={
        "source_path": "gs://my-bucket/data/csv/",
        "format": "csv",
        "schema_location": "gs://my-bucket/.schema/"
    }
)
```

### With CSV Options

```python
connector_config = ConnectorConfig(
    connector_type="autoloader",
    config={
        "source_path": "s3://my-bucket/data/csv/",
        "format": "csv",
        "schema_location": "s3://my-bucket/.schema/",
        "cloudFiles_format": {
            "sep": ",",
            "header": "true",
            "inferSchema": "true"
        }
    }
)
```

### Recursive Directory Loading

```python
connector_config = ConnectorConfig(
    connector_type="autoloader",
    config={
        "source_path": "s3://my-bucket/data/",
        "format": "json",
        "schema_location": "s3://my-bucket/.schema/",
        "recursively_walk_dir": True
    }
)
```

## Delta Live Tables Integration

### Basic DLT Table

```python
import dlt

@dlt.table
def csv_data_bronze():
    """Load CSV files via AutoLoader."""
    connector_config = ConnectorConfig(
        connector_type="autoloader",
        config={
            "source_path": "s3://my-bucket/data/csv/",
            "format": "csv",
            "schema_location": "s3://my-bucket/.schema/csv_schema"
        }
    )
    
    connector = ConnectorFactory.create_connector(
        "autoloader",
        connector_config,
        centralized
    )
    
    return connector.load(spark)
```

### With Schema Evolution

```python
@dlt.table(
    schema_location="s3://my-bucket/.schema/json_schema"
)
def json_data_bronze():
    """Load JSON with schema evolution."""
    connector_config = ConnectorConfig(
        connector_type="autoloader",
        config={
            "source_path": "s3://my-bucket/data/json/",
            "format": "json",
            "schema_location": "s3://my-bucket/.schema/json_schema",
            "cloudFiles_schemaEvolutionMode": "addNewColumns"
        }
    )
    
    connector = ConnectorFactory.create_connector(
        "autoloader",
        connector_config,
        centralized
    )
    
    return connector.load(spark)
```

### Streaming Pattern

```python
import dlt

@dlt.table
def streaming_csv_data():
    """Stream CSV files in real-time."""
    connector_config = ConnectorConfig(
        connector_type="autoloader",
        config={
            "source_path": "s3://my-bucket/incoming/",
            "format": "csv",
            "schema_location": "s3://my-bucket/.schema/",
            "max_files_per_trigger": 100
        }
    )
    
    connector = ConnectorFactory.create_connector(
        "autoloader",
        connector_config,
        centralized
    )
    
    return connector.load(spark)
```

## File Format Support

### CSV Files

```python
{
    "source_path": "s3://bucket/data/csv/",
    "format": "csv",
    "schema_location": "s3://bucket/.schema/",
    "cloudFiles_format": {
        "sep": ",",
        "header": "true",
        "inferSchema": "true"
    }
}
```

### JSON Files

```python
{
    "source_path": "s3://bucket/data/json/",
    "format": "json",
    "schema_location": "s3://bucket/.schema/"
}
```

### Parquet Files

```python
{
    "source_path": "s3://bucket/data/parquet/",
    "format": "parquet",
    "schema_location": "s3://bucket/.schema/"
}
```

### Delta Files

```python
{
    "source_path": "s3://bucket/data/delta/",
    "format": "delta",
    "schema_location": "s3://bucket/.schema/"
}
```

## Schema Management

### Schema Inference

AutoLoader automatically infers schema from files.

```python
connector_config = ConnectorConfig(
    connector_type="autoloader",
    config={
        "source_path": "s3://bucket/data/",
        "format": "json",
        "schema_location": "s3://bucket/.schema/"
    }
)

# Schema automatically inferred and stored at schema_location
```

### Schema Evolution

Handle new columns as schema evolves.

```python
connector_config = ConnectorConfig(
    connector_type="autoloader",
    config={
        "source_path": "s3://bucket/data/",
        "format": "json",
        "schema_location": "s3://bucket/.schema/",
        "cloudFiles_schemaEvolutionMode": "addNewColumns"
    }
)
```

### Schema Tracking

Schemas automatically tracked in schema_location for evolution handling.

## Cloud Storage Paths

### S3 Paths

```python
"source_path": "s3://bucket-name/path/to/data/"
```

### Azure Blob Storage

```python
"source_path": "abfss://container-name@storage-account.dfs.core.windows.net/path/"
```

### Google Cloud Storage

```python
"source_path": "gs://bucket-name/path/to/data/"
```

## Incremental Loading

### Checkpoint-Based Incremental

```python
@dlt.table
def incremental_csv():
    """Load new files incrementally."""
    connector_config = ConnectorConfig(
        connector_type="autoloader",
        config={
            "source_path": "s3://bucket/data/",
            "format": "csv",
            "schema_location": "s3://bucket/.schema/",
            "cloudFiles_maxFileAge": "1 day"  # Only files < 1 day old
        }
    )
    
    connector = ConnectorFactory.create_connector(
        "autoloader",
        connector_config,
        centralized
    )
    
    return connector.load(spark)
```

## Error Handling

### Invalid Path

```python
try:
    connector.validate()
except ConfigurationException as e:
    if "path" in str(e).lower():
        print("Invalid cloud storage path")
```

### Schema Mismatch

```python
try:
    df = connector.load(spark)
except Exception as e:
    if "schema" in str(e).lower():
        print("Schema mismatch - check schema_location")
```

## Performance Optimization

### 1. **Batch Size Control**

```python
connector_config = ConnectorConfig(
    connector_type="autoloader",
    config={
        "source_path": "s3://bucket/data/",
        "format": "csv",
        "schema_location": "s3://bucket/.schema/",
        "max_files_per_trigger": 200  # Higher for better performance
    }
)
```

### 2. **File Partitioning**

Organize files in cloud storage with partition structure:

```
s3://bucket/data/
├── year=2024/
│   ├── month=01/
│   │   └── day=01/
│   │       ├── file1.csv
│   │       └── file2.csv
│   └── month=02/
└── year=2025/
```

### 3. **Format Selection**

- **Parquet**: Fastest, most efficient
- **JSON**: Good balance
- **CSV**: Slower, less efficient

## Configuration Builder

```python
from src.framework.config import ConnectorConfigBuilderFactory

builder = ConnectorConfigBuilderFactory.create_builder("autoloader")

connector_config = (builder
    .with_source_path("s3://my-bucket/data/")
    .with_format("csv")
    .with_schema_location("s3://my-bucket/.schema/")
    .build()
)
```

## Testing

### Test Patterns

```python
class TestAutoLoaderConnector:
    def test_load_csv_returns_dataframe(self):
        """Test CSV loading."""
        connector = ConnectorFactory.create_connector(
            "autoloader",
            autoloader_config,
            centralized
        )
        
        df = connector.load(spark)
        
        assert df is not None
        assert df.count() > 0
    
    def test_schema_tracking(self):
        """Test schema is tracked."""
        connector = ConnectorFactory.create_connector(
            "autoloader",
            autoloader_config,
            centralized
        )
        
        df = connector.load(spark)
        schema = df.schema
        
        assert schema is not None
        assert len(schema.fields) > 0
```

## Best Practices

### 1. **Path Organization**

✓ Organize by date partitions
✓ Use meaningful directory names
✓ Separate by file type

### 2. **Schema Management**

✓ Always specify schema_location
✓ Use schema evolution mode appropriately
✓ Monitor schema changes

### 3. **Performance**

✓ Use Parquet format when possible
✓ Organize files with partitioning
✓ Set appropriate batch sizes

### 4. **Monitoring**

✓ Monitor ingestion lag
✓ Track schema evolution
✓ Alert on failures

## Related Classes

- [BaseConnector](../base_connector.md) - Base class
- [ConnectorFactory](../connector_factory.md) - Factory
- [PartitionStrategies](../partition_strategies.md) - Partitioning

## See Also

- [../QUICK_REFERENCE.md](../QUICK_REFERENCE.md) - Quick examples
- [../README.md](../README.md) - Connectors overview
- [README.md](./README.md) - Datasources overview
- [Databricks AutoLoader Docs](https://docs.databricks.com/en/ingestion/autoloader/)
