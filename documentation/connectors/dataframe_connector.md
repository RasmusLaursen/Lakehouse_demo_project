# DataFrameConnector

## Overview

`DataFrameConnector` is a connector for directly using Spark DataFrames as data sources. It's useful for testing, working with in-memory data, or integrating with existing DataFrame-based workflows.

**Location**: `src/framework/connectors/dataframe_connector.py`

**Extends**: `BaseConnector`

## Class Definition

```python
class DataFrameConnector(BaseConnector):
    """Connector for using Spark DataFrames directly."""
```

## Key Responsibilities

1. **DataFrame Storage**: Store DataFrame for loading
2. **Schema Management**: Handle optional schema application
3. **Validation**: Verify DataFrame is valid
4. **Resource Cleanup**: Clean up DataFrame references

## Constructor

```python
def __init__(self, connector_config: ConnectorConfig, 
             centralized_config: CentralizedPipelineConfig):
    """
    Initialize DataFrame connector.
    
    Args:
        connector_config: Connector configuration
        centralized_config: Shared pipeline configuration
    """
    super().__init__(connector_config, centralized_config)
    self.dataframe = None
```

## Core Methods

### validate()

Validate connector configuration.

```python
def validate(self) -> None:
    """
    Validate DataFrame connector configuration.
    
    Raises:
        ConfigurationException: If validation fails
    """
```

**Validation Checks**:
- Required configuration fields present
- DataFrame source specified if needed
- Configuration format valid

### load()

Load DataFrame (or pass through existing).

```python
def load(self, spark: SparkSession, schema=None) -> DataFrame:
    """
    Load or return DataFrame.
    
    Args:
        spark: SparkSession instance
        schema: Optional schema to apply
        
    Returns:
        DataFrame
        
    Raises:
        ConnectorException: If load fails
    """
```

**Parameters**:
- `spark` (SparkSession): Active Spark session
- `schema` (optional): StructType schema

**Returns**: `DataFrame` - Spark DataFrame

### close()

Clean up connector resources.

```python
def close(self) -> None:
    """Clean up DataFrame connector resources."""
```

## Configuration

### Required Fields

| Field | Type | Description |
|-------|------|-------------|
| dataframe | DataFrame | The DataFrame to use |

### Optional Fields

| Field | Type | Description |
|-------|------|-------------|
| sample_rows | int | Number of rows to sample |
| cache | bool | Cache DataFrame in memory |

## Usage Examples

### Basic Usage

```python
from src.framework.connectors import ConnectorFactory
from src.framework.config import ConnectorConfig, CentralizedPipelineConfig

# Create test data
data = [("John", 30), ("Jane", 25)]
columns = ["name", "age"]
df = spark.createDataFrame(data, schema=columns)

# Configure
connector_config = ConnectorConfig(
    connector_type="dataframe",
    config={"dataframe": df}
)

centralized = CentralizedPipelineConfig(
    source_name="test_source",
    target_schema="bronze"
)

# Create connector
connector = ConnectorFactory.create_connector(
    "dataframe",
    connector_config,
    centralized
)

# Load
result = connector.load(spark)
result.show()
```

### With Schema Application

```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Create schema
schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True)
])

# Load with schema
df = connector.load(spark, schema=schema)
```

### In Testing

```python
import pytest

def test_data_transformation():
    """Test with DataFrame connector."""
    # Create test data
    test_data = [("John", 30), ("Jane", 25)]
    
    # Create connector with test data
    connector_config = ConnectorConfig(
        connector_type="dataframe",
        config={"dataframe": spark.createDataFrame(test_data, ["name", "age"])}
    )
    
    connector = ConnectorFactory.create_connector(
        "dataframe",
        connector_config,
        centralized
    )
    
    # Get data
    df = connector.load(spark)
    
    # Assert
    assert df.count() == 2
    assert df.columns == ["name", "age"]
```

## Use Cases

### 1. **Testing**

Test data transformation logic without external dependencies.

```python
# Test DataFrame
test_df = spark.createDataFrame([
    ("test1", "value1"),
    ("test2", "value2")
], ["id", "value"])

connector = ConnectorFactory.create_connector(
    "dataframe",
    ConnectorConfig("dataframe", {"dataframe": test_df}),
    centralized
)
```

### 2. **Development & Debugging**

Work with small datasets during development.

```python
# Small test dataset
dev_data = spark.createDataFrame([
    {"id": 1, "name": "John", "age": 30},
    {"id": 2, "name": "Jane", "age": 25}
], "id int, name string, age int")

connector = ConnectorFactory.create_connector(
    "dataframe",
    ConnectorConfig("dataframe", {"dataframe": dev_data}),
    centralized
)
```

### 3. **Integration with Existing DataFrames**

Integrate with existing PySpark workflows.

```python
# Existing DataFrame
existing_df = spark.sql("SELECT * FROM existing_table")

connector = ConnectorFactory.create_connector(
    "dataframe",
    ConnectorConfig("dataframe", {"dataframe": existing_df}),
    centralized
)
```

## Configuration Builder

### Using ConnectorConfigBuilder

```python
from src.framework.config import ConnectorConfigBuilderFactory

builder = ConnectorConfigBuilderFactory.create_builder("dataframe")
connector_config = builder.build()

# Or with DataFrame reference
connector_config = ConnectorConfig(
    connector_type="dataframe",
    config={"dataframe": my_df}
)
```

## Error Handling

### Invalid Configuration

```python
try:
    connector = ConnectorFactory.create_connector(
        "dataframe",
        ConnectorConfig("dataframe", {}),  # Missing dataframe
        centralized
    )
    connector.validate()
except ConfigurationException as e:
    print(f"Configuration error: {e}")
```

### Load Failures

```python
try:
    df = connector.load(spark)
except ConnectorException as e:
    print(f"Failed to load: {e}")
```

## Performance Considerations

### 1. **Caching**

```python
connector_config = ConnectorConfig(
    connector_type="dataframe",
    config={
        "dataframe": df,
        "cache": True  # Cache for better performance
    }
)
```

### 2. **Sampling**

```python
connector_config = ConnectorConfig(
    connector_type="dataframe",
    config={
        "dataframe": df,
        "sample_rows": 1000  # Sample 1000 rows
    }
)
```

## Lifecycle Example

```python
# Create
connector_config = ConnectorConfig(
    connector_type="dataframe",
    config={"dataframe": df}
)

connector = ConnectorFactory.create_connector(
    "dataframe",
    connector_config,
    centralized
)

try:
    # Validate
    connector.validate()
    
    # Load
    result_df = connector.load(spark)
    
    # Process
    result_df.show()
    
finally:
    # Cleanup
    connector.close()
```

## Testing

### Test Pattern

```python
class TestDataFrameConnector:
    def test_load_returns_dataframe(self):
        """Test connector loads DataFrame."""
        test_df = spark.createDataFrame([("a",)], ["col"])
        
        connector = ConnectorFactory.create_connector(
            "dataframe",
            ConnectorConfig("dataframe", {"dataframe": test_df}),
            centralized
        )
        
        result = connector.load(spark)
        assert result.count() == 1
    
    def test_schema_application(self):
        """Test schema is applied correctly."""
        # Test data without explicit schema
        test_df = spark.createDataFrame([("John", 30)])
        
        # Apply schema on load
        schema = StructType([
            StructField("name", StringType()),
            StructField("age", IntegerType())
        ])
        
        connector = ConnectorFactory.create_connector(
            "dataframe",
            ConnectorConfig("dataframe", {"dataframe": test_df}),
            centralized
        )
        
        result = connector.load(spark, schema=schema)
        assert result.schema == schema
```

## Related Classes

- [BaseConnector](./base_connector.md) - Abstract base class
- [ConnectorFactory](./connector_factory.md) - Factory for creation
- [ConnectorConfig](../configuration/connector_config.md) - Configuration

## See Also

- [QUICK_REFERENCE.md](./QUICK_REFERENCE.md) - Quick lookup
- [ARCHITECTURE.md](./ARCHITECTURE.md) - Design patterns
- [test_connectors.py](../../../tests/unit/test_connectors.py) - Test examples
