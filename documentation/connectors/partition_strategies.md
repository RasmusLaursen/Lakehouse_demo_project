# PartitionStrategies

## Overview

`PartitionStrategies` provides various strategies for partitioning data during ingestion. It enables optimized data loading based on source characteristics and performance requirements.

**Location**: `src/framework/connectors/partition_strategies.py`

**Pattern**: Strategy Pattern

## Available Strategies

### Strategy Types

| Strategy | Class | Use Case |
|----------|-------|----------|
| **NoPartition** | NoPartitionStrategy | Single partition for small data |
| **DateRange** | DateRangePartitionStrategy | Date-based partitioning |
| **Sequential** | SequentialPartitionStrategy | Sequential ID range partitioning |
| **Custom** | CustomPartitionStrategy | User-defined partitioning |

## Strategy Classes

### NoPartitionStrategy

Single partition - no parallelization.

```python
class NoPartitionStrategy:
    """No partitioning - load as single partition."""
    
    def get_partitions(self) -> int:
        """Return 1 (no partitioning)."""
        return 1
```

**Use Case**: Small datasets, development/testing

**Example**:
```python
strategy = NoPartitionStrategy()
# Returns single partition
```

### DateRangePartitionStrategy

Partition by date ranges.

```python
class DateRangePartitionStrategy:
    """Partition data by date ranges."""
    
    def __init__(self, start_date: str, end_date: str, 
                 partition_interval: str = "day"):
        """
        Initialize date range partitioning.
        
        Args:
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            partition_interval: "day", "week", "month"
        """
```

**Parameters**:
- `start_date` (str): Start date in YYYY-MM-DD format
- `end_date` (str): End date in YYYY-MM-DD format
- `partition_interval` (str): "day", "week", or "month"

**Example**:
```python
strategy = DateRangePartitionStrategy(
    start_date="2024-01-01",
    end_date="2024-12-31",
    partition_interval="month"
)
# Creates 12 monthly partitions
```

### SequentialPartitionStrategy

Partition by sequential ID ranges.

```python
class SequentialPartitionStrategy:
    """Partition by sequential ID ranges."""
    
    def __init__(self, min_id: int, max_id: int, 
                 partitions: int = 10):
        """
        Initialize sequential partitioning.
        
        Args:
            min_id: Minimum ID value
            max_id: Maximum ID value
            partitions: Number of partitions
        """
```

**Parameters**:
- `min_id` (int): Minimum ID value
- `max_id` (int): Maximum ID value
- `partitions` (int): Desired number of partitions

**Example**:
```python
strategy = SequentialPartitionStrategy(
    min_id=1,
    max_id=1000000,
    partitions=10
)
# Creates 10 partitions of 100k IDs each
```

## Usage Examples

### In Connector Configuration

```python
from src.framework.connectors.partition_strategies import DateRangePartitionStrategy
from src.framework.config import ConnectorConfig

# Create strategy
strategy = DateRangePartitionStrategy(
    start_date="2024-01-01",
    end_date="2024-12-31",
    partition_interval="month"
)

# Use in connector config
connector_config = ConnectorConfig(
    connector_type="rest_api",
    config={
        "url": "https://api.example.com",
        "partition_strategy": strategy
    }
)
```

### With REST API Connector

```python
from src.framework.connectors import ConnectorFactory

# Config with partitioning
connector_config = ConnectorConfig(
    connector_type="rest_api",
    config={
        "url": "https://api.example.com/data",
        "query_params": {
            "date_from": "{partition_start}",
            "date_to": "{partition_end}"
        },
        "partition_strategy": DateRangePartitionStrategy(
            start_date="2024-01-01",
            end_date="2024-12-31",
            partition_interval="month"
        )
    }
)

connector = ConnectorFactory.create_connector(
    "rest_api",
    connector_config,
    centralized
)

# Load - automatically parallelized by partitions
df = connector.load(spark)
```

### Selecting Strategy Based on Data

```python
def get_partition_strategy(source_type, data_size):
    """Select appropriate partitioning strategy."""
    
    if data_size < 10_000_000:  # Small data
        return NoPartitionStrategy()
    
    elif source_type == "time_series":
        return DateRangePartitionStrategy(
            start_date="2024-01-01",
            end_date="2024-12-31",
            partition_interval="month"
        )
    
    elif source_type == "sequential_ids":
        return SequentialPartitionStrategy(
            min_id=1,
            max_id=data_size,
            partitions=10
        )
    
    else:
        return NoPartitionStrategy()

# Use
strategy = get_partition_strategy("time_series", 100_000_000)
```

## Performance Considerations

### 1. **Partition Count**

```python
# Too few partitions - underutilized cluster
strategy = SequentialPartitionStrategy(
    min_id=1, max_id=1000000, partitions=2
)

# Better - matches executor count
strategy = SequentialPartitionStrategy(
    min_id=1, max_id=1000000, partitions=16
)
```

### 2. **Partition Balance**

Ensure even distribution:

```python
# Date range - ensures even time chunks
strategy = DateRangePartitionStrategy(
    start_date="2024-01-01",
    end_date="2024-12-31",
    partition_interval="month"  # 30-31 days each
)
```

### 3. **Data Skew Prevention**

```python
# Avoid skewed partitions
strategy = SequentialPartitionStrategy(
    min_id=1,
    max_id=1000000,
    partitions=20  # More partitions to distribute
)
```

## Testing Partition Strategies

### Test Patterns

```python
class TestDateRangePartitionStrategy:
    def test_daily_partitions(self):
        """Test daily partitioning."""
        strategy = DateRangePartitionStrategy(
            start_date="2024-01-01",
            end_date="2024-01-03",
            partition_interval="day"
        )
        
        # Should create 3 partitions
        assert strategy.get_partitions() == 3
    
    def test_monthly_partitions(self):
        """Test monthly partitioning."""
        strategy = DateRangePartitionStrategy(
            start_date="2024-01-01",
            end_date="2024-12-31",
            partition_interval="month"
        )
        
        # Should create 12 partitions
        assert strategy.get_partitions() == 12


class TestSequentialPartitionStrategy:
    def test_even_distribution(self):
        """Test even partition distribution."""
        strategy = SequentialPartitionStrategy(
            min_id=1, max_id=1000, partitions=10
        )
        
        # Should create 10 partitions
        assert strategy.get_partitions() == 10
        
        # Each partition should have 100 IDs
        partitions = strategy.get_partition_ranges()
        for part in partitions:
            assert part["max"] - part["min"] == 100
```

## Configuration Integration

### In Connector Config Builder

```python
from src.framework.config import ConnectorConfigBuilderFactory

builder = ConnectorConfigBuilderFactory.create_builder("rest_api")
config = (builder
    .with_url("https://api.example.com")
    .with_partition_strategy(
        DateRangePartitionStrategy(
            start_date="2024-01-01",
            end_date="2024-12-31"
        )
    )
    .build()
)
```

## Related Classes

- [RestApiConnector](./datasources/rest_api_connector.md) - Uses strategies
- [JdbcConnector](./datasources/jdbc_connector.md) - Uses strategies
- [AutoLoaderConnector](./datasources/autoloader_connector.md) - May use strategies

## See Also

- [QUICK_REFERENCE.md](./QUICK_REFERENCE.md) - Quick lookup
- [ARCHITECTURE.md](./ARCHITECTURE.md) - Design patterns
