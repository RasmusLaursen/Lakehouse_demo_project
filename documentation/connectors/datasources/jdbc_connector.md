# JdbcConnector

## Overview

`JdbcConnector` is a connector for loading data from databases via JDBC connections. It supports various database types (MySQL, PostgreSQL, SQL Server, etc.) and provides features for efficient data loading and partitioning.

**Location**: `src/framework/connectors/jdbc_connector.py`

**Extends**: `BaseConnector`

**Type**: `"jdbc"`

## Key Features

- **Multi-Database Support**: Any JDBC-compatible database
- **SQL Query Support**: Custom SQL queries for data extraction
- **Partitioned Loading**: Parallel data extraction via partitioning
- **Connection Pooling**: Efficient connection management
- **Type Mapping**: Automatic SQL type to Spark type conversion
- **Filtering**: WHERE clause support for data filtering

## Class Definition

```python
class JdbcConnector(BaseConnector):
    """Connector for JDBC database sources."""
```

## Configuration

### Required Fields

| Field | Type | Description |
|-------|------|-------------|
| url | str | JDBC connection URL |
| username | str | Database username |
| password | str | Database password |
| query | str | SQL query or table name |

### Optional Fields

| Field | Type | Description |
|-------|------|-------------|
| driver | str | JDBC driver class |
| fetch_size | int | Fetch size (default: 10000) |
| partition_column | str | Column for partitioning |
| partition_strategy | PartitionStrategy | Partitioning strategy |
| timeout | int | Connection timeout |

## Core Methods

### validate()

Validate JDBC configuration.

```python
def validate(self) -> None:
    """Validate JDBC connector configuration."""
```

### load()

Load data from database.

```python
def load(self, spark: SparkSession, schema=None) -> DataFrame:
    """
    Load data from JDBC database.
    
    Args:
        spark: SparkSession instance
        schema: Optional schema for data
        
    Returns:
        DataFrame with database records
    """
```

### close()

Clean up database connections.

```python
def close(self) -> None:
    """Close JDBC connections."""
```

## Usage Examples

### Basic Database Query

```python
from src.framework.connectors import ConnectorFactory
from src.framework.config import ConnectorConfig, CentralizedPipelineConfig

connector_config = ConnectorConfig(
    connector_type="jdbc",
    config={
        "url": "jdbc:mysql://localhost:3306/mydb",
        "username": "user",
        "password": "password",
        "query": "SELECT * FROM users"
    }
)

centralized = CentralizedPipelineConfig(
    source_name="mysql_db",
    target_schema="bronze"
)

connector = ConnectorFactory.create_connector(
    "jdbc",
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

### PostgreSQL Database

```python
connector_config = ConnectorConfig(
    connector_type="jdbc",
    config={
        "url": "jdbc:postgresql://localhost:5432/analytics_db",
        "username": "analyst",
        "password": "password",
        "query": "SELECT id, name, email FROM customers WHERE status = 'active'"
    }
)
```

### SQL Server Database

```python
connector_config = ConnectorConfig(
    connector_type="jdbc",
    config={
        "url": "jdbc:sqlserver://sql-server:1433;database=warehouse",
        "username": "sa",
        "password": "password",
        "query": "SELECT * FROM [dbo].[Sales] WHERE Year >= 2024"
    }
)
```

### With Partitioning

```python
from src.framework.connectors.partition_strategies import SequentialPartitionStrategy

strategy = SequentialPartitionStrategy(
    min_id=1,
    max_id=1000000,
    partitions=10
)

connector_config = ConnectorConfig(
    connector_type="jdbc",
    config={
        "url": "jdbc:mysql://localhost:3306/mydb",
        "username": "user",
        "password": "password",
        "query": "SELECT * FROM large_table",
        "partition_column": "id",
        "partition_strategy": strategy
    }
)
```

### With Custom Fetch Size

```python
connector_config = ConnectorConfig(
    connector_type="jdbc",
    config={
        "url": "jdbc:mysql://localhost:3306/mydb",
        "username": "user",
        "password": "password",
        "query": "SELECT * FROM products",
        "fetch_size": 50000  # Optimize for large result sets
    }
)
```

## Database-Specific URLs

### MySQL

```python
"url": "jdbc:mysql://hostname:3306/database_name"
```

### PostgreSQL

```python
"url": "jdbc:postgresql://hostname:5432/database_name"
```

### Oracle

```python
"url": "jdbc:oracle:thin:@hostname:1521:instance_name"
```

### SQL Server

```python
"url": "jdbc:sqlserver://hostname:1433;database=database_name"
```

### Teradata

```python
"url": "jdbc:teradata://hostname/database=database_name"
```

## Query Examples

### Simple SELECT

```python
"query": "SELECT * FROM users"
```

### With WHERE Clause

```python
"query": "SELECT id, name, email FROM users WHERE created_date >= '2024-01-01'"
```

### Join Multiple Tables

```python
"query": """
    SELECT 
        u.id, u.name, o.order_id, o.total
    FROM users u
    INNER JOIN orders o ON u.id = o.user_id
    WHERE u.status = 'active'
"""
```

### Aggregated Query

```python
"query": """
    SELECT 
        DATE(created_at) as date,
        COUNT(*) as order_count,
        SUM(total) as total_revenue
    FROM orders
    GROUP BY DATE(created_at)
"""
```

## Partitioning

### Partitioned Load Strategy

```python
from src.framework.connectors.partition_strategies import SequentialPartitionStrategy

# Identify partition column and range
strategy = SequentialPartitionStrategy(
    min_id=1,
    max_id=10000000,
    partitions=20
)

config = ConnectorConfig(
    connector_type="jdbc",
    config={
        "url": "...",
        "username": "...",
        "password": "...",
        "query": "SELECT * FROM large_table",
        "partition_column": "id",
        "partition_strategy": strategy
    }
)
```

### Benefits

- **Parallel Reading**: Data read in parallel across partitions
- **Better Performance**: Distributes load across cluster
- **Memory Efficient**: Smaller chunks processed individually

## Error Handling

### Connection Errors

```python
from src.framework.exceptions import ConnectorException

try:
    df = connector.load(spark)
except ConnectorException as e:
    if "connection refused" in str(e).lower():
        print("Cannot connect to database")
except Exception as e:
    print(f"Unexpected error: {e}")
```

### Authentication Errors

```python
from src.framework.exceptions import ConfigurationException

try:
    connector.validate()
except ConfigurationException as e:
    if "auth" in str(e).lower():
        print("Invalid credentials")
```

### Query Errors

```python
try:
    df = connector.load(spark)
except Exception as e:
    if "syntax" in str(e).lower():
        print("Invalid SQL query")
```

## Performance Optimization

### 1. **Fetch Size Tuning**

```python
# Larger fetch size for better performance (uses more memory)
config = ConnectorConfig(
    connector_type="jdbc",
    config={
        "url": "...",
        "fetch_size": 100000  # 100K records per fetch
    }
)
```

### 2. **Partitioning**

```python
# Parallel load across multiple partitions
strategy = SequentialPartitionStrategy(
    min_id=1,
    max_id=10000000,
    partitions=16  # Match executor count
)
```

### 3. **Query Optimization**

```python
# Optimize query for Spark
"query": """
    SELECT id, name, email
    FROM users
    WHERE status = 'active'
    AND created_date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
"""
```

### 4. **Connection Pooling**

Connector manages connection pooling automatically for efficiency.

## Using Secrets

```python
from src.framework.config import SecretResolver

secret_resolver = SecretResolver(centralized_config)

connector_config = ConnectorConfig(
    connector_type="jdbc",
    config={
        "url": "jdbc:mysql://localhost:3306/mydb",
        "username": secret_resolver.resolve("db_username"),
        "password": secret_resolver.resolve("db_password"),
        "query": "SELECT * FROM users"
    }
)
```

## Configuration Builder

```python
from src.framework.config import ConnectorConfigBuilderFactory

builder = ConnectorConfigBuilderFactory.create_builder("jdbc")

connector_config = (builder
    .with_url("jdbc:mysql://localhost:3306/mydb")
    .with_username(secret_resolver.resolve("db_user"))
    .with_password(secret_resolver.resolve("db_password"))
    .with_query("SELECT * FROM users")
    .build()
)
```

## Testing

### Test Patterns

```python
class TestJdbcConnector:
    def test_load_returns_dataframe(self):
        """Test connector returns DataFrame."""
        connector = ConnectorFactory.create_connector(
            "jdbc",
            jdbc_config,
            centralized
        )
        
        df = connector.load(spark)
        
        assert df is not None
        assert df.count() > 0
    
    def test_partitioned_load(self):
        """Test partitioned loading."""
        strategy = SequentialPartitionStrategy(
            min_id=1, max_id=100, partitions=10
        )
        
        config = ConnectorConfig(
            "jdbc",
            {**jdbc_config, "partition_strategy": strategy}
        )
        
        connector = ConnectorFactory.create_connector(
            "jdbc", config, centralized
        )
        
        df = connector.load(spark)
        assert df.rdd.getNumPartitions() >= 10
```

## Related Classes

- [BaseConnector](../base_connector.md) - Base class
- [ConnectorFactory](../connector_factory.md) - Factory
- [PartitionStrategies](../partition_strategies.md) - Partitioning
- [SecretResolver](../../configuration/secret_resolver.md) - Credential management

## See Also

- [../QUICK_REFERENCE.md](../QUICK_REFERENCE.md) - Quick examples
- [../README.md](../README.md) - Connectors overview
- [README.md](./README.md) - Datasources overview
