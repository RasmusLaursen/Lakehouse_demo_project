# Connectors Architecture

## Design Overview

The connector framework follows a layered architecture with multiple design patterns working together to provide flexible, extensible data source integration.

## Architectural Layers

```
┌─────────────────────────────────────────────────────┐
│           Spark / Delta Live Tables                 │
│          (Data Loading Entry Points)                │
└────────────────────┬────────────────────────────────┘
                     │
┌────────────────────▼────────────────────────────────┐
│     ConnectorFactory (Factory Pattern)              │
│     (Instantiate appropriate connector)             │
└────────────────────┬────────────────────────────────┘
                     │
┌────────────────────▼────────────────────────────────┐
│         BaseConnector (Template Method)             │
│     (Define connector interface contract)           │
└────────────────────┬────────────────────────────────┘
                     │
   ┌─────────────────┼─────────────────┬──────────────┐
   │                 │                 │              │
┌──▼────────────────┐│  ┌──────────────▼─┐  ┌────────▼──┐
│ DataFrame         │ │  │  REST API      │  │  JDBC    │
│ Connector         │ │  │  Connector     │  │ Connector│
└──────────────────┘│  │                 │  └──────────┘
                   │  │  AutoLoader     │
┌──────────────────┬──▼──┴──────────────┘
│  Support Classes │
│ - OAuth2Manager  │
│ - Partition      │
│   Strategies     │
│ - PySparkAdapter │
└──────────────────┘
```

## Core Design Patterns

### 1. **Factory Pattern** (ConnectorFactory)

**Purpose**: Encapsulate connector creation logic

**How It Works**:
```python
# Client doesn't know how to create specific connectors
connector = ConnectorFactory.create_connector(
    "rest_api",           # Type
    connector_config,     # Config
    centralized_config    # Shared config
)
# Factory handles instantiation internally
```

**Benefits**:
- Centralized creation logic
- Easy to add new connector types
- Type validation before instantiation
- Decoupled client code from connector classes

### 2. **Template Method Pattern** (BaseConnector)

**Purpose**: Define algorithm structure, let subclasses fill in details

**How It Works**:
```python
class BaseConnector:
    def load(self, spark, schema=None):
        # Template method - defines structure
        self.validate()           # Step 1: Validate
        data = self._fetch_data() # Step 2: Fetch (implemented by subclass)
        return self._transform(data, schema)  # Step 3: Transform

class RestApiConnector(BaseConnector):
    def _fetch_data(self):
        # Subclass-specific implementation
        return self._call_rest_api()
```

**Benefits**:
- Enforces consistent connector behavior
- Reduces code duplication
- Easier to add new connector types
- Clear lifecycle (validate → fetch → transform)

### 3. **Strategy Pattern** (PartitionStrategies)

**Purpose**: Encapsulate different partitioning algorithms

**How It Works**:
```python
# Different strategies for different scenarios
date_strategy = DateRangePartitionStrategy(
    start_date="2024-01-01",
    end_date="2024-12-31"
)

id_strategy = SequentialPartitionStrategy(
    min_id=1, max_id=1000000, partitions=10
)

# Connector uses strategy transparently
connector_config = ConnectorConfig(
    type="rest_api",
    config={...},
    partition_strategy=date_strategy  # Pluggable strategy
)
```

**Benefits**:
- Different partitioning for different data types
- Pluggable strategies
- Easy to add new strategies
- Optimal performance for specific data patterns

### 4. **Adapter Pattern** (PySparkDatasourceAdapter)

**Purpose**: Bridge custom connectors with PySpark datasource framework

**How It Works**:
```python
# Adapt any connector to PySpark
connector = ConnectorFactory.create_connector("rest_api", config, centralized)
adapter = PySparkDatasourceAdapter(connector)
adapter.register_datasource("my_source")

# Now usable in Spark SQL
df = spark.read.format("my_source").load()
```

**Benefits**:
- Seamless PySpark integration
- Use connectors in Spark SQL
- Support for Delta Live Tables
- Type-safe schema mapping

## Configuration Flow

```
ConnectorConfigBuilderFactory
         │
         ▼
   (creates appropriate builder)
         │
         ├─ VolumeConfigBuilder
         ├─ RestApiConfigBuilder
         ├─ JdbcConfigBuilder
         ├─ AutoLoaderConfigBuilder
         │
         ▼
   ConnectorConfigBuilder.build()
         │
         ▼
   ConnectorConfig (built configuration)
         │
    ┌────┴────┐
    │          │
    ▼          ▼
ConnectorFactory    PySparkAdapter
    │          │
    ▼          ▼
BaseConnector  (adapts for Spark)
    │
    ├─ RestApiConnector
    ├─ JdbcConnector
    ├─ AutoLoaderConnector
    └─ DataFrameConnector
```

## Data Loading Flow

### 1. **Creation Phase**

```python
# Build configuration
builder = ConnectorConfigBuilderFactory.create_builder("rest_api")
config = builder.with_url("...").build()

# Create connector via factory
connector = ConnectorFactory.create_connector(
    "rest_api", config, centralized
)
```

### 2. **Validation Phase**

```python
# Validate before loading
connector.validate()
# Checks: config completeness, connectivity, credentials
```

### 3. **Loading Phase**

```python
# Load data
df = connector.load(spark, schema=optional_schema)
# Subclass-specific loading logic
```

### 4. **Cleanup Phase**

```python
# Always cleanup
connector.close()
# Close connections, release resources
```

## Support Classes Integration

### OAuth2TokenManager

Used by REST API connectors for authentication:

```
RestApiConnector
    │
    ├─ validate()
    │    ├─ OAuth2TokenManager.verify_credentials()
    │
    └─ load()
         └─ OAuth2TokenManager.get_token()
              ├─ Check cache
              ├─ Refresh if needed
              └─ Return token
```

### PartitionStrategies

Used by connectors for parallel data loading:

```
Connector.load()
    │
    └─ partition_strategy.get_partitions()
        ├─ DateRangePartitionStrategy → Monthly chunks
        ├─ SequentialPartitionStrategy → ID ranges
        └─ NoPartitionStrategy → Single partition
             │
             ▼
        Parallel load per partition
```

### PySparkDatasourceAdapter

Bridges connectors to PySpark ecosystem:

```
Connector
    │
    ├─ Adapter.register_datasource()
    │
    └─ SparkSession.read.format("name").load()
         │
         └─ Adapter translates Spark ops → Connector calls
```

## Error Handling Architecture

### Exception Hierarchy

```
Exception
├─ ConnectorException
│   ├─ ConfigurationException
│   ├─ ValidationException
│   ├─ ConnectionException
│   └─ DataLoadException
├─ TokenException
│   ├─ TokenAcquisitionException
│   └─ TokenRefreshException
└─ SchemaException
   ├─ SchemaMismatchException
   └─ SchemaInferenceException
```

### Error Propagation

```python
try:
    connector.validate()      # Raises ConfigurationException
except ConfigurationException:
    # Handle config error
    pass

try:
    df = connector.load(spark)  # Raises DataLoadException
except DataLoadException:
    # Handle load error
    pass
```

## Extensibility Points

### Adding New Connector Type

1. **Create Connector Class**
   ```python
   class MyConnector(BaseConnector):
       def validate(self): ...
       def load(self, spark, schema=None): ...
       def close(self): ...
   ```

2. **Register with Factory**
   ```python
   ConnectorFactory.register_connector_type("my_type", MyConnector)
   ```

3. **Create Builder** (Optional)
   ```python
   class MyConfigBuilder(BaseConfigBuilder):
       def build(self): ...
   ```

4. **Add Tests**
   ```python
   class TestMyConnector:
       def test_load(): ...
       def test_validate(): ...
   ```

### Adding New Strategy

1. **Implement Strategy Interface**
   ```python
   class MyStrategy:
       def get_partitions(self) -> int: ...
   ```

2. **Use in Connector Config**
   ```python
   config = ConnectorConfig(
       type="rest_api",
       config={...},
       partition_strategy=MyStrategy()
   )
   ```

## Performance Optimization

### 1. **Partitioning Strategy**

- **DateRange**: Parallelize by time periods
- **Sequential**: Parallelize by ID ranges
- **NoPartition**: For small data

### 2. **Connection Pooling**

- Reuse connections across loads
- Close when done

### 3. **Caching**

- OAuth2 tokens cached locally
- Reduce authentication overhead
- Auto-refresh before expiry

### 4. **Schema Caching**

- Cache inferred schemas
- Avoid repeated schema discovery

## Security Considerations

### Configuration Security

```python
# ❌ Don't embed secrets
config = {
    "api_key": "sk-1234567890"  # EXPOSED!
}

# ✅ Use SecretResolver
secret = centralized_config.secret_resolver.resolve("api_key")
config = {"api_key": secret}
```

### Token Management

- Tokens stored in memory
- Refreshed automatically
- Never logged or exposed

### Credential Handling

- Separate from connector code
- Managed by configuration layer
- Centralized resolution

## Related Documentation

- [Configuration System](../configuration/README.md)
- [BaseConnector](./base_connector.md)
- [ConnectorFactory](./connector_factory.md)
- [Partition Strategies](./partition_strategies.md)
- [PySparkDatasourceAdapter](./pyspark_datasource_adapter.md)
- [OAuth2TokenManager](./oauth2_token_manager.md)

## See Also

- [QUICK_REFERENCE.md](./QUICK_REFERENCE.md) - Quick lookup
- [datasources/README.md](./datasources/README.md) - Datasource variants
- [INDEX.md](./INDEX.md) - Documentation index
