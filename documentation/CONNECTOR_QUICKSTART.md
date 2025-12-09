# Connector Framework - Quick Start Guide

## 5-Minute Setup

### Option 1: Using Existing Volume Connector (No Changes Needed!)

Your existing pipelines already use connectors under the hood. The data contracts have been updated:

```yaml
# data_contracts/source_system/lakehouse.yml
servers:
  - server: dev
    customProperties:
      - property: connector_type
        value: "volume"
      - property: connector_config
        value:
          add_audit_columns: true
```

**That's it!** Your pipelines work exactly as before.

---

### Option 2: Adding a REST API Data Source

**Step 1**: Create a new data contract `data_contracts/source_system/my_api.yml`

```yaml
kind: DataContract
apiVersion: v3.0.0
name: "My External API Data"
version: "1.0.0"

servers:
  - server: dev
    type: "rest_api"
    environment: "development"
    customProperties:
      - property: connector_type
        value: "rest_api"
      - property: connector_config
        value:
          endpoint: "https://api.example.com/v1/customers"
          method: "GET"
          auth_type: "bearer"
          auth_token: "${secrets.api_token}"
          pagination_type: "offset"
          pagination_config:
            limit: 100
          data_path: "data"

schema:
  - name: customers
    type: table
    customProperties:
      - property: scd_type
        value: 1
      - property: keys
        value: ["customer_id"]
    properties:
      - name: customer_id
        type: integer
        primaryKey: true
      - name: name
        type: string
      - name: email
        type: string
```

**Step 2**: Create raw pipeline `src/solution/raw/raw_ingest_my_api.py`

```python
from src.framework.pipelines.raw_factory import create_raw_pipeline

create_raw_pipeline("my_api")
```

**Done!** The connector framework handles everything else.

---

### Option 3: Adding a Database Source

**Step 1**: Create data contract `data_contracts/source_system/my_database.yml`

```yaml
kind: DataContract
apiVersion: v3.0.0
name: "PostgreSQL Database"
version: "1.0.0"

servers:
  - server: dev
    type: "jdbc"
    environment: "development"
    customProperties:
      - property: connector_type
        value: "jdbc"
      - property: connector_config
        value:
          url: "jdbc:postgresql://db.company.com:5432/production"
          user: "${secrets.db_user}"
          password: "${secrets.db_password}"
          partition_column: "id"
          lower_bound: 1
          upper_bound: 10000000
          num_partitions: 20

schema:
  - name: orders
    type: table
    customProperties:
      - property: scd_type
        value: 1
      - property: keys
        value: ["order_id"]
    properties:
      - name: order_id
        type: integer
        primaryKey: true
      - name: customer_id
        type: integer
      - name: order_date
        type: timestamp
      - name: total_amount
        type: decimal
```

**Step 2**: Create raw pipeline

```python
from src.framework.pipelines.raw_factory import create_raw_pipeline

create_raw_pipeline("my_database")
```

**Done!** Parallel reading across 20 partitions automatically configured.

---

## Common Patterns

### Pattern 1: API with Bearer Token Authentication

```yaml
connector_config:
  endpoint: "https://api.service.com/data"
  method: "GET"
  auth_type: "bearer"
  auth_token: "${secrets.api_token}"
  data_path: "results"
```

### Pattern 2: API with Pagination

```yaml
connector_config:
  endpoint: "https://api.service.com/users"
  method: "GET"
  pagination_type: "cursor"
  pagination_config:
    cursor_param: "next"
    cursor_path: "pagination.next_cursor"
```

### Pattern 3: Database with Incremental Loading

```yaml
connector_config:
  url: "jdbc:mysql://db.host.com:3306/mydb"
  table: "transactions"
  incremental_column: "updated_at"
  incremental_value: "2024-01-01 00:00:00"
```

### Pattern 4: Large Database Table with Partitioning

```yaml
connector_config:
  url: "jdbc:postgresql://db:5432/warehouse"
  table: "fact_sales"
  partition_column: "sale_id"
  lower_bound: 1
  upper_bound: 100000000
  num_partitions: 50
```

---

## Testing Your Connector

### Quick Test Script

```python
from pyspark.sql import SparkSession
from src.framework.connectors import ConnectorFactory

# Initialize Spark
spark = SparkSession.builder.appName("connector_test").getOrCreate()

# Test Volume Connector
volume_config = {
    "catalog": "landing",
    "schema": "lakehouse_landing",
    "volume": "test_data",
    "format": "json"
}
connector = ConnectorFactory.create("volume", volume_config)
df = connector.read_batch(spark)
print(f"Read {df.count()} records from volume")

# Test REST API Connector
api_config = {
    "endpoint": "https://api.example.com/test",
    "method": "GET"
}
api_connector = ConnectorFactory.create("rest_api", api_config)
# api_df = api_connector.read_batch(spark)  # Uncomment to test
```

### Run Framework Tests

```bash
cd /path/to/project
python tests/unit/test_connectors.py
```

Expected output:
```
✅ ALL TESTS PASSED
Connector framework is ready to use!
```

---

## Troubleshooting

### Error: "Unknown connector type"

**Problem**: Connector type not recognized

**Solution**: Check spelling in data contract. Available types:
- `volume` or `volume_autoloader`
- `rest_api`, `http`, `https`
- `jdbc`, `database`

### Error: "Missing required config fields"

**Problem**: Connector configuration incomplete

**Solution**: Check connector documentation for required fields:
- **Volume**: catalog, schema, volume, format
- **REST API**: endpoint, method
- **JDBC**: url, table

### Error: JDBC connection failed

**Problem**: Can't connect to database

**Solutions**:
1. Verify JDBC driver installed: `spark.jars.packages`
2. Check network connectivity
3. Validate credentials in secrets
4. Test connection string format

### Error: API returns 401/403

**Problem**: Authentication failed

**Solutions**:
1. Verify `auth_type` matches API requirements
2. Check `auth_token` in secrets is current
3. Review API documentation for auth method

---

## Performance Tips

### REST API Optimization
- Set appropriate `rate_limit_delay` to avoid throttling
- Use `pagination_config.limit` to balance request size
- Configure `timeout` based on API response times

### JDBC Optimization
- Use `num_partitions` = 2-4x number of Spark cores
- Set `partition_column` to evenly distributed numeric column
- Configure `fetch_size` for memory-efficient reads
- Use `incremental_column` to avoid full table scans

### Volume Optimization
- Use columnar formats (parquet, delta) for better performance
- Enable `add_audit_columns` for change tracking
- Partition data by date/region for faster queries

---

## Next Steps

1. **Review Examples**: Check `documentation/CONNECTOR_FRAMEWORK.md`
2. **Run Tests**: `python tests/unit/test_connectors.py`
3. **Try It Out**: Add a new data source using the examples above
4. **Read Full Docs**: See complete documentation for advanced features

---

## Support

- **Documentation**: `documentation/CONNECTOR_FRAMEWORK.md`
- **Examples**: Data contracts in `data_contracts/source_system/`
- **Tests**: `tests/unit/test_connectors.py`
- **Code**: `src/framework/connectors/`

Happy connecting! 🚀
