# Factory Pattern - Quick Reference

## At a Glance

| Factory | Layer | Input | Output | Use Case |
|---------|-------|-------|--------|----------|
| **RawPipelineFactory** | Raw | External data (APIs, files, DBs) | `raw.<schema>.<model>` | Ingest data from any source |
| **BasePipelineFactory** | Base | Raw layer tables | `base.<schema>.<model>` | CDC, deduplication, history tracking |
| **DimensionFactory** | Curated | Base layer tables | `curated.dimensions.dim_*` | Create reference tables for star schema |
| **FactFactory** | Curated | Base layer + dimensions | `curated.facts.fact_*` | Create fact tables with FK lookups |

---

## Factory Pattern Stack

```
┌──────────────────────────────────┐
│  Solution Layer (src/solution/)  │
│  - raw_ingest_lakehouse.py       │
│  - base_ingest_lakehouse.py      │
│  - curated_create_dimensions.py  │
│  - curated_create_facts.py       │
└──────────────────────────────────┘
              │ calls
              ▼
┌──────────────────────────────────┐
│      Factory Classes             │
│  - RawPipelineFactory            │
│  - BasePipelineFactory           │
│  - DimensionFactory              │
│  - FactFactory                   │
└──────────────────────────────────┘
              │ uses
              ▼
┌──────────────────────────────────┐
│  Configuration & Helpers         │
│  - CentralizedPipelineConfig     │
│  - CatalogSchemaManager          │
│  - ConnectorFactory              │
│  - DataContractHelper            │
└──────────────────────────────────┘
              │ creates
              ▼
┌──────────────────────────────────┐
│      DLT Tables                  │
│  - Raw: raw.landing.*            │
│  - Base: base.base.*             │
│  - Dimensions: curated.dim_*     │
│  - Facts: curated.fact_*         │
└──────────────────────────────────┘
```

---

## One-Line Solution Implementation

```python
# src/solution/raw/raw_ingest_lakehouse.py
from src.framework.factory.raw_factory import create_raw_pipeline
create_raw_pipeline("lakehouse")

# src/solution/base/base_ingest_lakehouse.py
from src.framework.factory.base_factory import create_base_pipeline
create_base_pipeline("lakehouse")

# src/solution/curated/curated_create_dimensions.py
from pyspark.sql import SparkSession
from src.framework.factory.dimension_factory import CuratedDimensionFactory
spark = SparkSession.getActiveSession()
factory = CuratedDimensionFactory(spark, "lakehouse")
factory.create_dimension("dim_customer", "customer", "customer_id")

# src/solution/curated/curated_create_facts.py
from pyspark.sql import SparkSession
from src.framework.factory.fact_factory import CuratedFactFactory
spark = SparkSession.getActiveSession()
factory = CuratedFactFactory(spark, "lakehouse")
factory.create_fact(
    "fact_bookings",
    "bookings",
    {"customer_id": "customer_key", "seller_id": "seller_key"}
)
```

---

## Raw Factory API

### Constructor
```python
factory = RawPipelineFactory(spark)
```

### Main Method
```python
factory.create_pipeline(source_system_name: str) -> None
```

### Parameters
- `source_system_name`: Name of source system (e.g., "lakehouse")

### Creates Tables
```
raw.<schema>.<model>  (one per data contract schema)
```

### Example
```python
spark = SparkSession.getActiveSession()
factory = RawPipelineFactory(spark)
factory.create_pipeline("lakehouse")
```

---

## Base Factory API

### Constructor
```python
factory = BasePipelineFactory(spark)
```

### Main Method
```python
factory.create_pipeline(source_system_name: str) -> None
```

### Parameters
- `source_system_name`: Name of source system (e.g., "lakehouse")

### Creates Tables
```
base.<schema>.<model>  (with SCD Type 2)
  ├─ __START_AT: When version became active
  └─ __END_AT: When version ended (NULL = current)
```

### Example
```python
spark = SparkSession.getActiveSession()
factory = BasePipelineFactory(spark)
factory.create_pipeline("lakehouse")
```

### Query Current Records
```python
from pyspark.sql.functions import col

current = spark.read.table("base.base.customer").filter(col("__END_AT").isNull())
```

---

## Dimension Factory API

### Constructor
```python
factory = CuratedDimensionFactory(spark, source_system="lakehouse")
```

### Main Method
```python
factory.create_dimension(
    dimension_name: str,
    source_table: str,
    business_key_column: str,
    filter_active: bool = True,
    additional_transforms: Optional[Callable] = None
) -> None
```

### Parameters
| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `dimension_name` | str | - | Name like `dim_customer` |
| `source_table` | str | - | Base layer table name |
| `business_key_column` | str | - | Column name like `customer_id` |
| `filter_active` | bool | True | Filter `__END_AT IS NULL` |
| `additional_transforms` | Callable | None | Custom transformation function |

### Creates Table
```
curated.dimensions.<dimension_name>
  ├─ {entity}_id: Surrogate key (generated)
  ├─ {entity}_key: Business key (renamed)
  └─ Other columns from source
```

### Examples

**Simple dimension**:
```python
factory.create_dimension(
    "dim_customer",
    "customer",
    "customer_id"
)
```

**With custom transformation**:
```python
def enrich(df):
    address = spark.read.table("base.base.address")
    return df.join(address, on="id")

factory.create_dimension(
    "dim_customer",
    "customer",
    "customer_id",
    additional_transforms=enrich
)
```

**Keep all history**:
```python
factory.create_dimension(
    "dim_customer_history",
    "customer",
    "customer_id",
    filter_active=False  # All versions
)
```

---

## Fact Factory API

### Constructor
```python
factory = CuratedFactFactory(spark, source_system="lakehouse")
```

### Main Method
```python
factory.create_fact(
    fact_name: str,
    source_table: str,
    dimension_mappings: Dict[str, str],
    source_schema: Optional[str] = None,
    additional_transforms: Optional[Callable] = None
) -> None
```

### Parameters
| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `fact_name` | str | - | Name like `fact_bookings` |
| `source_table` | str | - | Base layer table name |
| `dimension_mappings` | dict | - | {source_col: dim_key_col} |
| `source_schema` | str | None | Schema name (base_schema if None) |
| `additional_transforms` | Callable | None | Custom transformation function |

### Creates Table
```
curated.facts.<fact_name>
  ├─ {entity}_key: FK to dimension (after lookup)
  ├─ Grain key: Identifier like booking_id
  ├─ Measures: Numerical values like revenue
  └─ Dates: fact_date, booking_date, etc.
```

### Examples

**Simple fact**:
```python
factory.create_fact(
    "fact_bookings",
    "bookings",
    {
        "customer_id": "customer_key",
        "seller_id": "seller_key",
        "lakehouse_id": "lakehouse_key"
    }
)
```

**With pre-aggregation**:
```python
from pyspark.sql.functions import col, year, sum as spark_sum

def aggregate(df):
    return (df
        .withColumn("booking_year", year(col("booking_date")))
        .groupBy("customer_id", "seller_id", "booking_year")
        .agg(spark_sum("revenue").alias("total_revenue")))

factory.create_fact(
    "fact_bookings_yearly",
    "bookings",
    {"customer_id": "customer_key", "seller_id": "seller_key"},
    additional_transforms=aggregate
)
```

---

## Data Contracts

### YAML Structure

```yaml
---
catalog: source_system
object: lakehouse

server:
  - property: connector_type
    value: rest_api
  - property: endpoint
    value: https://api.example.com

schema:
  - name: customer
    type: object
    is_root_call: true
    customProperties:
      - property: keys
        value: ["customer_id"]
      - property: sequence_column
        value: updated_at
      - property: stored_as_scd_type
        value: "2"
    properties:
      - name: customer_id
        type: integer
      - name: name
        type: string
```

---

## Configuration Files

### CentralizedPipelineConfig

Used by all factories to get catalog/schema names:

```python
config = CentralizedPipelineConfig.from_spark(spark, "lakehouse")

# Access properties
print(config.raw_catalog)      # "raw"
print(config.raw_schema)       # "landing"
print(config.base_catalog)     # "base"
print(config.curated_catalog)  # "curated"
```

### CatalogSchemaManager

Used by factories to construct table paths:

```python
manager = CatalogSchemaManager.from_pipeline_config(config)

# Get paths
raw_path = manager.get_raw_table_path("customer")
# Result: "raw.landing.customer"

base_path = manager.get_base_table_path("customer")
# Result: "base.base.customer"

dim_path = manager.get_dimension_table_path("dim_customer")
# Result: "curated.dimensions.dim_customer"

fact_path = manager.get_fact_table_path("fact_bookings")
# Result: "curated.facts.fact_bookings"
```

---

## Common Patterns

### Processing Multiple Sources

```python
for source in ["lakehouse", "review", "energydataservice"]:
    create_raw_pipeline(source)
    create_base_pipeline(source)
```

### Custom Transformations

```python
# Filter
def filter_recent(df):
    return df.filter(col("created_at") > date_sub(current_date(), 30))

# Enrich
def add_derived_columns(df):
    return df.withColumn("year_month", trunc(col("booking_date"), "month"))

# Aggregate
def summarize(df):
    return df.groupBy("customer_id").agg(sum("revenue"))

factory.create_fact(..., additional_transforms=add_derived_columns)
```

### Error Handling

```python
try:
    factory.create_pipeline("lakehouse")
except Exception as e:
    logger.error(f"Failed to create pipeline: {e}")
    raise
```

---

## Performance Tips

| Layer | Optimization |
|-------|-------------|
| **Raw** | Use streaming connectors for large files |
| **Base** | Ensure sequence_column is indexed in source |
| **Dimension** | Keep small (< 1M rows typical), partition if larger |
| **Fact** | Pre-aggregate at common grain levels |

---

## Debugging

### View Generated Tables

```python
# List raw tables
spark.sql("SHOW TABLES IN raw.landing").show()

# List base tables
spark.sql("SHOW TABLES IN base.base").show()

# List dimensions
spark.sql("SHOW TABLES IN curated.dimensions").show()

# List facts
spark.sql("SHOW TABLES IN curated.facts").show()
```

### Inspect Table Structure

```python
spark.read.table("base.base.customer").printSchema()
spark.read.table("curated.dimensions.dim_customer").printSchema()
spark.read.table("curated.facts.fact_bookings").printSchema()
```

### Check Data Quality

```python
# Raw layer
spark.read.table("raw.landing.customer").count()

# Base layer CDC
spark.read.table("base.base.customer").filter(col("__END_AT").isNull()).count()

# Dimension
spark.read.table("curated.dimensions.dim_customer").count()

# Fact
spark.read.table("curated.facts.fact_bookings").count()
```

---

## Common Issues

| Issue | Cause | Solution |
|-------|-------|----------|
| Empty table | No data in source | Check raw layer |
| CDC not working | Missing keys/sequence column | Verify data contract |
| Dimension lookup fails | Dimension doesn't exist | Create dimension first |
| Wrong foreign keys | Business key mismatch | Check dimension_mappings |
| Slow queries | Grain too detailed | Create pre-aggregated fact |

---

## Next Steps

- [Factory README](./README.md) - Full factory pattern documentation
- [Raw Factory Details](./raw_factory.md) - Deep dive
- [Base Factory Details](./base_factory.md) - CDC details
- [Dimension Factory Details](./dimension_factory.md) - Star schema
- [Fact Factory Details](./fact_factory.md) - Star schema facts
- [Configuration System](../configuration/README.md) - Config management
- [Connector Framework](../connectors/README.md) - Data sources
