# Base Factory - CDC and Deduplication Layer

**File**: `src/framework/factory/base_factory.py`

## Overview

The `BasePipelineFactory` creates base layer DLT tables with **Change Data Capture (CDC)** processing. It implements **Slowly Changing Dimensions (SCD) Type 2** to track historical changes in source data.

## Purpose

Transform raw, potentially dirty data into clean, deduplicated, historicized data that tracks all changes over time using surrogate keys and temporal columns.

---

## Class: BasePipelineFactory

### Constructor

```python
class BasePipelineFactory:
    def __init__(self, spark: SparkSession):
        """Initialize the factory.
        
        Args:
            spark: Active SparkSession
        """
        self.spark = spark
        self.ws = get_ws_client()
        self.dq_engine = get_dq_engine(self.ws)
```

---

## Main Methods

### create_pipeline(source_system_name: str)

**Signature**:
```python
def create_pipeline(self, source_system_name: str) -> None:
    """Create base layer CDC pipeline for a source system.
    
    This method creates DLT tables with Change Data Capture processing
    based on data contracts. It supports optional data quality validation.
    
    Args:
        source_system_name: Name of the source system (e.g., 'lakehouse', 'review')
    """
```

**Flow Diagram**:
```
create_pipeline(source_system_name)
├── Load CentralizedPipelineConfig
├── Create CatalogSchemaManager
├── Load data contract
├── Load data quality configuration (optional)
│
└── For each schema:
    ├── Validate CDC configuration
    ├── Check data quality rules
    ├── Create optional DQ validation table
    └── Create CDC (SCD Type 2) table
```

**Example**:
```python
spark = SparkSession.getActiveSession()
factory = BasePipelineFactory(spark)
factory.create_pipeline("lakehouse")  # Creates all base tables with CDC
```

---

### _process_schema(...)

**Signature**:
```python
def _process_schema(
    self,
    schema: Any,
    centralized_config: CentralizedPipelineConfig,
    validated_data_quality: List[Dict[str, Any]]
) -> None:
    """Process a single schema to create base table with optional DQ.
    
    Args:
        schema: Schema object from data contract
        centralized_config: Centralized pipeline configuration
        validated_data_quality: List of data quality check configurations
    """
```

**Steps**:

```python
1. Extract CDC configuration from schema
   ├─ keys: Business key columns for CDC
   ├─ sequence_column: Column determining change order
   └─ stored_as_scd_type: SCD implementation (typically 2)

2. Find matching data quality rules
   └─ If found, create optional DQ validation table

3. Determine source table
   ├─ If DQ enabled: use DQ output table
   └─ If DQ disabled: use raw layer directly

4. Create CDC table
   └─ Applies SCD Type 2 transformation
```

---

### _create_cdc_table(...)

**Signature**:
```python
def _create_cdc_table(
    self,
    model_name: str,
    source: str,
    centralized_config: CentralizedPipelineConfig,
    keys: List[str],
    sequence_column: str,
    stored_as_scd_type: int
) -> None:
    """Create CDC table using DLT APPLY CHANGES INTO.
    
    Implements SCD Type 2 to track all changes over time.
    
    Args:
        model_name: Name of the table
        source: Source table path
        centralized_config: Centralized pipeline configuration
        keys: Business key columns
        sequence_column: Column determining change order
        stored_as_scd_type: SCD type (typically 2)
    """
```

**What it does**:
1. Creates DLT table with APPLY CHANGES INTO
2. Tracks inserts and updates automatically
3. Adds temporal columns (`__START_AT`, `__END_AT`)
4. Maintains surrogate key

---

## SCD Type 2 Transformation

### Before (Raw Data)

```sql
SELECT * FROM raw.landing.customer
ORDER BY updated_at;

-- Output:
┌──────┬───────────┬─────────────────────┐
│ id   │ name      │ updated_at          │
├──────┼───────────┼─────────────────────┤
│ 1    │ Alice     │ 2024-01-01 10:00:00 │
│ 2    │ Bob       │ 2024-01-01 10:00:00 │
│ 1    │ Alicia    │ 2024-06-15 14:30:00 │ ← Name changed
│ 3    │ Charlie   │ 2024-08-01 09:00:00 │
└──────┴───────────┴─────────────────────┘
```

### After (Base Layer with SCD Type 2)

```sql
SELECT * FROM base.base.customer
ORDER BY id, __START_AT;

-- Output:
┌──────┬──────────┬─────────────────────┬─────────────────────┐
│ id   │ name     │ __START_AT          │ __END_AT            │
├──────┼──────────┼─────────────────────┼─────────────────────┤
│ 1    │ Alice    │ 2024-01-01 10:00:00 │ 2024-06-15 14:30:00 │
│ 1    │ Alicia   │ 2024-06-15 14:30:00 │ NULL                │ ← Current
│ 2    │ Bob      │ 2024-01-01 10:00:00 │ NULL                │ ← Current
│ 3    │ Charlie  │ 2024-08-01 09:00:00 │ NULL                │ ← Current
└──────┴──────────┴─────────────────────┴─────────────────────┘
```

### Key Points

| Column | Purpose | Usage |
|--------|---------|-------|
| `__START_AT` | When change became active | Temporal join key |
| `__END_AT` | When change ended (NULL = current) | `WHERE __END_AT IS NULL` for active records |
| Business Keys | `id`, etc. | Identify same entity across versions |

### Getting Current Records

```python
# Get only current data
df = spark.read.table("base.base.customer").filter(col("__END_AT").isNull())

# Get entire history
df = spark.read.table("base.base.customer")  # All versions

# Get specific point-in-time
df = (spark.read.table("base.base.customer")
      .filter(col("__START_AT") <= timestamp)
      .filter((col("__END_AT") > timestamp) | col("__END_AT").isNull()))
```

---

## Data Quality Integration

### Optional DQ Validation

```python
# If data quality rules exist, create validation table
if validated_data_quality:
    source = self._create_dq_table(
        model_name,
        centralized_config,
        validated_data_quality
    )
    # Use DQ output as source for CDC
else:
    source = f"raw.landing.{model_name}"  # Use raw directly
```

### DQ Table Structure

```sql
-- Before DQ
raw.landing.customer:
┌──────┬──────────┬──────────┐
│ id   │ name     │ email    │
├──────┼──────────┼──────────┤
│ 1    │ Alice    │ alice@.. │
│ 2    │ NULL     │ bob@..   │ ← Name is NULL (invalid)
│ 3    │ Charlie  │ NULL     │ ← Email is NULL (invalid)
└──────┴──────────┴──────────┘

-- After DQ
dq_customer:
┌──────┬──────────┬──────────┐
│ id   │ name     │ email    │
├──────┼──────────┼──────────┤
│ 1    │ Alice    │ alice@.. │
│ 2    │ UNKNOWN  │ bob@..   │ ← Filled with default
│ 3    │ Charlie  │ UNKNOWN  │ ← Filled with default
└──────┴──────────┴──────────┘

-- Then CDC applied to clean data
```

---

## CDC Configuration in Data Contracts

### Example YAML

```yaml
# data_contracts/source_system/lakehouse.yml
schema:
  - name: customer
    type: object
    
    customProperties:
      - property: keys
        value: ["customer_id"]
      - property: sequence_column
        value: updated_at
      - property: stored_as_scd_type
        value: "2"
      - property: data_quality_checks
        value:
          - check: not_null
            column: customer_id
          - check: unique
            column: customer_id
    
    properties:
      - name: customer_id
        type: integer
        required: true
      - name: name
        type: string
      - name: email
        type: string
      - name: updated_at
        type: timestamp
        required: true  # Used for sequencing
```

---

## DLT APPLY CHANGES INTO

### Concept

DLT's `APPLY CHANGES INTO` automatically handles CDC:

```python
@dlt.table(name="base.base.customer")
@dlt.expect_all_or_drop({"valid_id": "id IS NOT NULL"})
def base_customer():
    # This decorator specifies source and keys
    return dlt.read_stream("raw.landing.customer")

dlt.create_streaming_live_table(
    name="base.base.customer",
    comment="Base layer customer table with SCD Type 2"
)

dlt.apply_changes(
    target="base.base.customer",
    source="raw.landing.customer",
    keys=["customer_id"],
    sequence_by=col("updated_at"),
    ignore_null_updates=True,
    apply_as_deletes=expr("operation = 'DELETE'"),
    apply_as_truncates=expr("operation = 'TRUNCATE'")
)
```

---

## Key CDC Columns

DLT automatically adds:

| Column | Type | Purpose |
|--------|------|---------|
| `__START_AT` | timestamp | When version became active |
| `__END_AT` | timestamp | When version ended (NULL = current) |

Optional (configured):
| Column | Type | Purpose |
|--------|------|---------|
| `operation` | string | INSERT, UPDATE, DELETE |
| `_change_type` | string | NEW, UPDATE_PREIMAGE, UPDATE_POSTIMAGE |
| `_commit_version` | long | DLT version number |

---

## Error Handling

### Per-Schema Processing

```python
for schema in data_contract.schema_:
    try:
        self._process_schema(schema, ...)
    except Exception as e:
        logger.error(f"Error processing schema {schema.name}: {e}")
        continue  # Continue with next schema
```

### Common Errors

| Error | Cause | Solution |
|-------|-------|----------|
| Invalid keys | Business keys not found | Check data contract |
| No sequence column | sequence_column not specified | Add to contract |
| DQ validation failed | Data quality rules rejected records | Fix source data |
| Table already exists | Table created previously | Drop and rerun |

---

## Logging

### Log Levels

```python
logger.info(f"Creating base pipeline for: {source_system_name}")
logger.info(f"Processing model: {model_name}")
logger.debug(f"Keys: {keys}, Sequence: {sequence_column}")
logger.info(f"Created CDC table: {model_name}")
logger.error(f"Error processing {model_name}: {e}")
```

---

## Usage Examples

### Basic Pipeline Creation

```python
from pyspark.sql import SparkSession
from src.framework.factory.base_factory import BasePipelineFactory

spark = SparkSession.getActiveSession()
factory = BasePipelineFactory(spark)

# Create all base tables with CDC
factory.create_pipeline("lakehouse")

# Creates:
# - base.base.customer (with SCD Type 2)
# - base.base.booking (with SCD Type 2)
# - etc.
```

### In Solution Layer

```python
# src/solution/base/base_ingest_lakehouse.py
"""Base layer CDC pipeline for lakehouse."""

from src.framework.factory.base_factory import create_base_pipeline

# That's it!
create_base_pipeline("lakehouse")
```

### Reading Current Records

```python
from pyspark.sql.functions import col

spark = SparkSession.getActiveSession()

# Get current records only
current_customers = (spark.read.table("base.base.customer")
                     .filter(col("__END_AT").isNull()))

# Show results
current_customers.show()

# Use in downstream tables
@dlt.table(name="curated.dimensions.dim_customer")
def dimension():
    return spark.read.table("base.base.customer").filter(col("__END_AT").isNull())
```

### With Data Quality

```python
# Data contract with DQ rules
schema_with_dq:
  - name: customer
    customProperties:
      - property: data_quality_checks
        value:
          - check: not_null
            column: customer_id
          - check: unique
            column: customer_id
          - check: format
            column: email
            pattern: "^[^@]+@[^@]+\\.[^@]+$"

# Factory automatically:
# 1. Creates DQ validation table
# 2. Applies DQ rules
# 3. Creates CDC table from DQ output
```

---

## Design Decisions

### Why SCD Type 2?

**Tradeoff Analysis**:
| Aspect | SCD Type 1 | SCD Type 2 |
|--------|-----------|-----------|
| Storage | Low | Higher (multiple versions) |
| Query Complexity | Simple | Moderate (filter by time) |
| Historical Tracking | None | Complete |
| Star Schema Support | Limited | Excellent |

**Decision**: Type 2 chosen because:
- Downstream analytics need full history
- Storage cost acceptable
- Star schema requires temporal joins

### Why Optional DQ?

**Reasons**:
- Some data sources are clean (don't need DQ)
- Performance: avoid unnecessary processing
- Flexibility: enable when needed

### Why Per-Schema Error Handling?

**Reasons**:
- One schema failure shouldn't break pipeline
- Production reliability
- Easier debugging (know which schema failed)

---

## Performance Considerations

### Indexing
- Business keys should be indexed for CDC efficiency
- Sequence column should be sortable (typically timestamp)

### Deduplication
- If source has duplicates with same sequence value, one version kept
- Consider data quality before CDC

### Storage
- SCD Type 2 increases table size
- Can implement retention policies to archive old versions

---

## Integration Points

### Depends On:
- **CentralizedPipelineConfig**: Catalog/schema names
- **CatalogSchemaManager**: Path construction
- **DataContractHelper**: YAML parsing, CDC config extraction
- **DQXHelper**: Data quality validation
- **DLT**: Table creation and CDC

### Used By:
- Solution layer modules (e.g., `src/solution/base/base_ingest_lakehouse.py`)
- Downstream factories (dimension, fact factories)

### Related:
- **RawPipelineFactory**: Upstream ingestion
- **DimensionFactory**: Uses base tables with active record filter
- **FactFactory**: Uses base tables for fact source

---

## Troubleshooting

### CDC Table Not Updating

**Check**:
1. Is sequence column populated?
2. Are business keys present?
3. Check raw layer: is new data coming in?

### Duplicates in Base Table

**Cause**: Multiple records with same business key and sequence value

**Solution**:
1. Add data quality deduplication
2. Add secondary sort column

### DQ Validation Failing

**Check**:
1. Are rules correct? Review data contract
2. Sample data: what's actually coming in?
3. DQ rules too strict?

---

## Extensions

### Custom CDC Logic

```python
# If APPLY CHANGES doesn't fit your needs
@dlt.table(name="base.base.custom")
def custom_cdc():
    df = spark.read.table("raw.landing.custom")
    # Custom CDC logic here
    return df.withColumn("__START_AT", lit(current_timestamp()))
```

### Archive Old Versions

```python
# Archive records older than N days
old_records = (spark.read.table("base.base.customer")
               .filter(col("__END_AT") < current_timestamp() - expr("INTERVAL 1 YEAR")))

old_records.write.mode("append").option("path", "/archive/customer/")
```

---

## Next Steps

- [Dimension Factory](./dimension_factory.md) - Creating star schema dimensions
- [Fact Factory](./fact_factory.md) - Creating fact tables
- [Configuration System](../configuration/README.md) - CDC configuration details
- [Raw Factory](./raw_factory.md) - Upstream data ingestion
