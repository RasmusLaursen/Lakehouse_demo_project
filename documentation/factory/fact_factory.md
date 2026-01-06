# Fact Factory - Star Schema Fact Creation

**File**: `src/framework/factory/fact_factory.py`

## Overview

The `CuratedFactFactory` creates fact tables for the star schema. It reads from base layer fact sources, performs automatic dimension key lookups, renames columns to foreign keys, and supports optional custom transformations.

## Purpose

Create fact tables that:
- Contain measurable events or transactions
- Reference dimension tables via foreign keys
- Support dimensional queries and analytics
- Maintain referential integrity with dimensions
- Enable OLAP cube operations

---

## Class: CuratedFactFactory

### Constructor

```python
class CuratedFactFactory:
    def __init__(self, spark: SparkSession, source_system: str = "lakehouse"):
        """Initialize the factory.
        
        Args:
            spark: Active SparkSession
            source_system: Source system name (default: lakehouse)
        """
        self.spark = spark
        self.centralized_config = CentralizedPipelineConfig.from_spark(
            spark, source_system
        )
        self.centralized_config.validate()
        self.catalog_manager = CatalogSchemaManager.from_pipeline_config(
            self.centralized_config
        )
```

---

## Main Methods

### create_fact(...)

**Signature**:
```python
def create_fact(
    self,
    fact_name: str,
    source_table: str,
    dimension_mappings: Dict[str, str],
    source_schema: Optional[str] = None,
    additional_transforms: Optional[Callable[[DataFrame], DataFrame]] = None
) -> None:
    """Create a fact table with dimension key lookups.
    
    Creates a DLT fact table that:
    1. Reads from base layer
    2. Renames columns to dimension foreign keys
    3. Performs automatic dimension key lookups
    4. Applies optional custom transformations
    5. Registers with DLT
    
    Args:
        fact_name: Name of fact table (e.g., 'fact_bookings')
        source_table: Name of source table in base layer (e.g., 'bookings')
        dimension_mappings: Dict of {source_col: dimension_key_col}
            Example: {'customer_id': 'customer_key', 'seller_id': 'seller_key'}
        source_schema: Optional schema name (defaults to base_schema)
        additional_transforms: Optional function for custom transformations
    
    Example:
        >>> factory.create_fact(
        ...     'fact_bookings',
        ...     'bookings',
        ...     {
        ...         'customer_id': 'customer_key',
        ...         'seller_id': 'seller_key',
        ...         'lakehouse_id': 'lakehouse_key'
        ...     }
        ... )
    """
```

**Flow Diagram**:
```
create_fact(fact_name, source_table, dimension_mappings, ...)
│
├─ @dlt.table decorator
│  └─ Registers table with DLT
│
└─ Function body:
   ├─ Determine source schema
   │  └─ base_schema if not specified
   │
   ├─ Read base layer table
   │  └─ spark.read.table("base.base.{source_table}")
   │
   ├─ Rename columns to dimension keys
   │  ├─ customer_id → customer_key
   │  ├─ seller_id → seller_key
   │  └─ lakehouse_id → lakehouse_key
   │
   ├─ Apply custom transformations (if provided)
   │  └─ additional_transforms(df)
   │
   ├─ Lookup dimension foreign keys
   │  ├─ For each dimension_mapping:
   │  │  └─ lookup_key in dimension table
   │  └─ Replace business key with surrogate key
   │
   └─ Return fact table with FK to dimensions
```

**Example**:
```python
spark = SparkSession.getActiveSession()
factory = CuratedFactFactory(spark, "lakehouse")

# Create booking fact table
factory.create_fact(
    fact_name="fact_bookings",
    source_table="bookings",
    dimension_mappings={
        "customer_id": "customer_key",
        "seller_id": "seller_key",
        "lakehouse_id": "lakehouse_key"
    }
)

# Creates: curated.facts.fact_bookings
```

---

## Fact Transformation Pipeline

### Step 1: Read Source Table

```python
# Read base layer fact source
df = spark.read.table("base.base.bookings")

# Result:
┌────────────┬────────────┬──────────────┬────────────┬─────────┐
│booking_id  │customer_id │seller_id     │lakehouse_id│revenue  │
├────────────┼────────────┼──────────────┼────────────┼─────────┤
│ 1          │ 10         │ 20           │ 30         │ 1000    │
│ 2          │ 11         │ 21           │ 30         │ 2000    │
│ 3          │ 12         │ 22           │ 31         │ 1500    │
└────────────┴────────────┴──────────────┴────────────┴─────────┘
```

### Step 2: Rename Columns to Dimension Keys

```python
# Apply dimension_mappings
df = df.withColumnsRenamed({
    "customer_id": "customer_key",
    "seller_id": "seller_key",
    "lakehouse_id": "lakehouse_key"
})

# Result:
┌────────────┬──────────────┬─────────────┬──────────────┬─────────┐
│booking_id  │customer_key  │seller_key   │lakehouse_key │revenue  │
├────────────┼──────────────┼─────────────┼──────────────┼─────────┤
│ 1          │ 10           │ 20          │ 30           │ 1000    │
│ 2          │ 11           │ 21          │ 30           │ 2000    │
│ 3          │ 12           │ 22          │ 31           │ 1500    │
└────────────┴──────────────┴─────────────┴──────────────┴─────────┘
```

### Step 3: Apply Custom Transformations (Optional)

```python
# Example: Add booking date year, aggregate, filter
def enrich_bookings(df):
    from pyspark.sql.functions import col, year
    return (df
        .withColumn("booking_year", year(col("booking_date")))
        .filter(col("revenue") > 0))

factory.create_fact(
    fact_name="fact_bookings",
    source_table="bookings",
    dimension_mappings={...},
    additional_transforms=enrich_bookings
)
```

### Step 4: Dimension Key Lookups

```python
# For each dimension mapping, lookup surrogate key
# customer_key 10 → lookup in dim_customer

from src.framework.helper import dimension_keys_lookup

df = dimension_keys_lookup(
    curated_catalog="curated",
    curated_dimension_schema="dimensions",
    fact_df=df
)

# Result:
┌────────────┬──────────────┬─────────────┬──────────────┬─────────┐
│booking_id  │customer_key  │seller_key   │lakehouse_key │revenue  │
├────────────┼──────────────┼─────────────┼──────────────┼─────────┤
│ 1          │ 1            │ 3           │ 5            │ 1000    │
│ 2          │ 2            │ 4           │ 5            │ 2000    │
│ 3          │ 3            │ 5           │ 6            │ 1500    │
└────────────┴──────────────┴─────────────┴──────────────┴─────────┘

(business keys replaced with dimension surrogate keys)
```

---

## Fact Table Structure

### Typical Fact Table

```sql
SELECT * FROM curated.facts.fact_bookings LIMIT 3;

-- Output:
┌────────────┬──────────────┬─────────────┬──────────────┬──────────┬──────────────┐
│booking_id  │customer_key  │seller_key   │lakehouse_key │revenue   │booking_date  │
├────────────┼──────────────┼─────────────┼──────────────┼──────────┼──────────────┤
│ 1          │ 1            │ 3           │ 5            │ 1000.00  │ 2024-01-15   │
│ 2          │ 2            │ 4           │ 5            │ 2000.00  │ 2024-01-16   │
│ 3          │ 3            │ 5           │ 6            │ 1500.00  │ 2024-01-17   │
└────────────┴──────────────┴─────────────┴──────────────┴──────────┴──────────────┘
```

### Column Types

| Category | Columns | Purpose |
|----------|---------|---------|
| **Grain Keys** | booking_id | Identify fact at grain level |
| **Foreign Keys** | customer_key, seller_key, lakehouse_key | Join to dimensions |
| **Measures** | revenue | Numerical values to aggregate |
| **Dates** | booking_date | Dimension or fact-level date |

### Grain

**Definition**: The level of detail of a single fact row

**Example**: One row per booking
- NOT aggregated (no SUM of revenues)
- NOT grouped (each booking separate)
- Atomic level of transaction

---

## Dimension Lookups

### How dimension_keys_lookup Works

```python
from src.framework.helper import dimension_keys_lookup

df = dimension_keys_lookup(
    curated_catalog="curated",
    curated_dimension_schema="dimensions",
    fact_df=df
)
```

**Process**:

```
For each column named {entity}_key in fact table:
├─ Find dimension: curated.dimensions.dim_{entity}
│
├─ Lookup logic:
│  For each row in fact_df:
│  ├─ Find matching row in dimension
│  │  WHERE dim.{entity}_key = fact.{entity}_key
│  └─ Get dimension attributes if needed
│
└─ Handle mismatches (warn if key not found)
```

**Example**:

```python
# Fact table has:
┌─────────────────┐
│ customer_key: 10│ ← Business key
│ ...             │
└─────────────────┘

# Dimension has:
┌──────────────────────────────┐
│ dim_customer:                │
│  customer_key: 10            │
│  customer_id: 1  ← Surrogate │
│  name: Alice                 │
└──────────────────────────────┘

# After lookup:
┌──────────────────────┐
│ customer_key: 1      │ ← Surrogate key
│ ...                  │
└──────────────────────┘
```

---

## Usage Examples

### Simple Fact Table

```python
from pyspark.sql import SparkSession
from src.framework.factory.fact_factory import CuratedFactFactory

spark = SparkSession.getActiveSession()
factory = CuratedFactFactory(spark, "lakehouse")

# Create fact table
factory.create_fact(
    fact_name="fact_bookings",
    source_table="bookings",
    dimension_mappings={
        "customer_id": "customer_key",
        "seller_id": "seller_key",
        "lakehouse_id": "lakehouse_key"
    }
)

# Creates: curated.facts.fact_bookings
```

### Fact Table with Custom Transformation

```python
from pyspark.sql.functions import col, year, when

# Custom transformation: Add year column, flag outliers
def transform_bookings(df):
    return (df
        .withColumn("booking_year", year(col("booking_date")))
        .withColumn(
            "is_high_value",
            when(col("revenue") > 5000, 1).otherwise(0)
        ))

factory.create_fact(
    fact_name="fact_bookings_enhanced",
    source_table="bookings",
    dimension_mappings={
        "customer_id": "customer_key",
        "seller_id": "seller_key"
    },
    additional_transforms=transform_bookings
)

# Creates: curated.facts.fact_bookings_enhanced
```

### Multiple Fact Tables

```python
spark = SparkSession.getActiveSession()
factory = CuratedFactFactory(spark, "lakehouse")

# Create multiple facts
factory.create_fact(
    "fact_bookings",
    "bookings",
    {"customer_id": "customer_key", "seller_id": "seller_key"}
)

factory.create_fact(
    "fact_reviews",
    "reviews",
    {"customer_id": "customer_key", "seller_id": "seller_key"}
)

factory.create_fact(
    "fact_inventory",
    "inventory",
    {"lakehouse_id": "lakehouse_key"}
)
```

### Complex Fact Table with Pre-aggregation

```python
from pyspark.sql.functions import col, sum as spark_sum, year

# Pre-aggregate at year level
def aggregate_bookings(df):
    return (df
        .withColumn("booking_year", year(col("booking_date")))
        .groupBy("customer_id", "seller_id", "booking_year")
        .agg(spark_sum("revenue").alias("total_revenue")))

factory.create_fact(
    fact_name="fact_bookings_by_year",
    source_table="bookings",
    dimension_mappings={
        "customer_id": "customer_key",
        "seller_id": "seller_key"
    },
    additional_transforms=aggregate_bookings
)

# Creates: curated.facts.fact_bookings_by_year
```

---

## Integration with Dimensions

### Star Schema Example

```
Dimensions:
├─ curated.dimensions.dim_customer
├─ curated.dimensions.dim_seller
└─ curated.dimensions.dim_lakehouse

Fact:
└─ curated.facts.fact_bookings
   ├─ FK: customer_key → dim_customer
   ├─ FK: seller_key → dim_seller
   └─ FK: lakehouse_key → dim_lakehouse
```

### Querying with Dimensions

```python
from pyspark.sql.functions import col, sum as spark_sum

fact = spark.read.table("curated.facts.fact_bookings")
customer = spark.read.table("curated.dimensions.dim_customer")
seller = spark.read.table("curated.dimensions.dim_seller")

# Revenue by customer and seller
result = (fact
    .join(customer, on="customer_key")
    .join(seller, on="seller_key")
    .groupBy(customer.name, seller.name)
    .agg(spark_sum(fact.revenue).alias("total_revenue"))
    .orderBy(col("total_revenue").desc()))

result.show()
```

---

## Data Types and Grain

### Fact Types

| Type | Example | Characteristics |
|------|---------|-----------------|
| **Transaction** | Orders, Bookings | One row per event, detailed grain |
| **Periodic Snapshot** | Account Balance | One row per entity per period |
| **Accumulating Snapshot** | Order Fulfillment | One row that updates with milestones |

### Grain Definition

```python
# Atomic Grain (Detailed)
# One row per booking
fact_name="fact_bookings"
grain="(booking_id)"

# Aggregated Grain
# One row per customer per month
fact_name="fact_bookings_monthly"
grain="(customer_key, booking_year, booking_month)"
```

---

## Dimension Naming Convention

### Foreign Key Naming

| Dimension | Surrogate Key | Business Key |
|-----------|---------------|--------------|
| dim_customer | customer_key | customer_id |
| dim_seller | seller_key | seller_id |
| dim_lakehouse | lakehouse_key | lakehouse_id |
| dim_date | date_key | calendar_date |

**Pattern**: {entity}_key → Surrogate, {entity}_id → Business

---

## Design Decisions

### Why Dimension Key Lookups?

**Problem**: Fact sources have business keys, dimensions have surrogate keys

**Solution**: Automatic lookup during fact creation
- Joins fact to dimension
- Replaces business key with surrogate
- Single step in factory

### Why Additional Transforms?

**Reasons**:
- Some facts need pre-aggregation
- Some need derived columns
- Keep all fact logic in one place
- Avoid post-processing steps

### Why Rename First?

**Order**:
1. Rename source columns to dimension keys
2. Apply custom transforms
3. Lookup dimension foreign keys

**Why**: Transforms can reference renamed columns, lookup uses renamed names

---

## Error Handling

### Dimension Not Found

```python
# If dimension doesn't exist, dimension_keys_lookup logs warning
# Fact still created, but without dimension attributes
```

### Key Mismatch

```python
# If business key not in dimension:
# - Row still included in fact
# - FK value may be NULL or error depending on implementation
```

### Type Mismatch

```python
# If data type mismatch (e.g., string vs int):
# - Conversion attempted
# - If fails, row might be skipped or error logged
```

---

## Performance Considerations

### Join Performance

```python
# Dimension key lookup uses broadcast join (dimensions small)
# Efficient for most cases

# Optimization: Ensure dimensions are small (< 100MB typically)
```

### Custom Transforms

```python
# Pre-aggregation: Reduces fact table size, query faster
# But: Can't drill down to detail level

# Best practice: Create both
# - fact_bookings (detail)
# - fact_bookings_monthly (pre-aggregated)
```

### Grain Impact

```python
# Detailed grain: Larger table, more query flexibility
# Aggregated grain: Smaller table, faster queries

# Star schema supports queries at any level via dimension rollup
```

---

## Troubleshooting

### Fact Table Empty

**Causes**:
1. Source table is empty
2. Transform filtered all rows
3. Dimension lookup removed all rows

**Check**:
```python
spark.read.table("base.base.bookings").count()
spark.read.table("curated.facts.fact_bookings").count()
```

### Foreign Keys All NULL

**Cause**: Dimension key lookup not finding matches

**Check**:
```python
# Verify dimension exists
spark.read.table("curated.dimensions.dim_customer").count()

# Check for key mismatches
fact = spark.read.table("curated.facts.fact_bookings")
fact.filter(col("customer_key").isNull()).count()
```

### Transform Fails

**Debug**:
1. Test transform separately
2. Check intermediate data types
3. Verify column names

```python
test_df = spark.read.table("base.base.bookings")
result = transform_bookings(test_df)
result.show()
```

---

## Extensions

### Conformed Facts

Create multiple facts sharing same dimensions:

```python
# fact_bookings and fact_reviews both use dim_customer
# Both can be joined through customer dimension
```

### Surrogate Key Generation

If automatic lookup insufficient:

```python
# Custom lookup in additional_transforms
def custom_lookup(df):
    dim = spark.read.table("curated.dimensions.dim_customer")
    return df.join(dim, on="customer_id").drop("customer_id")

factory.create_fact(..., additional_transforms=custom_lookup)
```

---

## Next Steps

- [Dimension Factory](./dimension_factory.md) - Create supporting dimensions
- [Base Factory](./base_factory.md) - CDC table details
- [Configuration System](../configuration/README.md) - Configuration management
- [Connector Framework](../connectors/README.md) - Data ingestion
