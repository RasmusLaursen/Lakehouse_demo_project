# Dimension Factory - Star Schema Dimension Creation

**File**: `src/framework/factory/dimension_factory.py`

## Overview

The `CuratedDimensionFactory` creates standardized dimension tables for the star schema. It reads from deduplicated base layer tables, optionally filters for active records (SCD Type 2), applies business transformations, and creates surrogate keys.

## Purpose

Create conformed dimension tables that support dimensional modeling with:
- Business keys from base layer
- Surrogate keys for referential integrity
- Historical tracking through SCD Type 2
- Optional custom transformations (joins, enrichments)

---

## Class: CuratedDimensionFactory

### Constructor

```python
class CuratedDimensionFactory:
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

### create_dimension(...)

**Signature**:
```python
def create_dimension(
    self,
    dimension_name: str,
    source_table: str,
    business_key_column: str,
    filter_active: bool = True,
    additional_transforms: Optional[Callable[[DataFrame], DataFrame]] = None
) -> None:
    """Create a dimension table with standardized pattern.
    
    Creates a DLT dimension table that:
    1. Reads from base layer
    2. Optionally filters active records (__END_AT IS NULL)
    3. Applies custom transformations
    4. Renames business key to {entity}_key
    5. Adds surrogate key as {entity}_id
    
    Args:
        dimension_name: Name of dimension (e.g., 'dim_customer')
        source_table: Name of source table in base layer (e.g., 'customer')
        business_key_column: Column name of business key (e.g., 'customer_id')
        filter_active: Whether to filter for active records only (default: True)
        additional_transforms: Optional function to apply custom transformations
    """
```

**Flow Diagram**:
```
create_dimension(dimension_name, source_table, business_key_column, ...)
│
├─ @dlt.table decorator
│  └─ Registers table with DLT
│
└─ Function body:
   ├─ Read base layer table
   │  └─ spark.read.table("base.base.{source_table}")
   │
   ├─ Filter active records (if enabled)
   │  └─ .filter(col("__END_AT").isNull())
   │
   ├─ Apply custom transformations (if provided)
   │  └─ additional_transforms(df)
   │
   ├─ Add surrogate key
   │  ├─ Rename: {business_key} → {entity}_key
   │  └─ Generate: {entity}_id (monotonically increasing)
   │
   └─ Return clean dimension
```

**Example**:
```python
spark = SparkSession.getActiveSession()
factory = CuratedDimensionFactory(spark, "lakehouse")

# Create simple dimension
factory.create_dimension(
    dimension_name="dim_customer",
    source_table="customer",
    business_key_column="customer_id"
)

# Creates: curated.dimensions.dim_customer
```

---

## Dimension Transformation Pipeline

### Step 1: Read Base Table

```python
# Read base layer table
df = spark.read.table("base.base.customer")

# Result:
┌──────────┬──────────┬──────────┬──────────────────┬──────────────────┐
│ id       │ name     │ email    │ __START_AT       │ __END_AT         │
├──────────┼──────────┼──────────┼──────────────────┼──────────────────┤
│ 1        │ Alice    │ a@ex.com │ 2024-01-01       │ 2024-06-15       │
│ 1        │ Alicia   │ a@ex.com │ 2024-06-15       │ NULL             │
│ 2        │ Bob      │ b@ex.com │ 2024-01-01       │ NULL             │
│ 3        │ Charlie  │ c@ex.com │ 2024-08-01       │ NULL             │
└──────────┴──────────┴──────────┴──────────────────┴──────────────────┘
```

### Step 2: Filter Active Records (Optional)

```python
# If filter_active=True
df = df.filter(col("__END_AT").isNull())

# Result: Only current versions
┌──────────┬──────────┬──────────┬──────────────────┬──────────────────┐
│ id       │ name     │ email    │ __START_AT       │ __END_AT         │
├──────────┼──────────┼──────────┼──────────────────┼──────────────────┤
│ 1        │ Alicia   │ a@ex.com │ 2024-06-15       │ NULL             │
│ 2        │ Bob      │ b@ex.com │ 2024-01-01       │ NULL             │
│ 3        │ Charlie  │ c@ex.com │ 2024-08-01       │ NULL             │
└──────────┴──────────┴──────────┴──────────────────┴──────────────────┘
```

### Step 3: Apply Custom Transformations

```python
# Example: join with address table
def enrich_customer(df):
    address = spark.read.table("base.base.address")
    return df.join(address, on="id")

factory.create_dimension(
    dimension_name="dim_customer",
    source_table="customer",
    business_key_column="customer_id",
    additional_transforms=enrich_customer
)

# Result includes joined columns
```

### Step 4: Add Surrogate Key

```python
# Before:
┌──────────┬──────────┬──────────┐
│ id       │ name     │ email    │
├──────────┼──────────┼──────────┤
│ 1        │ Alicia   │ a@ex.com │
│ 2        │ Bob      │ b@ex.com │
│ 3        │ Charlie  │ c@ex.com │
└──────────┴──────────┴──────────┘

# After:
┌──────────────┬────────────┬──────────┬──────────┐
│ customer_id  │ customer_k │ name     │ email    │
│              │ ey         │          │          │
├──────────────┼────────────┼──────────┼──────────┤
│ 1            │ 1          │ Alicia   │ a@ex.com │
│ 2            │ 2          │ Bob      │ b@ex.com │
│ 3            │ 3          │ Charlie  │ c@ex.com │
└──────────────┴────────────┴──────────┴──────────┘
        ↓                ↓
   Business Key    Surrogate Key
   (from source)   (generated)
```

---

## Dimension Structure

### Typical Dimension Table

```sql
SELECT * FROM curated.dimensions.dim_customer LIMIT 3;

-- Output:
┌────────────┬────────────┬──────────┬──────────┬────────────────┐
│customer_id │customer_key│name      │email     │joined_date     │
├────────────┼────────────┼──────────┼──────────┼────────────────┤
│ 1          │ 1          │ Alicia   │ a@ex.com │ 2024-01-01     │
│ 2          │ 2          │ Bob      │ b@ex.com │ 2024-01-01     │
│ 3          │ 3          │ Charlie  │ c@ex.com │ 2024-08-01     │
└────────────┴────────────┴──────────┴──────────┴────────────────┘
```

### Column Naming Convention

| Column Type | Pattern | Example |
|-------------|---------|---------|
| Surrogate Key | `{entity}_key` | `customer_key` |
| Business Key | `{entity}_id` | `customer_id` |
| Attributes | Original names | `name`, `email` |
| Temporal | Original names | `created_at`, `updated_at` |

---

## Usage Examples

### Simple Dimension (Customer)

```python
from pyspark.sql import SparkSession
from src.framework.factory.dimension_factory import CuratedDimensionFactory

spark = SparkSession.getActiveSession()
factory = CuratedDimensionFactory(spark, "lakehouse")

# Create customer dimension
factory.create_dimension(
    dimension_name="dim_customer",
    source_table="customer",
    business_key_column="customer_id"
)

# Creates: curated.dimensions.dim_customer
```

### Dimension with Filtering

```python
# Only active customers (no soft deletes)
factory.create_dimension(
    dimension_name="dim_customer",
    source_table="customer",
    business_key_column="customer_id",
    filter_active=True  # Filters __END_AT IS NULL
)

# vs. All customer versions (including inactive)
factory.create_dimension(
    dimension_name="dim_customer_history",
    source_table="customer",
    business_key_column="customer_id",
    filter_active=False  # Keeps all versions
)
```

### Dimension with Custom Transformation

```python
# Join customer with address for enriched dimension
def enrich_with_address(df):
    address = spark.read.table("base.base.address")
    return (df.join(address, on="id")
            .select("customer_id", "name", "email", "street", "city"))

factory.create_dimension(
    dimension_name="dim_customer",
    source_table="customer",
    business_key_column="customer_id",
    additional_transforms=enrich_with_address
)

# Creates: curated.dimensions.dim_customer (with address data)
```

### Multiple Dimensions

```python
spark = SparkSession.getActiveSession()
factory = CuratedDimensionFactory(spark, "lakehouse")

# Create multiple dimensions
factory.create_dimension("dim_customer", "customer", "customer_id")
factory.create_dimension("dim_seller", "seller", "seller_id")
factory.create_dimension("dim_lakehouse", "lakehouse", "lakehouse_id")
factory.create_dimension("dim_date", "date_dim", "date_id")

# All available for fact tables
```

---

## Private Helper Methods

### _add_surrogate_key(...)

**Signature**:
```python
def _add_surrogate_key(
    self,
    df: DataFrame,
    business_key_column: str
) -> DataFrame:
    """Add surrogate key to dimension table.
    
    Renames business key to {entity}_key and creates new {entity}_id
    """
```

**Implementation**:
```python
# Extract entity name from business key
# Example: customer_id → customer
entity_name = business_key_column.replace("_id", "")

# Rename business key to {entity}_key
df = df.withColumnRenamed(
    business_key_column,
    f"{entity_name}_key"
)

# Add surrogate key
df = df.withColumn(
    f"{entity_name}_id",
    monotonically_increasing_id()
)

return df
```

---

## Integration with Fact Tables

### Reference in Fact Tables

```python
# Fact table uses dimension keys
fact_df = spark.read.table("curated.facts.fact_bookings")

print(fact_df.schema)
# StructType([
#   StructField("booking_id", IntegerType),
#   StructField("customer_key", IntegerType),  ← FK to dim_customer
#   StructField("seller_key", IntegerType),    ← FK to dim_seller
#   StructField("revenue", DecimalType),
#   StructField("booking_date", DateType)
# ])
```

### Joining with Dimensions

```python
from pyspark.sql.functions import col

fact = spark.read.table("curated.facts.fact_bookings")
customer_dim = spark.read.table("curated.dimensions.dim_customer")

# Join fact with dimension
enriched = (fact
    .join(customer_dim, 
          on=fact.customer_key == customer_dim.customer_key,
          how="left")
    .select(
        fact.booking_id,
        customer_dim.name,
        customer_dim.email,
        fact.revenue
    ))

enriched.show()
```

---

## Data Flow

### Complete Star Schema Example

```
Base Layer:
├─ base.base.customer
├─ base.base.seller
├─ base.base.lakehouse
└─ base.base.booking

    ↓ (DimensionFactory)

Curated Dimensions:
├─ curated.dimensions.dim_customer
├─ curated.dimensions.dim_seller
└─ curated.dimensions.dim_lakehouse

Base Fact Source:
└─ base.base.booking

    ↓ (FactFactory with dimension lookups)

Curated Facts:
└─ curated.facts.fact_bookings (with FK to dimensions)

    ↓ (Ready for Analytics)

Reports & BI Tools
```

---

## Design Decisions

### Why Surrogate Keys?

**Benefits**:
- Small integer keys (4-8 bytes) instead of large business keys
- Referential integrity across systems
- Performance in joins
- Insulates from business key changes

### Why Active Record Filtering?

**Options**:
1. Filter active only (filter_active=True) - default
2. Keep all versions (filter_active=False)

**Decision**: Default to active only because:
- Most analytics use current data
- Simpler queries
- Better performance

### Why Custom Transformations?

**Reasons**:
- Some dimensions need enrichment (addresses, categories, etc.)
- Don't want separate enrichment step
- Keeps all dimension logic in one place

---

## Performance Considerations

### Filtering Impact

```python
# filter_active=True: Uses WHERE clause, efficient
df = df.filter(col("__END_AT").isNull())
# Pushes down to storage layer, minimal data

# filter_active=False: No filter, all data
df = spark.read.table(...)
# Larger table, more memory
```

### Custom Transforms

```python
# Performance depends on join complexity
# Example: 1M customer + 1M address join = minimal cost
# Example: 1M customer + 100M audit logs join = expensive

# Mitigate with:
# - Filter left side first (active only)
# - Index join keys
# - Partition by join key
```

### Surrogate Key Generation

```python
# monotonically_increasing_id() is fast but:
# - Not sequential
# - May have gaps if data repartitioned
# - Alternative: row_number() over (order by business_key)
```

---

## Error Handling

### Missing Source Table

```python
try:
    df = spark.read.table(f"base.base.{source_table}")
except Exception as e:
    logger.error(f"Source table not found: base.base.{source_table}")
    raise
```

### Missing Business Key Column

```python
# Validated implicitly when adding surrogate key
# If column doesn't exist, withColumnRenamed fails
```

---

## Troubleshooting

### Dimension Table Empty

**Causes**:
1. Source table is empty
2. filter_active=True but no active records exist

**Check**:
```python
spark.read.table("base.base.customer").show()
spark.read.table("base.base.customer").filter(col("__END_AT").isNull()).count()
```

### Wrong Surrogate Keys

**Issue**: Keys not sequential or missing

**Why**: `monotonically_increasing_id()` is not sequential

**Solution**: Use row_number() if needed
```python
df = df.withColumn(
    f"{entity_name}_id",
    row_number().over(Window.orderBy(f"{entity_name}_key"))
)
```

### Custom Transform Fails

**Debug**:
1. Test transform separately
2. Check column names (case-sensitive)
3. Verify join conditions

```python
# Test separately
test_df = spark.read.table("base.base.customer")
result = enrich_with_address(test_df)
result.show()
```

---

## Extensions

### Type 1 SCD Dimensions

If you want to overwrite history instead of Type 2:

```python
factory.create_dimension(
    dimension_name="dim_category",
    source_table="category",
    business_key_column="category_id",
    filter_active=True  # Only current, Type 1 style
)
```

### Slowly Changing Dimension Type 3

Track one previous value:

```python
# Implement custom logic in additional_transforms
def scd_type3(df):
    # Add "previous_value" column
    return df.withColumn(...)

factory.create_dimension(
    dimension_name="dim_customer_type3",
    source_table="customer",
    business_key_column="customer_id",
    additional_transforms=scd_type3
)
```

---

## Next Steps

- [Fact Factory](./fact_factory.md) - Create fact tables referencing dimensions
- [Base Factory](./base_factory.md) - CDC table details
- [Configuration System](../configuration/README.md) - Configuration management
