# Factory Documentation Index

Navigation guide for the Layer Factory Pattern documentation.

## Documentation Hierarchy

```
factory/
├── README.md (START HERE)
│   └─ Overview and architecture
│
├── QUICK_REFERENCE.md
│   └─ API reference and common patterns
│
├── ARCHITECTURE.md (Coming soon)
│   └─ Deep design patterns
│
├── INDEX.md (this file)
│   └─ Navigation guide
│
├── raw_factory.md
│   └─ Raw layer ingestion
│
├── base_factory.md
│   └─ Base layer CDC and deduplication
│
├── dimension_factory.md
│   └─ Dimension table creation
│
└── fact_factory.md
    └─ Fact table creation
```

---

## Quick Navigation

### By Use Case

**"I want to ingest data"**
→ [RawPipelineFactory](./raw_factory.md)
- Reads from REST APIs, databases, files
- Creates raw layer tables
- Handles multiple connector types

**"I want to track changes"**
→ [BasePipelineFactory](./base_factory.md)
- CDC (Change Data Capture)
- SCD Type 2 (Slowly Changing Dimensions)
- Historical tracking with temporal columns

**"I want to create star schema"**
→ [DimensionFactory](./dimension_factory.md) + [FactFactory](./fact_factory.md)
- Dimension tables (reference data)
- Fact tables (measurable events)
- Surrogate keys and foreign keys

**"I need quick API reference"**
→ [QUICK_REFERENCE.md](./QUICK_REFERENCE.md)
- Constructor signatures
- Method parameters
- Code examples
- Common patterns

### By Layer

| Layer | Factory | File |
|-------|---------|------|
| Raw | RawPipelineFactory | [raw_factory.md](./raw_factory.md) |
| Base | BasePipelineFactory | [base_factory.md](./base_factory.md) |
| Curated (Dimensions) | CuratedDimensionFactory | [dimension_factory.md](./dimension_factory.md) |
| Curated (Facts) | CuratedFactFactory | [fact_factory.md](./fact_factory.md) |

---

## Documentation Structure

### README.md - Start Here
**Length**: ~600 lines | **Time**: 15-20 min read

**Contents**:
- Architecture overview
- Factory pattern benefits
- Layer factory descriptions
- Data flow examples
- Integration points
- When to use each factory

**Best for**: Understanding the big picture

---

### QUICK_REFERENCE.md - API at a Glance
**Length**: ~400 lines | **Time**: 5-10 min reference

**Contents**:
- Quick comparison table
- One-liner APIs
- Parameter tables
- Usage examples
- Data contract templates
- Debugging tips

**Best for**: Quick lookups and copy-paste code

---

### raw_factory.md - Ingestion Deep Dive
**Length**: ~450 lines | **Time**: 20-30 min read

**Contents**:
- Purpose and overview
- Constructor and methods
- Two-pass schema processing
- Connector integration
- Data contract structure
- Configuration resolution
- Error handling
- Usage examples
- Design decisions
- Troubleshooting

**Best for**: Understanding raw layer ingestion

---

### base_factory.md - CDC Deep Dive
**Length**: ~500 lines | **Time**: 25-35 min read

**Contents**:
- Purpose (CDC, deduplication)
- Constructor and methods
- SCD Type 2 transformation
- Before/after examples
- DQ integration
- CDC configuration in YAML
- DLT APPLY CHANGES
- Temporal columns
- Query patterns
- Error handling
- Extensions

**Best for**: Understanding CDC and change tracking

---

### dimension_factory.md - Star Schema Deep Dive
**Length**: ~450 lines | **Time**: 20-30 min read

**Contents**:
- Purpose (dimensional modeling)
- Constructor and methods
- Transformation pipeline
- Dimension structure
- Surrogate keys
- Usage examples
- Integration with facts
- Design decisions
- Performance
- Extensions

**Best for**: Understanding dimensional modeling

---

### fact_factory.md - Fact Tables Deep Dive
**Length**: ~500 lines | **Time**: 25-35 min read

**Contents**:
- Purpose (fact tables)
- Constructor and methods
- Transformation pipeline
- Dimension key lookups
- Fact structure and grain
- Integration with dimensions
- Star schema examples
- Data types and grain
- Usage examples
- Performance
- Extensions

**Best for**: Understanding fact table creation and queries

---

## Learning Path

### Path 1: Complete Overview (60-90 minutes)
1. [README.md](./README.md) - 20 min
2. [QUICK_REFERENCE.md](./QUICK_REFERENCE.md) - 10 min
3. [raw_factory.md](./raw_factory.md) - 25 min
4. [base_factory.md](./base_factory.md) - 25 min

### Path 2: Star Schema Focus (45-60 minutes)
1. [README.md](./README.md) - 20 min (skip raw/base sections)
2. [dimension_factory.md](./dimension_factory.md) - 20 min
3. [fact_factory.md](./fact_factory.md) - 20 min

### Path 3: Practical Quick Start (15-20 minutes)
1. [QUICK_REFERENCE.md](./QUICK_REFERENCE.md) - 10 min
2. Copy code examples and modify for your use case

### Path 4: Troubleshooting (10-15 minutes)
1. [QUICK_REFERENCE.md](./QUICK_REFERENCE.md) - Debugging section
2. Relevant factory file troubleshooting sections

---

## Topics by File

### Data Contracts & Configuration
- README.md - Configuration overview
- raw_factory.md - Data contract structure
- base_factory.md - CDC configuration in YAML
- QUICK_REFERENCE.md - Configuration examples

### CDC & Change Tracking
- base_factory.md - Complete CDC guide
- base_factory.md - SCD Type 2 before/after
- dimension_factory.md - Filtering active records
- QUICK_REFERENCE.md - Query current records

### Star Schema
- README.md - Star schema overview
- dimension_factory.md - Dimension creation
- fact_factory.md - Fact creation
- fact_factory.md - Dimension key lookups
- QUICK_REFERENCE.md - Star schema examples

### Error Handling
- raw_factory.md - Per-table error handling
- base_factory.md - Per-schema error handling
- Each file - Troubleshooting section
- QUICK_REFERENCE.md - Common issues table

### Performance
- raw_factory.md - Connector types
- base_factory.md - CDC performance
- dimension_factory.md - Surrogate key generation
- fact_factory.md - Grain impacts
- QUICK_REFERENCE.md - Performance tips

---

## Feature Comparison

### Which Factory?

| Need | Factory | Reference |
|------|---------|-----------|
| Ingest REST API | RawPipelineFactory | [raw_factory.md](./raw_factory.md) |
| Ingest database | RawPipelineFactory | [raw_factory.md](./raw_factory.md) |
| Track changes | BasePipelineFactory | [base_factory.md](./base_factory.md) |
| Deduplication | BasePipelineFactory | [base_factory.md](./base_factory.md) |
| Dimension tables | DimensionFactory | [dimension_factory.md](./dimension_factory.md) |
| Fact tables | FactFactory | [fact_factory.md](./fact_factory.md) |
| Star schema | Both curated | [dimension_factory.md](./dimension_factory.md) + [fact_factory.md](./fact_factory.md) |

---

## Code Examples by Factory

### RawPipelineFactory

**File**: [raw_factory.md](./raw_factory.md)

Basic pipeline:
```python
from src.framework.factory.raw_factory import RawPipelineFactory
factory = RawPipelineFactory(spark)
factory.create_pipeline("lakehouse")
```

Solution layer:
```python
from src.framework.factory.raw_factory import create_raw_pipeline
create_raw_pipeline("lakehouse")
```

---

### BasePipelineFactory

**File**: [base_factory.md](./base_factory.md)

Basic pipeline:
```python
from src.framework.factory.base_factory import BasePipelineFactory
factory = BasePipelineFactory(spark)
factory.create_pipeline("lakehouse")
```

Query current records:
```python
from pyspark.sql.functions import col
current = spark.read.table("base.base.customer").filter(col("__END_AT").isNull())
```

---

### DimensionFactory

**File**: [dimension_factory.md](./dimension_factory.md)

Simple dimension:
```python
from src.framework.factory.dimension_factory import CuratedDimensionFactory
factory = CuratedDimensionFactory(spark, "lakehouse")
factory.create_dimension("dim_customer", "customer", "customer_id")
```

With transformation:
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

---

### FactFactory

**File**: [fact_factory.md](./fact_factory.md)

Simple fact:
```python
from src.framework.factory.fact_factory import CuratedFactFactory
factory = CuratedFactFactory(spark, "lakehouse")
factory.create_fact(
    "fact_bookings",
    "bookings",
    {
        "customer_id": "customer_key",
        "seller_id": "seller_key"
    }
)
```

With aggregation:
```python
from pyspark.sql.functions import col, year, sum as spark_sum

def aggregate(df):
    return (df
        .withColumn("booking_year", year(col("booking_date")))
        .groupBy("customer_id", "booking_year")
        .agg(spark_sum("revenue").alias("total_revenue")))

factory.create_fact(
    "fact_bookings_yearly",
    "bookings",
    {"customer_id": "customer_key"},
    additional_transforms=aggregate
)
```

---

## Section Highlights

### Key Concepts by File

**README.md**:
- Factory pattern definition
- Benefits (DRY, consistency, maintainability)
- Architecture diagram
- Integration points

**raw_factory.md**:
- Two-pass schema processing (root calls, then dependent)
- Connector integration
- Secret resolution
- Backfill support

**base_factory.md**:
- SCD Type 2 (tracking all changes)
- Before/after examples
- Temporal columns (`__START_AT`, `__END_AT`)
- DQ validation

**dimension_factory.md**:
- Surrogate keys
- Business keys
- SCD Type 2 filtering
- Custom transformations

**fact_factory.md**:
- Grain (level of detail)
- Dimension key lookups
- Fact types (transaction, snapshot)
- Star schema integration

---

## For Different Roles

### Data Engineer
**Start with**: [raw_factory.md](./raw_factory.md)
**Then read**: [base_factory.md](./base_factory.md)
**Reference**: [QUICK_REFERENCE.md](./QUICK_REFERENCE.md)

### Data Analyst
**Start with**: [README.md](./README.md)
**Then read**: [dimension_factory.md](./dimension_factory.md), [fact_factory.md](./fact_factory.md)
**Reference**: [QUICK_REFERENCE.md](./QUICK_REFERENCE.md) - Querying section

### DevOps/Platform
**Start with**: [README.md](./README.md)
**Then read**: [raw_factory.md](./raw_factory.md) - Integration points
**Focus on**: Error handling and troubleshooting sections

### Data Architect
**Start with**: [README.md](./README.md)
**Then read**: All files for complete picture
**Special focus**: Integration points and design decisions

---

## Related Documentation

- [Configuration System](../configuration/README.md) - Config management
- [Connector Framework](../connectors/README.md) - Data source connectors
- [Full Documentation Index](../README.md) - All documentation

---

## File Statistics

| File | Lines | Topics | Examples |
|------|-------|--------|----------|
| README.md | 550+ | 10+ | 15+ |
| QUICK_REFERENCE.md | 400+ | 8+ | 20+ |
| raw_factory.md | 450+ | 12+ | 10+ |
| base_factory.md | 500+ | 14+ | 15+ |
| dimension_factory.md | 450+ | 13+ | 15+ |
| fact_factory.md | 500+ | 14+ | 15+ |
| **TOTAL** | **2,850+** | **71+** | **90+** |

---

## Search Tips

### Finding Information

**CDC & Change Tracking**:
- base_factory.md → "SCD Type 2 Transformation"
- dimension_factory.md → "Filtering active records"

**Dimension Key Lookups**:
- fact_factory.md → "Dimension Lookups"

**Surrogate Keys**:
- dimension_factory.md → "Surrogate key generation"

**Error Handling**:
- Each file → "Troubleshooting" section

**Star Schema**:
- dimension_factory.md + fact_factory.md
- README.md → "Curated Layer Example"

---

## Printing & Export

### Recommended Pages to Print

1. QUICK_REFERENCE.md (for desk reference)
2. Factory-specific file (as needed)

### Export Formats

All files are in Markdown. Export to:
- PDF (use VS Code or browser)
- HTML (via GitHub or static site)
- Word (via Pandoc or Word import)

---

## Updates & Maintenance

- [README.md](./README.md) - Architecture is stable
- Factory-specific files - Update when implementation changes
- [QUICK_REFERENCE.md](./QUICK_REFERENCE.md) - Keep in sync with APIs

Last Updated: December 2024
Next Review: Q2 2025

---

## Getting Started

**First time here?** → Start with [README.md](./README.md)

**Need specific info?** → Check the table of contents above

**Need working code?** → Go to [QUICK_REFERENCE.md](./QUICK_REFERENCE.md)

**Debugging issue?** → Find your factory in the files and look for "Troubleshooting"

---

## Contact & Support

For questions about factories:
1. Check the relevant factory file's troubleshooting section
2. Review error messages for hints
3. Check related configuration documentation
4. Review data contracts for configuration issues
