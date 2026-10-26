# Layer Factories - Factory Pattern for Data Pipelines

This section documents the **Factory Pattern** implementation used to create DLT pipelines across all data lakehouse layers: Raw, Base, Curated (Dimensions & Facts), and Enriched.

## Overview

The factory pattern is a creational design pattern that encapsulates object creation logic. In this lakehouse architecture, factories dynamically create DLT tables for each layer based on data contracts, eliminating repetitive boilerplate code and ensuring consistency across the system.

## Layer Factory Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Factory Pattern Stack                     │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  Solution Layer (src/solution/)                              │
│  ├── raw/*.py (e.g., raw_ingest_lakehouse.py)               │
│  ├── base/*.py (e.g., base_ingest_lakehouse.py)             │
│  ├── curated/*.py (e.g., curated_create_dimensions.py)      │
│  └── enriched/*.py (e.g., enriched_create_features.py)      │
│         │                                                     │
│         ▼ (calls)                                            │
│  ┌─────────────────────────────────────────────────────────┐│
│  │         Framework Factories                             ││
│  ├────────────────────────────────────────────────────────┤│
│  │ • RawPipelineFactory      - Ingestion & Connectors    ││
│  │ • BasePipelineFactory     - CDC & Data Quality        ││
│  │ • CuratedDimensionFactory - Star Schema Dimensions    ││
│  │ • CuratedFactFactory      - Star Schema Facts         ││
│  │ • EnrichedFactory         - Business Transformations  ││
│  └─────────────────────────────────────────────────────────┘│
│         │                                                     │
│         ▼ (uses)                                             │
│  ┌─────────────────────────────────────────────────────────┐│
│  │   Centralized Infrastructure (Config, Helpers, etc.)   ││
│  ├────────────────────────────────────────────────────────┤│
│  │ • CentralizedPipelineConfig    - Shared metadata      ││
│  │ • CatalogSchemaManager         - Path construction    ││
│  │ • ConnectorConfigBuilderFactory - Config building     ││
│  │ • ConnectorFactory             - Connector creation   ││
│  │ • DataContractHelper           - Contract parsing     ││
│  └─────────────────────────────────────────────────────────┘│
│         │                                                     │
│         ▼ (creates)                                          │
│  ┌─────────────────────────────────────────────────────────┐│
│  │              DLT Tables                                 ││
│  ├────────────────────────────────────────────────────────┤│
│  │ raw.<schema>.<model>       - Raw ingested data        ││
│  │ base.<schema>.<model>      - Deduplicated CDC data    ││
│  │ curated.dimensions.*       - Dimension tables         ││
│  │ curated.facts.*            - Fact tables              ││
│  │ enriched.<schema>.*        - Business features       ││
│  └─────────────────────────────────────────────────────────┘│
│                                                               │
└─────────────────────────────────────────────────────────────┘
```

## Key Benefits

| Benefit | Description |
|---------|-------------|
| **DRY (Don't Repeat Yourself)** | Eliminates repeated code across different data sources |
| **Encapsulation** | Complex logic (connectors, DQ, CDC) hidden behind simple API |
| **Consistency** | All tables follow same patterns and standards |
| **Maintainability** | Changes to layer logic made in one place |
| **Extensibility** | New connectors/transformations added without touching layer code |
| **Declarative** | Solution files are minimal, focus on *what* not *how* |
| **Type Safety** | Strong typing ensures configuration correctness |

## Layer Factories

### 1. RawPipelineFactory - Ingestion Layer

**Location**: `src/framework/factory/raw_factory.py`

**Purpose**: Dynamically creates raw layer ingestion tables using connector framework

**Key Features**:
- Reads data contracts for connectors (REST API, JDBC, S3, Autoloader, Volume)
- Two-pass schema processing (root calls first, then dependent calls)
- Automatic connector instantiation based on connector type
- Optional backfill support
- Secret resolution from Databricks secrets

**Example**:
```python
from src.framework.factory.raw_factory import create_raw_pipeline

# Solution layer - one line!
create_raw_pipeline("lakehouse")  # Creates all raw tables for lakehouse
```

**What happens internally**:
1. Loads `data_contracts/source_system/lakehouse.yml`
2. For each schema entry:
   - Extracts connector type (rest_api, jdbc, volume, etc.)
   - Creates appropriate connector builder
   - Resolves configuration and secrets
   - Creates DLT table using connector

---

### 2. BasePipelineFactory - Deduplication & CDC Layer

**Location**: `src/framework/factory/base_factory.py`

**Purpose**: Creates base layer tables with CDC (Change Data Capture) processing

**Key Features**:
- SCD Type 2 (Slowly Changing Dimensions) support
- Optional data quality validation integration
- Multi-key CDC handling
- Source table deduplication
- Both full-load and CDC modes

**Example**:
```python
from src.framework.factory.base_factory import create_base_pipeline

# Solution layer - one line!
create_base_pipeline("lakehouse")  # Creates all base tables with CDC
```

**What happens internally**:
1. Reads data contract with CDC configuration
2. For each table:
   - Checks for data quality rules
   - Creates optional DQ validation table
   - Creates CDC table (SCD Type 2) from raw data
   - Tracks historical changes with `__START_AT` and `__END_AT`

**CDC Table Structure**:
```
╔═════════════╦═══════╦═══════════════╦═══════════════════════╗
║ id          ║ name  ║ __START_AT    ║ __END_AT              ║
╠═════════════╬═══════╬═══════════════╬═══════════════════════╣
║ 1           ║ Alice ║ 2024-01-01    ║ 2024-06-15 (updated) ║
║ 1           ║ Alicia║ 2024-06-15    ║ NULL (current)       ║
║ 2           ║ Bob   ║ 2024-01-01    ║ NULL (current)       ║
╚═════════════╩═══════╩═══════════════╩═══════════════════════╝
```

---

### 3. CuratedDimensionFactory - Dimension Tables

**Location**: `src/framework/factory/dimension_factory.py`

**Purpose**: Creates standardized dimension tables with business keys and surrogate keys

**Key Features**:
- Reads from base layer (automatically filters active records)
- Adds business key columns
- Generates surrogate keys for dimensional model
- Supports custom transformations (joins, enrichments)
- Type 2 SCD support (filters `__END_AT` IS NULL)

**Example**:
```python
from src.framework.factory.dimension_factory import CuratedDimensionFactory
from pyspark.sql import SparkSession

spark = SparkSession.getActiveSession()
factory = CuratedDimensionFactory(spark, "lakehouse")

# Create dimension from base table
factory.create_dimension(
    dimension_name="dim_customer",
    source_table="customer",
    business_key_column="customer_id",
    filter_active=True  # Only active records
)
```

**Dimension Structure**:
```
curated.dimensions.dim_customer:
- customer_key (surrogate key, generated)
- customer_id (business key, renamed from source)
- [other fields from base table]
- [metadata: created_at, updated_at, etc.]
```

---

### 4. CuratedFactFactory - Fact Tables

**Location**: `src/framework/factory/fact_factory.py`

**Purpose**: Creates fact tables with automatic dimension key lookups

**Key Features**:
- Reads from base layer for fact data
- Performs automatic dimension key lookups
- Column renaming from business keys to foreign keys
- Supports custom transformations
- Pre-aggregation optional

**Example**:
```python
from src.framework.factory.fact_factory import CuratedFactFactory
from pyspark.sql import SparkSession

spark = SparkSession.getActiveSession()
factory = CuratedFactFactory(spark, "lakehouse")

# Create fact table with dimension mappings
factory.create_fact(
    fact_name="fact_bookings",
    source_table="bookings",
    dimension_mappings={
        "customer_id": "customer_key",
        "seller_id": "seller_key",
        "lakehouse_id": "lakehouse_key"
    }
)
```

**Fact Structure**:
```
curated.facts.fact_bookings:
- booking_id (grain key)
- customer_key (FK to dim_customer)
- seller_key (FK to dim_seller)
- lakehouse_key (FK to dim_lakehouse)
- revenue_amount (measure)
- booking_date (dimension)
- [other measures and dimensions]
```

---

## Factory Pattern Implementation Details

### Template Method Pattern

Each factory follows the **Template Method Pattern**:

```python
class BaseFactory:
    def create(self, entity_name: str):
        # Template method - orchestrates the process
        config = self.load_config(entity_name)
        data = self.transform_data(config)
        result = self.create_dlt_table(data)
        return result
    
    def load_config(self, entity_name: str):
        # Specific implementation
        pass
    
    def transform_data(self, config):
        # Specific implementation
        pass
    
    def create_dlt_table(self, data):
        # Common DLT creation logic
        @dlt.table(...)
        def table():
            return data
```

### Closure Variable Capture

All factories properly capture variables for DLT decorators:

```python
def create_table(self, table_name: str):
    # Capture current state
    catalog_manager = self.catalog_manager
    spark = self.spark
    centralized_config = self.centralized_config
    
    @dlt.table(name=catalog_manager.get_table_path(...))
    def _table():
        # Uses captured variables, not instance variables
        df = spark.read.table(...)
        return df
```

**Why this matters**: DLT decorators execute in different execution contexts. Capturing prevents "stale reference" errors.

### Configuration-Driven Creation

Factories read from data contracts and configuration:

```yaml
# data_contracts/source_system/lakehouse.yml
schema:
  - name: customer
    connector_type: rest_api
    endpoint: "https://api.example.com/customers"
    auth_type: bearer
    auth_token: "{{secrets/lakehouse/api_token}}"
    keys: [customer_id]
    sequence_column: updated_at
    stored_as_scd_type: 2
```

Factory processes this YAML to:
1. Extract connector configuration
2. Build connector instance
3. Create DLT table with proper schema

---

## Data Flow Through Factories

### Raw Layer Example
```
┌──────────────────────────────────────────┐
│ REST API (External Source)               │
│ https://api.example.com/customers        │
└────────────────────┬─────────────────────┘
                     │
                     ▼
        ┌────────────────────────────┐
        │  RawPipelineFactory        │
        │  ├─ Load data contract     │
        │  ├─ Create REST connector  │
        │  ├─ Resolve secrets        │
        │  └─ Call HTTP API          │
        └────────────────┬───────────┘
                         │
                         ▼
        ┌────────────────────────────┐
        │ DLT Table Creation         │
        │ @dlt.table(                │
        │   name="raw.landing.       │
        │         customer"          │
        │ )                          │
        └────────────────┬───────────┘
                         │
                         ▼
        ┌────────────────────────────┐
        │ raw.landing.customer       │
        │ [schema from API response] │
        └────────────────────────────┘
```

### Base Layer Example
```
┌──────────────────────────────────────┐
│ raw.landing.customer                 │
│ (Original, raw API data)             │
└────────────────────┬─────────────────┘
                     │
                     ▼
        ┌────────────────────────────┐
        │  BasePipelineFactory       │
        │  ├─ Load CDC config        │
        │  ├─ Check DQ rules         │
        │  ├─ Apply dedup logic      │
        │  └─ Track changes          │
        └────────────────┬───────────┘
                         │
                         ▼
        ┌────────────────────────────┐
        │ DLT Table Creation         │
        │ @dlt.table(                │
        │   name="base.base.         │
        │         customer"          │
        │ )                          │
        └────────────────┬───────────┘
                         │
                         ▼
        ┌────────────────────────────┐
        │ base.base.customer         │
        │ ├─ Deduplicated data      │
        │ ├─ SCD Type 2 tracking    │
        │ └─ Quality validated      │
        └────────────────────────────┘
```

### Curated Layer Example
```
┌──────────────────────────┐
│ base.base.customer       │
│ (Cleaned, deduplicated)  │
└────────────┬─────────────┘
             │
             ├─────────────────────────────┐
             │                             │
             ▼                             ▼
┌──────────────────────────┐  ┌──────────────────────────┐
│ DimensionFactory         │  │ FactFactory              │
│ ├─ Filter active rows    │  │ ├─ Read fact source      │
│ ├─ Add surrogate key     │  │ ├─ Lookup dimension keys │
│ └─ Business transforms   │  │ ├─ Rename columns       │
└────────────┬─────────────┘  └────────────┬─────────────┘
             │                             │
             ▼                             ▼
┌──────────────────────────┐  ┌──────────────────────────┐
│ curated.dimensions.      │  │ curated.facts.           │
│ dim_customer             │  │ fact_bookings            │
└──────────────────────────┘  └──────────────────────────┘
```

---

## Integration Points

### 1. Configuration System
Factories depend on:
- **CentralizedPipelineConfig**: Shared catalog/schema names
- **CatalogSchemaManager**: Path construction helpers
- **ConnectorConfigBuilderFactory**: Configuration building

### 2. Connector Framework
Raw factory uses:
- **ConnectorFactory**: Creates connector instances (REST API, JDBC, etc.)
- **ConnectorConfigBuilder**: Builds connector-specific config

### 3. Data Contracts
All factories read from:
- **DataContractHelper**: Parses YAML contracts
- **Schema definitions**: Define table structure and CDC rules

### 4. Helpers
Factories leverage:
- **DQXHelper**: Data quality validation
- **DatabricksHelper**: Spark config retrieval
- **LoggingHelper**: Structured logging

---

## Common Patterns

### Single-Line Solution Implementation
```python
# src/solution/raw/raw_ingest_lakehouse.py
"""Raw layer pipeline for lakehouse."""
from src.framework.factory.raw_factory import create_raw_pipeline

create_raw_pipeline("lakehouse")  # That's it!
```

### Error Handling
```python
# All factories include try-catch for robustness
for schema in contract.schemas:
    try:
        self._process_schema(schema)
    except Exception as e:
        logger.error(f"Error processing {schema.name}: {e}")
        continue  # Don't fail entire pipeline
```

### Logging
```python
# Structured logging for debugging
logger.info(f"Creating table: {model_name}")
logger.debug(f"Applied transformation: {transform_name}")
logger.error(f"Failed to process {model_name}: {error}")
```

### Configuration Validation
```python
# Validate config before use
centralized_config = CentralizedPipelineConfig.from_spark(...)
centralized_config.validate()  # Raises if invalid
```

---

## When to Use Each Factory

| Factory | When to Use | Example |
|---------|-----------|---------|
| **RawPipelineFactory** | Ingesting external data | REST APIs, databases, files |
| **BasePipelineFactory** | CDC processing & dedup | Type 2 slowly-changing dimensions |
| **DimensionFactory** | Creating reference tables | Customer, Product, Date dimensions |
| **FactFactory** | Creating fact tables | Orders, Transactions, Events |
| **EnrichedFactory** | Business transformations | Feature engineering, aggregations |

---

## Next Steps

For detailed information about each factory:

- [**RawPipelineFactory**](./raw_factory.md) - Ingestion with connectors
- [**BasePipelineFactory**](./base_factory.md) - CDC and deduplication
- [**CuratedDimensionFactory**](./dimension_factory.md) - Dimension table creation
- [**CuratedFactFactory**](./fact_factory.md) - Fact table creation
- [**Factory Integration Guide**](./factory_integration.md) - How to extend and customize

See [**ARCHITECTURE.md**](./ARCHITECTURE.md) for deep-dive design patterns.

See [**QUICK_REFERENCE.md**](./QUICK_REFERENCE.md) for quick API reference.
