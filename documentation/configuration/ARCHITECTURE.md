# Architecture Guide

## Overview

This guide explains the overall design and architecture of the configuration system, including design patterns, data flows, and extension points.

## Architecture Layers

The configuration system is organized into distinct layers:

### Layer 1: Centralized Metadata
**Component**: `CentralizedPipelineConfig`

Manages pipeline-wide shared settings:
- Catalogs for each layer
- Schemas for each layer
- Environment and source system
- Default settings

**Responsibility**: Single source of truth for pipeline metadata

### Layer 2: Connector Configuration
**Component**: `ConnectorConfig`

Wraps connector-specific settings:
- Connector type identification
- Type-agnostic configuration dictionary
- Get/set API for access
- Secret extraction support

**Responsibility**: Clean API for connector-specific parameters

### Layer 3: Path Management
**Component**: `CatalogSchemaManager`

Constructs fully qualified table paths:
- Generic table paths: `catalog.schema.table`
- Layer-specific paths: raw, base, dimensions, facts, enriched
- Eliminates hardcoded paths

**Responsibility**: Centralized path construction

### Layer 4: Secret Resolution
**Component**: `SecretResolver`

Resolves secret references to actual values:
- Supports multiple formats (Spark config, Databricks secrets, protocol)
- Integrates with Databricks secret store
- Provides logging without exposing secrets

**Responsibility**: Secure secret resolution

### Layer 5: Configuration Building
**Components**: `BaseConfigBuilder` + Subclasses

Builds final configuration using Template Method Pattern:
- Base class defines common workflow
- Subclasses implement connector-specific logic
- Fluent API for method chaining

**Responsibility**: Configuration building workflow

### Layer 6: Builder Instantiation
**Component**: `ConnectorConfigBuilderFactory`

Factory for creating appropriate builders:
- Maps connector types to builders
- Supports runtime registration
- Provides type discovery

**Responsibility**: Builder instantiation

## Design Patterns

### 1. Template Method Pattern
**Used In**: BaseConfigBuilder and subclasses

**How It Works**:
```python
# Base class defines the algorithm structure
class BaseConfigBuilder:
    def merge_schema_overrides(self, schema):
        # Common implementation for all builders
        pass
    
    def resolve_secrets(self):
        # Common implementation for all builders
        pass
    
    def merge_shared_context(self):
        # Abstract - subclasses implement
        raise NotImplementedError()

# Subclass implements only the specific part
class VolumeConfigBuilder(BaseConfigBuilder):
    def merge_shared_context(self):
        # Volume-specific implementation
        context = {"catalog": self.pipeline_config.landing_catalog}
        return self
```

**Benefits**:
- Code reuse for common steps
- Consistent workflow
- Easy to extend with new connector types

### 2. Factory Pattern
**Used In**: ConnectorConfigBuilderFactory

**How It Works**:
```python
class ConnectorConfigBuilderFactory:
    BUILDER_MAPPING = {
        "volume": VolumeConfigBuilder,
        "rest_api": RestApiConfigBuilder,
        # ... more types
    }
    
    @staticmethod
    def create_builder(connector_type, config, centralized):
        builder_class = BUILDER_MAPPING[connector_type]
        return builder_class(config, centralized)
```

**Benefits**:
- Centralizes builder instantiation
- Extensible via registration
- Type-safe instantiation

### 3. Strategy Pattern
**Used In**: Different builder implementations

**How It Works**:
```python
# Each builder has different strategy for merge_shared_context()
class VolumeConfigBuilder:
    def merge_shared_context(self):
        # Volume strategy
        return self

class RestApiConfigBuilder:
    def merge_shared_context(self):
        # REST API strategy
        return self
```

**Benefits**:
- Encapsulates algorithms
- Allows runtime selection
- Easy to add new strategies

### 4. Wrapper Pattern
**Used In**: ConnectorConfig

**How It Works**:
```python
class ConnectorConfig:
    def __init__(self, connector_type, config_dict=None):
        self._connector_type = connector_type
        self._config = config_dict or {}
    
    def get(self, key, default=None):
        return self._config.get(key, default)
    
    def set(self, key, value):
        self._config[key] = value
```

**Benefits**:
- Clean API for dictionary manipulation
- Type information included
- Easy to add business logic

## Data Flow

### Configuration Building Workflow

```
Data Contract (YAML)
        ↓
    ┌───────────────────────────┐
    │ 1. Parse Data Contract    │
    │ - Extract server config   │
    │ - Extract schema config   │
    └───────────────────────────┘
        ↓
    ┌───────────────────────────┐
    │ 2. Create ConnectorConfig │
    │ from_server_config()      │
    └───────────────────────────┘
        ↓
    ┌───────────────────────────┐
    │ 3. Create Builder via     │
    │ Factory.create_builder()  │
    └───────────────────────────┘
        ↓
    ┌───────────────────────────┐
    │ 4. merge_shared_context() │
    │ Add centralized settings  │
    └───────────────────────────┘
        ↓
    ┌───────────────────────────┐
    │ 5. merge_schema_overrides()
    │ Apply schema properties   │
    └───────────────────────────┘
        ↓
    ┌───────────────────────────┐
    │ 6. resolve_secrets()      │
    │ Resolve {{...}} refs      │
    └───────────────────────────┘
        ↓
    ┌───────────────────────────┐
    │ 7. build()                │
    │ Return final config dict  │
    └───────────────────────────┘
        ↓
Final Configuration (dict)
```

### Configuration Flow for REST API Workflow

```
1. Data Contract
   ├─ server config: connector_type="rest_api_workflow_ds"
   │  ├─ endpoint: "https://api.example.com"
   │  ├─ auth_type: "oauth2_refresh"
   │  └─ refresh_token: "{{secrets/scope/token}}"
   └─ schema config: "meterdata"
      ├─ table_name: "meterdata/{from}/{to}"
      ├─ workflow_step: 2
      └─ depends_on: "metering_points"

2. ConnectorConfig
   {"endpoint": "https://...", "auth_type": "oauth2_refresh", ...}

3. RestApiConfigBuilder
   ├─ merge_shared_context()
   │  └─ Add: raw_catalog, raw_schema
   ├─ merge_schema_overrides()
   │  └─ Add: table_name, workflow_step, depends_on
   ├─ resolve_secrets()
   │  └─ {{secrets/scope/token}} → actual_token
   ├─ pre_load_oauth2_token()
   │  └─ Exchange refresh_token → access_token
   └─ build()

4. Final Config
   {
       "endpoint": "https://...",
       "auth_type": "oauth2_refresh",
       "refresh_token": "actual_token",
       "raw_catalog": "raw",
       "raw_schema": "raw",
       "table_name": "meterdata/{from}/{to}",
       "workflow_step": 2,
       "depends_on": "metering_points"
   }
```

## Extension Points

### Adding New Schema Property

1. Add to `SCHEMA_LEVEL_PROPERTIES` in `BaseConfigBuilder`
2. Implement custom merge logic if needed
3. Property automatically extracted from schema and merged

See: [ADDING_SCHEMA_CONFIG.md](./ADDING_SCHEMA_CONFIG.md)

### Adding New Connector Type

1. Create builder class extending `BaseConfigBuilder`
2. Implement `merge_shared_context()`
3. Add properties to `CentralizedPipelineConfig` (if needed)
4. Register with `ConnectorConfigBuilderFactory`
5. Write tests

See: [ADDING_NEW_CONNECTOR.md](./ADDING_NEW_CONNECTOR.md)

## Component Interactions

```
┌──────────────────────────────────────────────────────────┐
│ CentralizedPipelineConfig                                │
│ (shared catalogs, schemas, environment)                  │
└──────────────┬───────────────────────────────────────────┘
               │
               │ used by
               ↓
┌──────────────────────────────────────────────────────────┐
│ ConnectorConfigBuilderFactory                            │
│ (creates appropriate builder)                            │
└───────────────┬────────────────────────────────────────┬─┘
                │                                        │
                │ creates                        used by │
                ↓                                        ↓
┌────────────────────────────┐    ┌──────────────────────────────┐
│ BaseConfigBuilder          │    │ ConnectorConfig              │
│ (template method pattern)  │    │ (connector type + config)    │
├────────────────────────────┤    └──────────────────────────────┘
│ - merge_schema_overrides() │              ↑
│ - resolve_secrets()        │              │ uses
│ - build()                  │              │
└───────────┬────────────────┘              │
            │
            │ extended by
            ├─────────────────────────────────────────┐
            ├─────────────────────────────────────────┐
            ├─────────────────────────────────────────┐
            ├─────────────────────────────────────────┐
            ↓
┌─────────────────────────────────────────────────────────────┐
│ Connector-Specific Builders                                 │
│ (VolumeConfigBuilder, RestApiConfigBuilder, etc.)          │
│ implement: merge_shared_context()                           │
└─────────────────────────────────────────────────────────────┘
            │
            │ uses
            ↓
┌──────────────────────────────────────────────────────────┐
│ SecretResolver                                           │
│ (resolves {{secrets/scope/key}}, {{spark.*}}, etc.)     │
└──────────────────────────────────────────────────────────┘
            │
            │ uses
            ↓
┌──────────────────────────────────────────────────────────┐
│ CatalogSchemaManager                                      │
│ (constructs catalog.schema.table paths)                  │
└──────────────────────────────────────────────────────────┘
```

## Layer Organization

The Lakehouse organizes data in layers with specific catalogs and schemas:

| Layer | Catalog | Schema | Purpose |
|-------|---------|--------|---------|
| **Landing** | landing | landing | Raw files from source systems |
| **Raw** | raw | raw | Lightly transformed raw data |
| **Base** | base | base | Heavily cleaned, deduplicated data |
| **Dimensions** | curated | dimensions | Reference dimension tables |
| **Facts** | curated | facts | Fact tables for analysis |
| **Enriched** | enriched | enriched | Business-ready enriched data |

Each layer has specific configuration that is managed centrally and used consistently across all connectors.

## Security Considerations

✅ **Do**:
- Store sensitive values as secrets
- Use `{{secrets/scope/key}}` format for secrets
- Resolve secrets only when needed
- Log resolution progress without exposing values

❌ **Don't**:
- Hardcode credentials in configuration
- Log actual secret values
- Pass secrets via command-line arguments
- Commit credentials to version control

## Performance Considerations

- **Lazy Resolution**: Secrets resolved only when builder calls `resolve_secrets()`
- **Caching**: Resolved secrets remain in configuration for reuse
- **Logging**: Debug logging available but disabled by default
- **Type-Safe**: Type hints enable early error detection

## Testability

- **Mockable**: All dependencies injectable
- **Isolated**: Each component independently testable
- **Predictable**: Deterministic behavior with given inputs
- **Comprehensive**: 45 unit tests covering all scenarios

See: `tests/unit/test_config.py`

## See Also

- [Configuration Overview](./README.md)
- [Individual Component Documentation](./README.md)
- [ADDING_SCHEMA_CONFIG.md](./ADDING_SCHEMA_CONFIG.md)
- [ADDING_NEW_CONNECTOR.md](./ADDING_NEW_CONNECTOR.md)
