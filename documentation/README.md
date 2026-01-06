# Documentation

Welcome to the Lakehouse Demo Project documentation. This guide helps you navigate all documentation resources.

## 📚 Main Documentation Sections

### 1. **Configuration System**
Complete documentation for the modular configuration system.

- **[README](./configuration/README.md)** - Configuration system overview
- **[QUICK_REFERENCE](./configuration/QUICK_REFERENCE.md)** - Quick examples and code snippets
- **[ARCHITECTURE](./configuration/ARCHITECTURE.md)** - Design patterns and architecture
- **[INDEX](./configuration/INDEX.md)** - Complete navigation guide

**Individual Classes:**
- [CentralizedPipelineConfig](./configuration/centralized_config.md)
- [ConnectorConfig](./configuration/connector_config.md)
- [CatalogSchemaManager](./configuration/catalog_schema_manager.md)
- [SecretResolver](./configuration/secret_resolver.md)

**Builders:**
- [BaseConfigBuilder](./configuration/builders/base_config_builder.md)
- [VolumeConfigBuilder](./configuration/builders/volume_config_builder.md)
- [RestApiConfigBuilder](./configuration/builders/rest_api_config_builder.md)
- [JdbcConfigBuilder](./configuration/builders/jdbc_config_builder.md)
- [AutoLoaderConfigBuilder](./configuration/builders/autoloader_config_builder.md)
- [ConnectorConfigBuilderFactory](./configuration/builders/builder_factory.md)

### 2. **Layer Factories**
Complete documentation for pipeline factories across all data layers.

- **[README](./factory/README.md)** - Factory pattern overview and architecture
- **[QUICK_REFERENCE](./factory/QUICK_REFERENCE.md)** - Quick API reference and examples
- **[INDEX](./factory/INDEX.md)** - Complete navigation guide

**Layer Factories:**
- [RawPipelineFactory](./factory/raw_factory.md) - Raw layer ingestion
- [BasePipelineFactory](./factory/base_factory.md) - CDC and deduplication
- [CuratedDimensionFactory](./factory/dimension_factory.md) - Dimension tables
- [CuratedFactFactory](./factory/fact_factory.md) - Fact tables

### 3. **Connector Framework**
Comprehensive documentation for data source connectors.

- **[README](./connectors/README.md)** - Connector system overview
- **[QUICK_REFERENCE](./connectors/QUICK_REFERENCE.md)** - Quick examples and code snippets
- **[ARCHITECTURE](./connectors/ARCHITECTURE.md)** - Design patterns and architecture
- **[INDEX](./connectors/INDEX.md)** - Complete navigation guide

**Core Classes:**
- [BaseConnector](./connectors/base_connector.md) - Abstract base class
- [ConnectorFactory](./connectors/connector_factory.md) - Factory pattern
- [DataFrameConnector](./connectors/dataframe_connector.md) - Spark DataFrame connector

**Support Classes:**
- [OAuth2TokenManager](./connectors/oauth2_token_manager.md) - Token management
- [PartitionStrategies](./connectors/partition_strategies.md) - Partitioning strategies
- [PySparkDatasourceAdapter](./connectors/pyspark_datasource_adapter.md) - PySpark integration

**Datasources:**
- [REST API Connector](./connectors/datasources/rest_api_connector.md)
- [REST API for DLT](./connectors/datasources/rest_api_datasource.md)
- [REST API Workflow](./connectors/datasources/rest_api_workflow_datasource.md)
- [JDBC Connector](./connectors/datasources/jdbc_connector.md)
- [AutoLoader Connector](./connectors/datasources/autoloader_connector.md)

### 4. **Testing**
Information about BDD acceptance tests.

- **[features/](./features/)** - BDD feature files and step definitions

## 🚀 Getting Started

### First Time Users

1. **Configuration System**: Start with [configuration/README.md](./configuration/README.md)
2. **Connectors**: Continue with [connectors/README.md](./connectors/README.md)
3. **Quick Examples**: See [QUICK_REFERENCE guides](#quick-reference-guides) below

### Developers Adding Features

1. Read relevant [ARCHITECTURE guides](#architecture-guides)
2. Check [QUICK_REFERENCE guides](#quick-reference-guides) for examples
3. Use [INDEX guides](#index-guides) to find specific classes
4. Follow patterns in existing code

### DevOps/Deployment

- Configuration deployment: See [configuration/README.md#deployment](./configuration/README.md)
- Secret management: See [configuration/secret_resolver.md](./configuration/secret_resolver.md)

## 📖 Documentation by Topic

### Configuration System

| Topic | Location |
|-------|----------|
| Overview | [configuration/README.md](./configuration/README.md) |
| Quick examples | [configuration/QUICK_REFERENCE.md](./configuration/QUICK_REFERENCE.md) |
| Design patterns | [configuration/ARCHITECTURE.md](./configuration/ARCHITECTURE.md) |
| Navigation | [configuration/INDEX.md](./configuration/INDEX.md) |
| All classes | [configuration/](./configuration/) |

### Layer Factories

| Topic | Location |
|-------|----------|
| Overview | [factory/README.md](./factory/README.md) |
| Quick API reference | [factory/QUICK_REFERENCE.md](./factory/QUICK_REFERENCE.md) |
| Navigation | [factory/INDEX.md](./factory/INDEX.md) |
| Raw layer | [factory/raw_factory.md](./factory/raw_factory.md) |
| Base layer (CDC) | [factory/base_factory.md](./factory/base_factory.md) |
| Dimensions | [factory/dimension_factory.md](./factory/dimension_factory.md) |
| Facts | [factory/fact_factory.md](./factory/fact_factory.md) |

### Connector System

| Topic | Location |
|-------|----------|
| Overview | [connectors/README.md](./connectors/README.md) |
| Quick examples | [connectors/QUICK_REFERENCE.md](./connectors/QUICK_REFERENCE.md) |
| Design patterns | [connectors/ARCHITECTURE.md](./connectors/ARCHITECTURE.md) |
| Navigation | [connectors/INDEX.md](./connectors/INDEX.md) |
| All classes | [connectors/](./connectors/) |

### Testing

| Topic | Location |
|-------|----------|
| BDD features | [features/](./features/) |
| Step definitions | [features/steps/](./features/steps/) |

## 🎯 Common Tasks

### Task: Create a data pipeline layer

1. Read [factory/README.md](./factory/README.md) - Understand factory pattern
2. Choose layer: [factory/raw_factory.md](./factory/raw_factory.md), [factory/base_factory.md](./factory/base_factory.md), etc.
3. See [factory/QUICK_REFERENCE.md](./factory/QUICK_REFERENCE.md) for code examples
4. Create data contract YAML for raw layer

### Task: Ingest data from external source

1. Read [factory/raw_factory.md](./factory/raw_factory.md)
2. Check [connectors/README.md](./connectors/README.md) for connector types
3. Configure connector in data contract
4. Use [factory/QUICK_REFERENCE.md#raw-factory-api](./factory/QUICK_REFERENCE.md#raw-factory-api)

### Task: Configure a new data source

1. Read [connectors/README.md](./connectors/README.md)
2. Check [connectors/QUICK_REFERENCE.md#configuration-examples](./connectors/QUICK_REFERENCE.md#configuration-examples)
3. Use builder from [configuration/builders/](./configuration/builders/)

### Task: Track data changes (CDC)

1. Read [factory/base_factory.md](./factory/base_factory.md) - CDC overview
2. See [factory/base_factory.md#scd-type-2-transformation](./factory/base_factory.md#scd-type-2-transformation)
3. Use [factory/QUICK_REFERENCE.md#base-factory-api](./factory/QUICK_REFERENCE.md#base-factory-api)

### Task: Create star schema

1. Read [factory/README.md#curated-layer](./factory/README.md#curated-layer)
2. Create dimensions: [factory/dimension_factory.md](./factory/dimension_factory.md)
3. Create facts: [factory/fact_factory.md](./factory/fact_factory.md)
4. See [factory/QUICK_REFERENCE.md](./factory/QUICK_REFERENCE.md) for examples

### Task: Add a new connector type

1. Start with [connectors/ARCHITECTURE.md#extensibility-points](./connectors/ARCHITECTURE.md#extensibility-points)
2. Review [connectors/base_connector.md](./connectors/base_connector.md)
3. Check example: [connectors/datasources/rest_api_connector.md](./connectors/datasources/rest_api_connector.md)

### Task: Add a schema property

1. Read [configuration/README.md](./configuration/README.md)
2. See [configuration/centralized_config.md](./configuration/centralized_config.md)
3. Check [configuration/builders/](./configuration/builders/) for builder pattern

### Task: Handle authentication

1. See [connectors/oauth2_token_manager.md](./connectors/oauth2_token_manager.md)
2. Check [configuration/secret_resolver.md](./configuration/secret_resolver.md)
3. Examples in [connectors/QUICK_REFERENCE.md#oauth2-authentication](./connectors/QUICK_REFERENCE.md#oauth2-authentication)

### Task: Optimize performance

1. Read [connectors/partition_strategies.md](./connectors/partition_strategies.md)
2. Check [connectors/ARCHITECTURE.md#performance-optimization](./connectors/ARCHITECTURE.md#performance-optimization)
3. For factories, see [factory/QUICK_REFERENCE.md#performance-tips](./factory/QUICK_REFERENCE.md#performance-tips)
3. See [connectors/QUICK_REFERENCE.md#performance-tips](./connectors/QUICK_REFERENCE.md#performance-tips)

## 🔍 Quick Reference Guides

- [Configuration Quick Reference](./configuration/QUICK_REFERENCE.md)
- [Connectors Quick Reference](./connectors/QUICK_REFERENCE.md)

## 🏗️ Architecture Guides

- [Configuration Architecture](./configuration/ARCHITECTURE.md)
- [Connectors Architecture](./connectors/ARCHITECTURE.md)

## 📑 Index Guides

- [Configuration Index](./configuration/INDEX.md)
- [Connectors Index](./connectors/INDEX.md)

## 📂 Directory Structure

```
documentation/
├── README.md (this file)
│
├── configuration/
│   ├── README.md
│   ├── QUICK_REFERENCE.md
│   ├── ARCHITECTURE.md
│   ├── INDEX.md
│   ├── centralized_config.md
│   ├── connector_config.md
│   ├── catalog_schema_manager.md
│   ├── secret_resolver.md
│   └── builders/
│       ├── README.md
│       ├── base_config_builder.md
│       ├── volume_config_builder.md
│       ├── rest_api_config_builder.md
│       ├── jdbc_config_builder.md
│       ├── autoloader_config_builder.md
│       └── builder_factory.md
│
├── connectors/
│   ├── README.md
│   ├── QUICK_REFERENCE.md
│   ├── ARCHITECTURE.md
│   ├── INDEX.md
│   ├── base_connector.md
│   ├── connector_factory.md
│   ├── dataframe_connector.md
│   ├── oauth2_token_manager.md
│   ├── partition_strategies.md
│   ├── pyspark_datasource_adapter.md
│   └── datasources/
│       ├── README.md
│       ├── rest_api_connector.md
│       ├── rest_api_datasource.md
│       ├── rest_api_workflow_datasource.md
│       ├── jdbc_connector.md
│       └── autoloader_connector.md
│
└── features/
    ├── ingest.feature
    └── steps/
        └── ingest_steps.py
```

## 🔄 Related Documentation

### In Source Code

- Framework code: `src/framework/`
- Configuration classes: `src/framework/config/`
- Connectors: `src/framework/connectors/`
- Tests: `tests/`

### External Resources

- [Delta Live Tables Documentation](https://docs.databricks.com/en/delta-live-tables/)
- [PySpark Documentation](https://spark.apache.org/docs/latest/api/python/)
- [Databricks Documentation](https://docs.databricks.com/)

## 💡 Tips for Using This Documentation

### Finding Information

1. **Looking for quick code examples?** → Use [QUICK_REFERENCE guides](#quick-reference-guides)
2. **Want to understand design?** → Read [ARCHITECTURE guides](#architecture-guides)
3. **Need to find a specific class?** → Check [INDEX guides](#index-guides)
4. **Searching by topic?** → See [Common Tasks](#common-tasks)

### Navigation

- Each file has cross-references to related files
- Use the navigation menus in README files
- Follow breadcrumb "See Also" sections
- Check "Related Classes" for dependencies

### Keeping Current

Documentation is organized to match code structure, making it easy to:
- Find relevant docs when reading code
- Update docs when changing code
- Add docs when adding features

## ❓ Troubleshooting

**Can't find what you're looking for?**
- Try the [INDEX guides](#index-guides) - they have search by use case
- Check related "See Also" sections
- Look in the [Common Tasks](#common-tasks) section

**Documentation seems out of date?**
- Check the code in `src/` to verify
- File an issue or update the docs
- Documentation structure matches code structure

## 📝 Version Information

- **Last Updated**: December 2025
- **Documentation Structure**: Matches code organization
- **Configuration System**: Modular pattern (centralized + connector-specific)
- **Connector System**: Factory + Template Method + Strategy patterns

---

**Start exploring:** [Configuration System](./configuration/README.md) or [Connector Framework](./connectors/README.md)
