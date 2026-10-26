# Connectors Documentation Index

## Quick Navigation

- **New to connectors?** → Start with [README.md](./README.md)
- **Want quick examples?** → See [QUICK_REFERENCE.md](./QUICK_REFERENCE.md)
- **Understand the design?** → Read [ARCHITECTURE.md](./ARCHITECTURE.md)
- **Find specific class?** → Use this index

## Core Components

### Base Classes

| Class | File | Purpose |
|-------|------|---------|
| **BaseConnector** | [base_connector.md](./base_connector.md) | Abstract base for all connectors |
| **ConnectorFactory** | [connector_factory.md](./connector_factory.md) | Creates connector instances |

### Concrete Connectors

| Connector | Type | File | Purpose |
|-----------|------|------|---------|
| **DataFrameConnector** | dataframe | [dataframe_connector.md](./dataframe_connector.md) | Use Spark DataFrames directly |
| **RestApiConnector** | rest_api | [datasources/rest_api_connector.md](./datasources/rest_api_connector.md) | Call REST APIs |
| **JdbcConnector** | jdbc | [datasources/jdbc_connector.md](./datasources/jdbc_connector.md) | Connect via JDBC |
| **AutoLoaderConnector** | autoloader | [datasources/autoloader_connector.md](./datasources/autoloader_connector.md) | Load cloud files |
| **RestApiDatasource** | rest_api_ds | [datasources/rest_api_datasource.md](./datasources/rest_api_datasource.md) | REST API for DLT |
| **RestApiWorkflowDatasource** | rest_api_workflow_ds | [datasources/rest_api_workflow_datasource.md](./datasources/rest_api_workflow_datasource.md) | REST API workflow for DLT |

### Support Classes

| Class | File | Purpose |
|-------|------|---------|
| **OAuth2TokenManager** | [oauth2_token_manager.md](./oauth2_token_manager.md) | Manage OAuth2 tokens |
| **PartitionStrategies** | [partition_strategies.md](./partition_strategies.md) | Partition data strategies |
| **PySparkDatasourceAdapter** | [pyspark_datasource_adapter.md](./pyspark_datasource_adapter.md) | PySpark integration |

## Guides

### Getting Started

1. [README.md](./README.md) - Overview and structure
2. [base_connector.md](./base_connector.md) - Understand base class
3. [connector_factory.md](./connector_factory.md) - Creating connectors
4. [dataframe_connector.md](./dataframe_connector.md) - Simple example

### Learning Paths

**Path 1: Using Existing Connectors**
1. [README.md](./README.md)
2. [QUICK_REFERENCE.md](./QUICK_REFERENCE.md)
3. Specific connector docs (rest_api, jdbc, etc.)

**Path 2: Understanding Design**
1. [README.md](./README.md)
2. [ARCHITECTURE.md](./ARCHITECTURE.md)
3. [base_connector.md](./base_connector.md)
4. [connector_factory.md](./connector_factory.md)

**Path 3: Advanced Usage**
1. [oauth2_token_manager.md](./oauth2_token_manager.md)
2. [partition_strategies.md](./partition_strategies.md)
3. [pyspark_datasource_adapter.md](./pyspark_datasource_adapter.md)

**Path 4: Creating Custom Connector**
1. [README.md](./README.md)
2. [base_connector.md](./base_connector.md)
3. [ARCHITECTURE.md](./ARCHITECTURE.md)
4. Specific connector implementation (rest_api_connector.md)

## Reference By Topic

### Authentication

- [oauth2_token_manager.md](./oauth2_token_manager.md) - OAuth2 management
- [QUICK_REFERENCE.md](./QUICK_REFERENCE.md#oauth2-authentication) - Quick examples

### Data Loading

- [base_connector.md](./base_connector.md#load) - Load method documentation
- [QUICK_REFERENCE.md](./QUICK_REFERENCE.md#loading-data) - Quick examples

### Partitioning

- [partition_strategies.md](./partition_strategies.md) - Available strategies
- [QUICK_REFERENCE.md](./QUICK_REFERENCE.md#partitioning-strategies) - Quick examples

### PySpark Integration

- [pyspark_datasource_adapter.md](./pyspark_datasource_adapter.md) - Adapter documentation
- [QUICK_REFERENCE.md](./QUICK_REFERENCE.md#pyspark-integration) - Quick examples

### Error Handling

- [QUICK_REFERENCE.md](./QUICK_REFERENCE.md#error-handling) - Exception types and handling
- [ARCHITECTURE.md](./ARCHITECTURE.md#error-handling-architecture) - Error hierarchy

### Configuration

- [QUICK_REFERENCE.md](./QUICK_REFERENCE.md#configuration-examples) - Config examples
- [../configuration/README.md](../configuration/README.md) - Configuration system

### Testing

- [QUICK_REFERENCE.md](./QUICK_REFERENCE.md#testing-connectors) - Test patterns
- [base_connector.md](./base_connector.md#testing) - Base class testing

## Datasources Documentation

### DLT Datasources

| Datasource | File | Purpose |
|-----------|------|---------|
| **RestApiDatasource** | [datasources/rest_api_datasource.md](./datasources/rest_api_datasource.md) | REST API for Delta Live Tables |
| **RestApiWorkflowDatasource** | [datasources/rest_api_workflow_datasource.md](./datasources/rest_api_workflow_datasource.md) | REST API workflow variant |

See: [datasources/README.md](./datasources/README.md)

## Related Documentation

### Configuration System

- [../configuration/README.md](../configuration/README.md) - Configuration overview
- [../configuration/connector_config.md](../configuration/connector_config.md) - ConnectorConfig class
- [../configuration/builders/](../configuration/builders/) - Configuration builders

### Main Documentation

- [../README.md](../README.md) - Main documentation index
- [../CONNECTOR_FRAMEWORK.md](../CONNECTOR_FRAMEWORK.md) - Framework overview
- [../CONNECTOR_QUICKSTART.md](../CONNECTOR_QUICKSTART.md) - Getting started

## Search By Use Case

### Use Case: Load data from REST API

1. [QUICK_REFERENCE.md](./QUICK_REFERENCE.md#rest-api-connector) - Quick example
2. [datasources/rest_api_connector.md](./datasources/rest_api_connector.md) - Full documentation
3. [oauth2_token_manager.md](./oauth2_token_manager.md) - For authentication
4. [partition_strategies.md](./partition_strategies.md) - For large datasets

### Use Case: Load data from database via JDBC

1. [QUICK_REFERENCE.md](./QUICK_REFERENCE.md#jdbc-connector) - Quick example
2. [datasources/jdbc_connector.md](./datasources/jdbc_connector.md) - Full documentation
3. [partition_strategies.md](./partition_strategies.md) - For parallel loading

### Use Case: Load files using AutoLoader

1. [QUICK_REFERENCE.md](./QUICK_REFERENCE.md#autoloader-connector) - Quick example
2. [datasources/autoloader_connector.md](./datasources/autoloader_connector.md) - Full documentation

### Use Case: Use connector in Delta Live Tables

1. [QUICK_REFERENCE.md](./QUICK_REFERENCE.md#delta-live-tables) - Quick example
2. [pyspark_datasource_adapter.md](./pyspark_datasource_adapter.md) - Adapter documentation
3. [datasources/rest_api_datasource.md](./datasources/rest_api_datasource.md) - DLT connector

### Use Case: Create custom connector

1. [README.md](./README.md#extension-points) - Overview
2. [base_connector.md](./base_connector.md) - Base class interface
3. [ARCHITECTURE.md](./ARCHITECTURE.md#extensibility-points) - Extension guide
4. [connector_factory.md](./connector_factory.md) - Factory registration

### Use Case: Test connector code

1. [QUICK_REFERENCE.md](./QUICK_REFERENCE.md#testing-connectors) - Test patterns
2. [base_connector.md](./base_connector.md#testing) - Base class testing
3. [dataframe_connector.md](./dataframe_connector.md#testing) - Example tests

### Use Case: Handle authentication

1. [oauth2_token_manager.md](./oauth2_token_manager.md) - Token management
2. [QUICK_REFERENCE.md](./QUICK_REFERENCE.md#oauth2-authentication) - Quick examples
3. [../configuration/secret_resolver.md](../configuration/secret_resolver.md) - Credential storage

### Use Case: Optimize large data loads

1. [partition_strategies.md](./partition_strategies.md) - Partitioning strategies
2. [QUICK_REFERENCE.md](./QUICK_REFERENCE.md#performance-tips) - Performance tips
3. [ARCHITECTURE.md](./ARCHITECTURE.md#performance-optimization) - Optimization details

## File Structure

```
documentation/connectors/
│
├── README.md                    # Overview and getting started
├── ARCHITECTURE.md              # Design patterns and architecture
├── QUICK_REFERENCE.md          # Quick examples and code snippets
├── INDEX.md                    # This file
│
├── base_connector.md           # BaseConnector abstract class
├── connector_factory.md        # ConnectorFactory (factory pattern)
├── dataframe_connector.md      # DataFrameConnector implementation
│
├── oauth2_token_manager.md     # OAuth2 token management
├── partition_strategies.md     # Partitioning strategies
├── pyspark_datasource_adapter.md  # PySpark integration adapter
│
└── datasources/
    ├── README.md              # Datasources overview
    ├── rest_api_connector.md  # REST API connector
    ├── rest_api_datasource.md # REST API for DLT
    ├── rest_api_workflow_datasource.md  # REST API workflow variant
    ├── jdbc_connector.md      # JDBC connector
    └── autoloader_connector.md # AutoLoader connector
```

## Navigation Tips

### For Quick Answers

Use [QUICK_REFERENCE.md](./QUICK_REFERENCE.md):
- Copy-paste code examples
- Common configuration patterns
- Performance tips

### For Deep Understanding

Read [ARCHITECTURE.md](./ARCHITECTURE.md):
- Design patterns explained
- Data flow diagrams
- Extensibility guide

### For Implementation Details

Check specific class files:
- [base_connector.md](./base_connector.md) - Methods and contracts
- [connector_factory.md](./connector_factory.md) - Creation logic
- Datasource files - Implementation specifics

### For Troubleshooting

Try:
1. [QUICK_REFERENCE.md](./QUICK_REFERENCE.md#error-handling) - Common errors
2. [ARCHITECTURE.md](./ARCHITECTURE.md#error-handling-architecture) - Error hierarchy
3. Specific connector docs - Known issues

## Cross-References

### Configuration System

- [../configuration/README.md](../configuration/README.md) - Configuration overview
- [../configuration/connector_config.md](../configuration/connector_config.md) - ConnectorConfig
- [../configuration/builders/builder_factory.md](../configuration/builders/builder_factory.md) - Builder factory

### Related Classes

- [../configuration/centralized_config.md](../configuration/centralized_config.md) - Shared configuration
- [../configuration/catalog_schema_manager.md](../configuration/catalog_schema_manager.md) - Schema management
- [../configuration/secret_resolver.md](../configuration/secret_resolver.md) - Secret handling

## Last Updated

This index was created as part of connector system documentation restructuring to match configuration documentation structure.

## See Also

- [README.md](./README.md) - Main entry point
- [QUICK_REFERENCE.md](./QUICK_REFERENCE.md) - Quick code examples
- [ARCHITECTURE.md](./ARCHITECTURE.md) - Design patterns
