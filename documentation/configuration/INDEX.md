# Configuration Documentation Index

## Welcome

This directory contains comprehensive documentation for the Lakehouse configuration system, organized to match the code structure. Start with [README.md](./README.md) or [QUICK_REFERENCE.md](./QUICK_REFERENCE.md).

## 📚 Documentation Organization

### Core Reference (Start Here!)
1. **[README.md](./README.md)** - System overview and structure
2. **[QUICK_REFERENCE.md](./QUICK_REFERENCE.md)** - Quick lookup guide (bookmark this!)
3. **[ARCHITECTURE.md](./ARCHITECTURE.md)** - Design patterns and data flows

### Core Classes (Match src/framework/config/)
- **[centralized_config.md](./centralized_config.md)** - Shared pipeline metadata
- **[connector_config.md](./connector_config.md)** - Connector-specific wrapper
- **[catalog_schema_manager.md](./catalog_schema_manager.md)** - Table path construction
- **[secret_resolver.md](./secret_resolver.md)** - Secret reference resolution

### Builders (Match src/framework/config/builders/)
- **[builders/README.md](./builders/README.md)** - Builders overview
- **[builders/base_config_builder.md](./builders/base_config_builder.md)** - Abstract template base
- **[builders/volume_config_builder.md](./builders/volume_config_builder.md)** - Volume connector
- **[builders/rest_api_config_builder.md](./builders/rest_api_config_builder.md)** - REST API connector
- **[builders/jdbc_config_builder.md](./builders/jdbc_config_builder.md)** - JDBC connector
- **[builders/autoloader_config_builder.md](./builders/autoloader_config_builder.md)** - AutoLoader connector
- **[builders/builder_factory.md](./builders/builder_factory.md)** - Factory pattern

### How-To Guides
- **[../ADDING_SCHEMA_CONFIG.md](../ADDING_SCHEMA_CONFIG.md)** - Add schema properties
- **[../ADDING_NEW_CONNECTOR.md](../ADDING_NEW_CONNECTOR.md)** - Add new connector types

## 🎯 Quick Navigation

### By Task
| I want to... | Read this |
|---|---|
| Understand the system | [README.md](./README.md) |
| Get quick answers | [QUICK_REFERENCE.md](./QUICK_REFERENCE.md) |
| Learn design patterns | [ARCHITECTURE.md](./ARCHITECTURE.md) |
| Add a schema property | [../ADDING_SCHEMA_CONFIG.md](../ADDING_SCHEMA_CONFIG.md) |
| Add a new connector | [../ADDING_NEW_CONNECTOR.md](../ADDING_NEW_CONNECTOR.md) |
| Understand a specific class | See Core Classes list |
| Understand builders | [builders/README.md](./builders/README.md) |

### By Role
| I'm a... | Start with |
|---|---|
| **New Developer** | [README.md](./README.md) → [QUICK_REFERENCE.md](./QUICK_REFERENCE.md) |
| **Backend Developer** | [QUICK_REFERENCE.md](./QUICK_REFERENCE.md) → Task-specific guide |
| **Architect** | [ARCHITECTURE.md](./ARCHITECTURE.md) |
| **QA/Tester** | [builders/README.md](./builders/README.md) & test files |

## 📂 Directory Structure

```
documentation/configuration/
│
├─ README.md                              # Overview (START HERE)
├─ QUICK_REFERENCE.md                     # Quick lookup (BOOKMARK THIS)
├─ ARCHITECTURE.md                        # Design & patterns
│
├─ Core Classes (match src/framework/config/)
├─ centralized_config.md                  # CentralizedPipelineConfig
├─ connector_config.md                    # ConnectorConfig
├─ catalog_schema_manager.md              # CatalogSchemaManager
├─ secret_resolver.md                     # SecretResolver
│
├─ builders/                              # (match src/framework/config/builders/)
│  ├─ README.md                           # Builders overview
│  ├─ base_config_builder.md              # BaseConfigBuilder
│  ├─ volume_config_builder.md            # VolumeConfigBuilder
│  ├─ rest_api_config_builder.md          # RestApiConfigBuilder
│  ├─ jdbc_config_builder.md              # JdbcConfigBuilder
│  ├─ autoloader_config_builder.md        # AutoLoaderConfigBuilder
│  └─ builder_factory.md                  # ConnectorConfigBuilderFactory
│
└─ ../ADDING_SCHEMA_CONFIG.md             # How to add schema properties
   ../ADDING_NEW_CONNECTOR.md             # How to add connectors
```

## 🚀 Quick Start

### 1. Understand the System (15 minutes)
```
1. Read: README.md (System overview)
2. Scan: QUICK_REFERENCE.md (At a glance)
3. Review: One code example for your connector
```

### 2. Learn Core Concepts (30 minutes)
```
1. Read: ARCHITECTURE.md (Design patterns)
2. Review: Relevant core class documentation
3. Examine: builders/README.md (Builder pattern)
```

### 3. Do Your Task (varies)
```
Adding property?
  → ADDING_SCHEMA_CONFIG.md

Adding connector?
  → ADDING_NEW_CONNECTOR.md

Need details?
  → Refer to specific class documentation
```

## 📊 Test Coverage

**Total Tests**: 45 (All Passing ✅)

```
- ConnectorConfig .......................... 9 tests
- CentralizedPipelineConfig ............... 4 tests
- CatalogSchemaManager .................... 6 tests
- SecretResolver .......................... 5 tests
- VolumeConfigBuilder ..................... 3 tests
- RestApiConfigBuilder .................... 4 tests
- ConnectorConfigBuilderFactory ........... 10 tests
- BaseConfigBuilder ....................... 4 tests
- Integration ............................. 2 tests
```

See: `tests/unit/test_config.py`

## 🔗 Cross-References

Each documentation file includes:
- **Purpose** - Why the class/component exists
- **Usage Examples** - Real code examples
- **Related Classes** - Links to related documentation
- **See Also** - Additional resources

## 📝 Document Statistics

| Document | Type | Lines | Examples |
|----------|------|-------|----------|
| README.md | Overview | 200 | 5 |
| QUICK_REFERENCE.md | Reference | 200 | 15 |
| ARCHITECTURE.md | Guide | 400 | 10 |
| Core Classes (4) | Reference | 1200+ | 20+ |
| Builders (6) | Reference | 1500+ | 25+ |
| TOTAL | — | ~3500+ | ~70+ |

## ❓ FAQ

**Q: Where should I start?**
A: Read [README.md](./README.md), then bookmark [QUICK_REFERENCE.md](./QUICK_REFERENCE.md)

**Q: How do I find a specific class?**
A: Check the "Core Classes" list or see [README.md](./README.md#core-components)

**Q: Where are code examples?**
A: Every class documentation has "Usage Examples" section

**Q: How do I add a new feature?**
A: See [../ADDING_SCHEMA_CONFIG.md](../ADDING_SCHEMA_CONFIG.md) or [../ADDING_NEW_CONNECTOR.md](../ADDING_NEW_CONNECTOR.md)

**Q: How do I run the tests?**
A: See "Testing" section in [QUICK_REFERENCE.md](./QUICK_REFERENCE.md)

## 🔧 Maintenance

This documentation is structured to match the code organization in `src/framework/config/`:

- Each file has corresponding documentation
- Each builder has its own page
- Updates to code should reflect in docs
- Add new builders/classes → Add documentation

## 📞 Need Help?

1. **Check [QUICK_REFERENCE.md](./QUICK_REFERENCE.md)** - Most common questions
2. **Check [README.md](./README.md)** - System overview
3. **Check [ARCHITECTURE.md](./ARCHITECTURE.md)** - Design patterns
4. **See [builders/README.md](./builders/README.md)** - Builder questions
5. **Review test file** - See `tests/unit/test_config.py` for examples

---

**Last Updated**: 2025-12-19  
**Configuration System Version**: 2.0  
**Test Status**: 45/45 Passing ✅
