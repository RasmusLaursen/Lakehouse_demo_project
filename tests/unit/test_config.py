"""
Unit tests for configuration classes and builders.

Tests cover:
1. CentralizedPipelineConfig - shared metadata across layers
2. ConnectorConfig - connector-specific configuration wrapper
3. CatalogSchemaManager - table path construction
4. SecretResolver - secret reference resolution
5. BaseConfigBuilder - abstract builder with template methods
6. Connector-specific builders (Volume, RestApi, JDBC, AutoLoader)
7. ConnectorConfigBuilderFactory - builder instantiation
"""

import pytest
import unittest
from unittest.mock import Mock, MagicMock, patch
import json

from src.framework.config import (
    CentralizedPipelineConfig,
    ConnectorConfig,
    CatalogSchemaManager,
    SecretResolver,
    ConnectorConfigBuilderFactory,
)
from src.framework.config.builders import (
    BaseConfigBuilder,
    VolumeConfigBuilder,
    RestApiConfigBuilder,
    JdbcConfigBuilder,
    AutoLoaderConfigBuilder,
)


class TestConnectorConfig(unittest.TestCase):
    """Test ConnectorConfig wrapper class."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.config_dict = {
            "endpoint": "https://api.example.com",
            "auth_type": "bearer",
            "auth_token": "test_token_123",
            "params": {"key": "value"},
            "secret_keys": ["auth_token"]
        }
        self.connector_config = ConnectorConfig("rest_api", self.config_dict)
    
    def test_init_with_dict(self):
        """Test ConnectorConfig initialization with dictionary."""
        assert self.connector_config.connector_type == "rest_api"
        assert self.connector_config.get("endpoint") == "https://api.example.com"
    
    def test_init_without_dict(self):
        """Test ConnectorConfig initialization without dictionary."""
        config = ConnectorConfig("volume")
        assert config.connector_type == "volume"
        assert config.to_dict() == {}
    
    def test_get_value(self):
        """Test getting configuration values."""
        assert self.connector_config.get("endpoint") == "https://api.example.com"
        assert self.connector_config.get("non_existent") is None
        assert self.connector_config.get("non_existent", "default") == "default"
    
    def test_set_value(self):
        """Test setting configuration values."""
        self.connector_config.set("new_key", "new_value")
        assert self.connector_config.get("new_key") == "new_value"
    
    def test_merge_configs(self):
        """Test merging configuration dictionaries."""
        new_config = self.connector_config.merge({
            "endpoint": "https://api.new.com",
            "new_param": "param_value"
        })
        
        assert new_config.connector_type == "rest_api"
        assert new_config.get("endpoint") == "https://api.new.com"
        assert new_config.get("new_param") == "param_value"
        # Original should be unchanged
        assert self.connector_config.get("endpoint") == "https://api.example.com"
    
    def test_extract_secrets(self):
        """Test extracting secret keys."""
        secret_keys = self.connector_config.extract_secrets()
        assert secret_keys == ["auth_token"]
        assert "secret_keys" not in self.connector_config.to_dict()
    
    def test_to_dict(self):
        """Test exporting configuration as dictionary."""
        config_dict = self.connector_config.to_dict()
        assert config_dict["endpoint"] == "https://api.example.com"
        assert config_dict["auth_type"] == "bearer"
    
    def test_source_type_property(self):
        """Test source_type property."""
        assert self.connector_config.source_type == "volume"  # default
        
        config_with_source = ConnectorConfig("rest_api", {"source_type": "api"})
        assert config_with_source.source_type == "api"
    
    def test_from_server_config(self):
        """Test creating ConnectorConfig from server configuration."""
        server_config = Mock()
        server_config.customProperties = [
            Mock(property="connector_type", value="rest_api"),
            Mock(property="connector_config", value={"endpoint": "https://api.example.com"}),
            Mock(property="secret_keys", value=["api_key"])
        ]
        
        connector_config = ConnectorConfig.from_server_config(server_config)
        
        assert connector_config.connector_type == "rest_api"
        assert connector_config.get("endpoint") == "https://api.example.com"
        assert connector_config.get("secret_keys") == ["api_key"]


class TestCentralizedPipelineConfig(unittest.TestCase):
    """Test CentralizedPipelineConfig class."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.config = CentralizedPipelineConfig(
            source_system_name="lakehouse",
            environment="dev",
            landing_catalog="landing",
            raw_catalog="raw",
            base_catalog="base",
            curated_catalog="curated",
            enriched_catalog="enriched",
            landing_schema="lakehouse_landing",
            raw_schema="lakehouse_raw",
            base_schema="lakehouse_base",
            dimensions_schema="dimensions",
            facts_schema="facts",
            enriched_schema="enriched"
        )
    
    def test_initialization(self):
        """Test CentralizedPipelineConfig initialization."""
        assert self.config.source_system_name == "lakehouse"
        assert self.config.environment == "dev"
        assert self.config.base_catalog == "base"
        assert self.config.dimensions_schema == "dimensions"
    
    def test_validate_success(self):
        """Test validation succeeds with complete config."""
        assert self.config.validate() is True
    
    def test_validate_fails_missing_catalog(self):
        """Test validation fails with missing required catalog."""
        invalid_config = CentralizedPipelineConfig(
            source_system_name="lakehouse",
            environment="dev",
            landing_catalog="landing",
            raw_catalog="raw",
            base_catalog=None,  # Missing
            curated_catalog="curated",
            enriched_catalog="enriched",
            landing_schema="lakehouse_landing",
            raw_schema="lakehouse_raw",
            base_schema="lakehouse_base",
            dimensions_schema="dimensions",
            facts_schema="facts",
            enriched_schema="enriched"
        )
        
        with pytest.raises(ValueError) as exc_info:
            invalid_config.validate()
        assert "base_catalog" in str(exc_info.value)
    
    def test_from_spark(self):
        """Test creating from Spark configuration."""
        spark = Mock()
        spark.conf.get.side_effect = lambda key, default=None: {
            "environment": "dev"
        }.get(key, default)
        
        with patch("src.framework.helper.get_pipeline_configurations") as mock_get_config:
            mock_get_config.side_effect = [
                {  # catalogs
                    "landing_catalog": "landing",
                    "raw_catalog": "raw",
                    "base_catalog": "base",
                    "curated_catalog": "curated",
                    "enriched_catalog": "enriched"
                },
                {  # schemas
                    "lakehouse_landing_schema": "lakehouse_landing",
                    "lakehouse_raw_schema": "lakehouse_raw",
                    "lakehouse_base_schema": "lakehouse_base",
                    "dimensions_schema": "dimensions",
                    "facts_schema": "facts",
                    "enriched_schema": "enriched"
                }
            ]
            
            config = CentralizedPipelineConfig.from_spark(spark, "lakehouse")
            
            assert config.source_system_name == "lakehouse"
            assert config.landing_catalog == "landing"


class TestCatalogSchemaManager(unittest.TestCase):
    """Test CatalogSchemaManager class."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.manager = CatalogSchemaManager(
            landing_catalog="landing",
            raw_catalog="raw",
            base_catalog="base",
            curated_catalog="curated",
            enriched_catalog="enriched",
            landing_schema="landing",
            raw_schema="raw",
            base_schema="lakehouse_base",
            dimensions_schema="dimensions",
            facts_schema="facts",
            enriched_schema="enriched"
        )
    
    def test_get_table_path(self):
        """Test getting fully qualified table path."""
        path = self.manager.get_table_path("catalog", "schema", "table")
        assert path == "catalog.schema.table"
    
    def test_get_base_table_path(self):
        """Test getting base layer table path."""
        path = self.manager.get_base_table_path("customer")
        assert path == "base.lakehouse_base.customer"
    
    def test_get_dimension_table_path(self):
        """Test getting dimension table path."""
        path = self.manager.get_dimension_table_path("dim_customer")
        assert path == "curated.dimensions.dim_customer"
    
    def test_get_fact_table_path(self):
        """Test getting fact table path."""
        path = self.manager.get_fact_table_path("fact_sales")
        assert path == "curated.facts.fact_sales"
    
    def test_get_raw_table_path(self):
        """Test getting raw layer table path."""
        path = self.manager.get_raw_table_path("orders")
        assert path == "raw.raw.orders"
    
    def test_from_pipeline_config(self):
        """Test creating from CentralizedPipelineConfig."""
        config = CentralizedPipelineConfig(
            source_system_name="test",
            environment="dev",
            landing_catalog="landing",
            raw_catalog="raw",
            base_catalog="base",
            curated_catalog="curated",
            enriched_catalog="enriched",
            landing_schema="landing",
            raw_schema="raw",
            base_schema="base",
            dimensions_schema="dimensions",
            facts_schema="facts",
            enriched_schema="enriched"
        )
        
        manager = CatalogSchemaManager.from_pipeline_config(config)
        
        assert manager.base_catalog == "base"
        assert manager.dimensions_schema == "dimensions"


class TestSecretResolver(unittest.TestCase):
    """Test SecretResolver class."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.resolver = SecretResolver()
    
    def test_resolve_plain_string(self):
        """Test resolving plain string (not a secret reference)."""
        result = self.resolver.resolve("plain_value")
        assert result == "plain_value"
    
    def test_resolve_spark_config_format(self):
        """Test resolving {{spark.config-key}} format."""
        spark = Mock()
        spark.conf.get.return_value = "secret_value_123"
        
        with patch("src.framework.helper.get_spark", return_value=spark):
            result = self.resolver.resolve("{{spark.api-token}}")
            assert result == "secret_value_123"
    
    def test_resolve_databricks_secret_format(self):
        """Test resolving {{secrets/scope/key}} format."""
        with patch.object(self.resolver, "_get_dbutils_secret", return_value="secret_value"):
            result = self.resolver.resolve("{{secrets/scope/key}}")
            assert result == "secret_value"
    
    def test_resolve_secret_protocol_format(self):
        """Test resolving secret:// protocol format."""
        with patch.object(self.resolver, "_get_dbutils_secret", return_value="secret_value"):
            result = self.resolver.resolve("secret://scope/key")
            assert result == "secret_value"
    
    def test_extract_scope_and_key(self):
        """Test extracting scope and key from secret reference."""
        scope, key = self.resolver._extract_scope_and_key("{{secrets/my_scope/my_key}}")
        assert scope == "my_scope"
        assert key == "my_key"
        
        scope, key = self.resolver._extract_scope_and_key("secret://my_scope/my_key")
        assert scope == "my_scope"
        assert key == "my_key"


class TestVolumeConfigBuilder(unittest.TestCase):
    """Test VolumeConfigBuilder class."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.base_config = ConnectorConfig("volume", {
            "path": "/Volumes/landing/volume1/data"
        })
        self.centralized_config = Mock(spec=CentralizedPipelineConfig)
        self.centralized_config.landing_catalog = "landing"
        self.centralized_config.landing_schema = "landing_schema"
        self.centralized_config.filetype = "parquet"
    
    def test_merge_shared_context(self):
        """Test merging volume-specific context."""
        builder = VolumeConfigBuilder(self.base_config, self.centralized_config)
        result = builder.merge_shared_context()
        
        assert result == builder  # Should return self for chaining
        assert builder.config.get("catalog") == "landing"
        assert builder.config.get("schema") == "landing_schema"
        assert builder.config.get("format") == "parquet"
    
    def test_fluent_api_chaining(self):
        """Test fluent API method chaining."""
        builder = VolumeConfigBuilder(self.base_config, self.centralized_config)
        
        result = (builder
                  .merge_shared_context()
                  .resolve_secrets()
                  .build())
        
        assert isinstance(result, dict)
        assert result["path"] == "/Volumes/landing/volume1/data"
        assert result["catalog"] == "landing"


class TestRestApiConfigBuilder(unittest.TestCase):
    """Test RestApiConfigBuilder class."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.base_config = ConnectorConfig("rest_api", {
            "endpoint": "https://api.example.com",
            "auth_type": "bearer",
            "auth_token": "test_token"
        })
        self.centralized_config = Mock(spec=CentralizedPipelineConfig)
        self.centralized_config.raw_catalog = "raw"
        self.centralized_config.raw_schema = "raw_schema"
    
    def test_merge_shared_context_workflow_datasource(self):
        """Test merging context for workflow datasource."""
        config = ConnectorConfig("rest_api_workflow_ds", {
            "endpoint": "https://api.example.com"
        })
        builder = RestApiConfigBuilder(config, self.centralized_config)
        result = builder.merge_shared_context()
        
        assert result == builder
        assert builder.config.get("raw_catalog") == "raw"
        assert builder.config.get("raw_schema") == "raw_schema"
    
    def test_merge_shared_context_rest_api(self):
        """Test merging context for standard rest_api."""
        builder = RestApiConfigBuilder(self.base_config, self.centralized_config)
        result = builder.merge_shared_context()
        
        assert result == builder
        # Standard REST API should not add workflow-specific context
    
    def test_build_spark_schema(self):
        """Test building Spark schema from data contract."""
        schema_object = Mock()
        schema_object.properties = [
            Mock(name="id", type="integer"),
            Mock(name="value", type="double")
        ]
        
        builder = RestApiConfigBuilder(self.base_config, self.centralized_config)
        
        with patch("src.framework.helper.schema_properties_to_spark_schema") as mock_build:
            mock_spark_schema = Mock()
            mock_build.return_value = mock_spark_schema
            
            result = builder.build_spark_schema("test_model", schema_object)
            
            assert result == builder  # Should return self for chaining
            assert builder.config.get("schema") == mock_spark_schema
    
    def test_pre_load_oauth2_token(self):
        """Test pre-loading OAuth2 token."""
        config = ConnectorConfig("rest_api", {
            "endpoint": "https://api.example.com",
            "auth_type": "oauth2_refresh",
            "refresh_token": "refresh_123",
        })
        
        builder = RestApiConfigBuilder(config, self.centralized_config)
        
        # Mock the OAuth2TokenManager
        with patch("src.framework.connectors.oauth2_token_manager.OAuth2TokenManager") as mock_manager_class:
            mock_manager = Mock()
            mock_manager.exchange_token.return_value = "access_token_123"
            mock_manager_class.return_value = mock_manager
            
            result = builder.pre_load_oauth2_token("test_model")
            
            assert result == builder  # Should return self for chaining


class TestConnectorConfigBuilderFactory(unittest.TestCase):
    """Test ConnectorConfigBuilderFactory class."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.centralized_config = Mock(spec=CentralizedPipelineConfig)
        self.centralized_config.landing_catalog = "landing"
        self.centralized_config.landing_schema = "landing_schema"
        self.centralized_config.filetype = "parquet"
        self.centralized_config.raw_catalog = "raw"
        self.centralized_config.raw_schema = "raw_schema"
    
    def test_create_builder_volume(self):
        """Test creating VolumeConfigBuilder."""
        config = ConnectorConfig("volume", {"path": "/test"})
        builder = ConnectorConfigBuilderFactory.create_builder(
            "volume", config, self.centralized_config
        )
        
        assert isinstance(builder, VolumeConfigBuilder)
    
    def test_create_builder_rest_api(self):
        """Test creating RestApiConfigBuilder."""
        config = ConnectorConfig("rest_api", {"endpoint": "https://api.example.com"})
        builder = ConnectorConfigBuilderFactory.create_builder(
            "rest_api", config, self.centralized_config
        )
        
        assert isinstance(builder, RestApiConfigBuilder)
    
    def test_create_builder_rest_api_ds(self):
        """Test creating RestApiConfigBuilder for rest_api_ds."""
        config = ConnectorConfig("rest_api_ds", {"endpoint": "https://api.example.com"})
        builder = ConnectorConfigBuilderFactory.create_builder(
            "rest_api_ds", config, self.centralized_config
        )
        
        assert isinstance(builder, RestApiConfigBuilder)
    
    def test_create_builder_rest_api_workflow_ds(self):
        """Test creating RestApiConfigBuilder for workflow."""
        config = ConnectorConfig("rest_api_workflow_ds", {"endpoint": "https://api.example.com"})
        builder = ConnectorConfigBuilderFactory.create_builder(
            "rest_api_workflow_ds", config, self.centralized_config
        )
        
        assert isinstance(builder, RestApiConfigBuilder)
    
    def test_create_builder_jdbc(self):
        """Test creating JdbcConfigBuilder."""
        config = ConnectorConfig("jdbc", {"host": "localhost"})
        builder = ConnectorConfigBuilderFactory.create_builder(
            "jdbc", config, self.centralized_config
        )
        
        assert isinstance(builder, JdbcConfigBuilder)
    
    def test_create_builder_autoloader(self):
        """Test creating AutoLoaderConfigBuilder."""
        config = ConnectorConfig("autoloader", {"path": "/path"})
        builder = ConnectorConfigBuilderFactory.create_builder(
            "autoloader", config, self.centralized_config
        )
        
        assert isinstance(builder, AutoLoaderConfigBuilder)
    
    def test_create_builder_unknown_type(self):
        """Test creating builder with unknown type raises error."""
        config = ConnectorConfig("unknown", {})
        
        with pytest.raises(ValueError) as exc_info:
            ConnectorConfigBuilderFactory.create_builder(
                "unknown", config, self.centralized_config
            )
        assert "No builder found" in str(exc_info.value)
    
    def test_register_custom_builder(self):
        """Test registering custom builder."""
        class CustomBuilder(BaseConfigBuilder):
            def merge_shared_context(self):
                return self
        
        ConnectorConfigBuilderFactory.register_builder("custom", CustomBuilder)
        
        config = ConnectorConfig("custom", {})
        builder = ConnectorConfigBuilderFactory.create_builder(
            "custom", config, self.centralized_config
        )
        
        assert isinstance(builder, CustomBuilder)
    
    def test_get_registered_types(self):
        """Test getting list of registered types."""
        types = ConnectorConfigBuilderFactory.get_registered_types()
        
        assert "volume" in types
        assert "rest_api" in types
        assert "jdbc" in types
        assert "autoloader" in types


class TestBaseConfigBuilder(unittest.TestCase):
    """Test BaseConfigBuilder abstract class."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.base_config = ConnectorConfig("volume", {
            "path": "/test",
            "params": {"existing": "value"}
        })
        self.centralized_config = Mock(spec=CentralizedPipelineConfig)
    
    def test_merge_schema_overrides_with_properties(self):
        """Test merging schema-level property overrides."""
        schema = Mock()
        schema.customProperties = [
            Mock(property="table_name", value="my_table"),
            Mock(property="params", value={"new": "param"}),
            Mock(property="secret_keys", value=["token"])
        ]
        
        builder = VolumeConfigBuilder(self.base_config, self.centralized_config)
        result = builder.merge_schema_overrides(schema)
        
        assert result == builder
        assert builder.config.get("table_name") == "my_table"
        # Params should be merged
        params = builder.config.get("params")
        assert params.get("existing") == "value"
        assert params.get("new") == "param"
        # Secret keys should be extended
        secret_keys = builder.config.get("secret_keys")
        assert "token" in secret_keys
    
    def test_merge_schema_overrides_without_properties(self):
        """Test merging with no schema properties."""
        schema = Mock()
        schema.customProperties = None
        
        builder = VolumeConfigBuilder(self.base_config, self.centralized_config)
        result = builder.merge_schema_overrides(schema)
        
        assert result == builder
    
    def test_resolve_secrets_success(self):
        """Test successfully resolving secrets."""
        config = ConnectorConfig("rest_api", {
            "endpoint": "https://api.example.com",
            "auth_token": "{{spark.api-token}}",
            "secret_keys": ["auth_token"]
        })
        
        spark = Mock()
        spark.conf.get.return_value = "resolved_token_123"
        
        builder = RestApiConfigBuilder(config, self.centralized_config)
        
        with patch("src.framework.helper.databricks_helper.get_spark", return_value=spark):
            result = builder.resolve_secrets()
            
            assert result == builder
            assert builder.config.get("auth_token") == "resolved_token_123"
    
    def test_build_returns_dict(self):
        """Test build() returns configuration dictionary."""
        # Create centralized config with required attributes
        centralized = Mock(spec=CentralizedPipelineConfig)
        centralized.landing_catalog = "landing"
        centralized.landing_schema = "schema"
        centralized.filetype = "parquet"
        
        builder = VolumeConfigBuilder(self.base_config, centralized)
        builder.merge_shared_context()
        
        result = builder.build()
        
        assert isinstance(result, dict)
        assert result["path"] == "/test"
        assert result["catalog"] == "landing"
        assert result["schema"] == "schema"


class TestConfigIntegration(unittest.TestCase):
    """Integration tests for the complete configuration system."""
    
    def test_full_volume_workflow(self):
        """Test complete workflow for volume connector."""
        # Create centralized config
        centralized = CentralizedPipelineConfig(
            source_system_name="lakehouse",
            environment="dev",
            landing_catalog="landing",
            raw_catalog="raw",
            base_catalog="base",
            curated_catalog="curated",
            enriched_catalog="enriched",
            landing_schema="landing_schema",
            raw_schema="raw_schema",
            base_schema="base_schema",
            dimensions_schema="dimensions",
            facts_schema="facts",
            enriched_schema="enriched"
        )
        
        # Create connector config
        connector = ConnectorConfig("volume", {
            "path": "/Volumes/data"
        })
        
        # Build using factory
        builder = ConnectorConfigBuilderFactory.create_builder("volume", connector, centralized)
        final_config = (builder
                       .merge_shared_context()
                       .resolve_secrets()
                       .build())
        
        # Verify result
        assert final_config["path"] == "/Volumes/data"
        assert final_config["catalog"] == "landing"
        assert final_config["schema"] == "landing_schema"
    
    def test_full_rest_api_workflow(self):
        """Test complete workflow for REST API connector."""
        # Create centralized config
        centralized = CentralizedPipelineConfig(
            source_system_name="lakehouse",
            environment="dev",
            landing_catalog="landing",
            raw_catalog="raw",
            base_catalog="base",
            curated_catalog="curated",
            enriched_catalog="enriched",
            landing_schema="landing_schema",
            raw_schema="raw_schema",
            base_schema="base_schema",
            dimensions_schema="dimensions",
            facts_schema="facts",
            enriched_schema="enriched"
        )
        
        # Create connector config
        connector = ConnectorConfig("rest_api_workflow_ds", {
            "endpoint": "https://api.example.com",
            "auth_type": "bearer",
            "auth_token": "test_token"
        })
        
        # Build using factory
        builder = ConnectorConfigBuilderFactory.create_builder(
            "rest_api_workflow_ds", connector, centralized
        )
        final_config = (builder
                       .merge_shared_context()
                       .resolve_secrets()
                       .build())
        
        # Verify result
        assert final_config["endpoint"] == "https://api.example.com"
        assert final_config["raw_catalog"] == "raw"
        assert final_config["raw_schema"] == "raw_schema"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
