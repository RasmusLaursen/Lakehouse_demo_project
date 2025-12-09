"""
Test connector framework implementation.

This script validates:
1. Connector registration and factory
2. Volume connector configuration
3. REST API connector configuration
4. JDBC connector configuration
5. PipelineConfig connector integration
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.framework.connectors import (
    ConnectorFactory, 
    AutoLoaderConnector,
    VolumeConnector,  # Backward compatibility alias
    RestApiConnector, 
    JdbcConnector,
    TableConnector,
    DataFrameConnector
)
from src.framework.factory.config import PipelineConfig


def test_connector_registration():
    """Test that all connectors are properly registered."""
    print("Testing connector registration...")
    
    registered = ConnectorFactory.get_registered_types()
    print(f"Registered connector types: {registered}")
    
    expected_types = [
        "autoloader",
        "volume", 
        "volume_autoloader",
        "s3",
        "adls",
        "gcs",
        "kafka",
        "eventhub",
        "rest_api", 
        "http", 
        "https", 
        "jdbc", 
        "database",
        "table",
        "table_stream",
        "dataframe"
    ]
    
    for connector_type in expected_types:
        assert ConnectorFactory.is_registered(connector_type), \
            f"Connector '{connector_type}' not registered"
    
    print("✓ All connectors registered successfully")


def test_autoloader_connector_volume():
    """Test AutoLoaderConnector with volume source."""
    print("\nTesting AutoLoaderConnector (volume source)...")
    
    # Valid volume configuration
    config = {
        "source_type": "volume",
        "catalog": "landing",
        "schema": "lakehouse_landing",
        "volume": "customer_contract",
        "format": "parquet",
        "add_audit_columns": True
    }
    
    connector = ConnectorFactory.create("autoloader", config)
    assert isinstance(connector, AutoLoaderConnector), "Should create AutoLoaderConnector"
    print("✓ AutoLoaderConnector (volume) created successfully")
    
    # Test backward compatibility with "volume" type
    connector2 = ConnectorFactory.create("volume", config)
    assert isinstance(connector2, AutoLoaderConnector), "Should create AutoLoaderConnector"
    print("✓ Backward compatibility with 'volume' type works")
    
    # Test invalid configuration
    try:
        invalid_config = {"source_type": "volume", "catalog": "landing"}
        ConnectorFactory.create("autoloader", invalid_config)
        assert False, "Should raise ValueError for missing fields"
    except ValueError as e:
        print(f"✓ Validation caught missing fields: {e}")


def test_autoloader_connector_s3():
    """Test AutoLoaderConnector with S3 source."""
    print("\nTesting AutoLoaderConnector (S3 source)...")
    
    config = {
        "source_type": "s3",
        "path": "s3://my-bucket/data/customers/",
        "format": "json",
        "options": {
            "cloudFiles.schemaLocation": "s3://my-bucket/schemas/",
            "cloudFiles.inferColumnTypes": "true"
        }
    }
    
    connector = ConnectorFactory.create("s3", config)
    assert isinstance(connector, AutoLoaderConnector), "Should create AutoLoaderConnector"
    print("✓ AutoLoaderConnector (S3) created successfully")


def test_autoloader_connector_kafka():
    """Test AutoLoaderConnector with Kafka source."""
    print("\nTesting AutoLoaderConnector (Kafka source)...")
    
    config = {
        "source_type": "kafka",
        "kafka_bootstrap_servers": "localhost:9092",
        "topics": ["customer-events", "order-events"],
        "options": {
            "startingOffsets": "latest",
            "kafka.security.protocol": "SASL_SSL"
        }
    }
    
    connector = ConnectorFactory.create("kafka", config)
    assert isinstance(connector, AutoLoaderConnector), "Should create AutoLoaderConnector"
    print("✓ AutoLoaderConnector (Kafka) created successfully")
    
    # Test invalid Kafka config
    try:
        invalid_config = {"source_type": "kafka", "topics": "my-topic"}
        ConnectorFactory.create("kafka", invalid_config)
        assert False, "Should raise ValueError for missing kafka_bootstrap_servers"
    except ValueError as e:
        print(f"✓ Validation caught missing kafka config: {e}")


def test_autoloader_connector_eventhub():
    """Test AutoLoaderConnector with Event Hub source."""
    print("\nTesting AutoLoaderConnector (Event Hub source)...")
    
    config = {
        "source_type": "eventhub",
        "eventhub_connection_string": "Endpoint=sb://namespace.servicebus.windows.net/;...",
        "eventhub_name": "customer-events"
    }
    
    connector = ConnectorFactory.create("eventhub", config)
    assert isinstance(connector, AutoLoaderConnector), "Should create AutoLoaderConnector"
    print("✓ AutoLoaderConnector (Event Hub) created successfully")


def test_rest_api_connector():
    """Test RestApiConnector configuration and validation."""
    print("\nTesting RestApiConnector...")
    
    # Valid configuration
    config = {
        "endpoint": "https://api.example.com/v1/data",
        "method": "GET",
        "auth_type": "bearer",
        "auth_token": "test_token_12345",
        "pagination_type": "offset",
        "pagination_config": {
            "limit": 100,
            "offset_param": "offset",
            "limit_param": "limit"
        },
        "data_path": "data.items"
    }
    
    connector = ConnectorFactory.create("rest_api", config)
    assert isinstance(connector, RestApiConnector), "Should create RestApiConnector"
    print("✓ RestApiConnector created successfully")
    
    # Test invalid configuration
    try:
        invalid_config = {"endpoint": "https://api.example.com"}  # Missing method
        ConnectorFactory.create("rest_api", invalid_config)
        assert False, "Should raise ValueError for missing method"
    except ValueError as e:
        print(f"✓ Validation caught missing method: {e}")


def test_jdbc_connector():
    """Test JdbcConnector configuration and validation."""
    print("\nTesting JdbcConnector...")
    
    # Valid configuration
    config = {
        "url": "jdbc:postgresql://localhost:5432/mydb",
        "table": "customers",
        "user": "admin",
        "password": "secret",
        "partition_column": "id",
        "lower_bound": 1,
        "upper_bound": 100000,
        "num_partitions": 10
    }
    
    connector = ConnectorFactory.create("jdbc", config)
    assert isinstance(connector, JdbcConnector), "Should create JdbcConnector"
    print("✓ JdbcConnector created successfully")
    
    # Test invalid partitioning configuration
    try:
        invalid_config = {
            "url": "jdbc:mysql://localhost:3306/mydb",
            "table": "orders",
            "partition_column": "id"  # Missing other partition fields
        }
        ConnectorFactory.create("jdbc", invalid_config)
        assert False, "Should raise ValueError for incomplete partitioning"
    except ValueError as e:
        print(f"✓ Validation caught incomplete partitioning: {e}")


def test_connector_aliases():
    """Test that connector type aliases work correctly."""
    print("\nTesting connector type aliases...")
    
    # volume_autoloader should create AutoLoaderConnector
    config = {
        "source_type": "volume",
        "catalog": "landing",
        "schema": "test",
        "volume": "data",
        "format": "json"
    }
    connector = ConnectorFactory.create("volume_autoloader", config)
    assert isinstance(connector, AutoLoaderConnector), "Alias 'volume_autoloader' should work"
    
    # http/https should create RestApiConnector
    api_config = {
        "endpoint": "https://api.test.com",
        "method": "GET"
    }
    connector1 = ConnectorFactory.create("http", api_config)
    connector2 = ConnectorFactory.create("https", api_config)
    assert isinstance(connector1, RestApiConnector), "Alias 'http' should work"
    assert isinstance(connector2, RestApiConnector), "Alias 'https' should work"
    
    # database should create JdbcConnector
    db_config = {
        "url": "jdbc:postgresql://localhost/db",
        "table": "test"
    }
    connector = ConnectorFactory.create("database", db_config)
    assert isinstance(connector, JdbcConnector), "Alias 'database' should work"
    
    print("✓ All connector aliases work correctly")


def test_unknown_connector_type():
    """Test that unknown connector types raise appropriate error."""
    print("\nTesting unknown connector type...")
    
    try:
        config = {"some": "config"}
        ConnectorFactory.create("unknown_type", config)
        assert False, "Should raise ValueError for unknown connector"
    except ValueError as e:
        assert "Unknown connector type" in str(e)
        print(f"✓ Unknown connector type rejected: {e}")


def test_pipeline_config_connector_fields():
    """Test that PipelineConfig has connector fields with defaults."""
    print("\nTesting PipelineConfig connector fields...")
    
    # PipelineConfig should have connector_type and connector_config
    # We can't fully test from_spark without a real SparkSession,
    # but we can verify the dataclass structure
    
    from dataclasses import fields
    config_fields = {f.name for f in fields(PipelineConfig)}
    
    assert "connector_type" in config_fields, "PipelineConfig missing connector_type field"
    assert "connector_config" in config_fields, "PipelineConfig missing connector_config field"
    
    print("✓ PipelineConfig has connector fields")


def test_table_connector():
    """Test TableConnector for reading Unity Catalog tables."""
    print("\nTesting TableConnector...")
    
    # Batch table read
    config = {
        "catalog": "raw",
        "schema": "lakehouse",
        "table": "customer",
        "streaming": False,
        "add_audit_columns": True
    }
    
    connector = ConnectorFactory.create("table", config)
    assert isinstance(connector, TableConnector), "Should create TableConnector"
    print("✓ TableConnector created successfully")
    
    # Streaming table read
    streaming_config = {
        "catalog": "raw",
        "schema": "lakehouse",
        "table": "customer",
        "streaming": True
    }
    
    connector2 = ConnectorFactory.create("table_stream", streaming_config)
    assert isinstance(connector2, TableConnector), "Should create TableConnector for streaming"
    print("✓ TableConnector with streaming works")
    
    # Test validation
    try:
        invalid_config = {"catalog": "raw"}
        ConnectorFactory.create("table", invalid_config)
        assert False, "Should raise ValueError for missing fields"
    except ValueError as e:
        print(f"✓ Validation caught missing fields: {e}")


def test_dataframe_connector():
    """Test DataFrameConnector for wrapping existing DataFrames."""
    print("\nTesting DataFrameConnector...")
    
    # We can't create real DataFrames without Spark, but we can test structure
    # This will be tested in actual Databricks environment
    
    # Test that connector is registered
    assert ConnectorFactory.is_registered("dataframe"), "DataFrameConnector not registered"
    print("✓ DataFrameConnector is registered")
    
    # Note: Full DataFrame tests require Spark runtime
    print("✓ DataFrameConnector structure validated (full test requires Spark)")


def run_all_tests():
    """Run all connector framework tests."""
    print("="*60)
    print("CONNECTOR FRAMEWORK VALIDATION")
    print("="*60)
    
    try:
        test_connector_registration()
        test_autoloader_connector_volume()
        test_autoloader_connector_s3()
        test_autoloader_connector_kafka()
        test_autoloader_connector_eventhub()
        test_rest_api_connector()
        test_jdbc_connector()
        test_connector_aliases()
        test_unknown_connector_type()
        test_pipeline_config_connector_fields()
        test_table_connector()
        test_dataframe_connector()
        
        print("\n" + "="*60)
        print("✅ ALL TESTS PASSED")
        print("="*60)
        print("\nConnector framework is ready to use!")
        print("\nAvailable connectors:")
        for connector_type in ConnectorFactory.get_registered_types():
            print(f"  - {connector_type}")
        
    except AssertionError as e:
        print(f"\n❌ TEST FAILED: {e}")
        raise
    except Exception as e:
        print(f"\n❌ UNEXPECTED ERROR: {e}")
        raise


if __name__ == "__main__":
    run_all_tests()
