"""
Tests for PySpark DataSource API integration.

Tests the new DataSource-based connectors (Spark 4.0+) for non-Databricks sources:
- REST API DataSource
- Schema inference
- Partition creation
- Batch reading
- Streaming support
"""

import pytest
from unittest.mock import Mock, MagicMock, patch
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from src.framework.connectors import RestApiDataSource
from src.framework.connectors.partition_strategies import PageInputPartition, OffsetInputPartition
from src.framework.connectors.pyspark_datasource_adapter import SimpleInputPartition


class TestRestApiDataSource:
    """Test suite for RestApiDataSource (PySpark DataSource API)."""
    
    def test_datasource_initialization(self):
        """Test RestApiDataSource initializes with config."""
        config = {
            "endpoint": "https://api.example.com/data",
            "method": "GET"
        }
        
        datasource = RestApiDataSource(config)
        
        assert datasource.config["endpoint"] == "https://api.example.com/data"
        assert datasource.config["method"] == "GET"
    
    def test_datasource_name(self):
        """Test DataSource name registration."""
        assert RestApiDataSource.name() == "rest_api"
    
    @patch('src.framework.connectors.rest_api_datasource.requests.request')
    def test_schema_inference(self, mock_request):
        """Test schema inference from API response."""
        # Mock API response
        mock_response = Mock()
        mock_response.json.return_value = [
            {"id": 1, "name": "test", "active": True}
        ]
        mock_response.raise_for_status = Mock()
        mock_request.return_value = mock_response
        
        config = {
            "endpoint": "https://api.example.com/data",
            "method": "GET"
        }
        
        datasource = RestApiDataSource(config)
        schema = datasource.schema()
        
        assert isinstance(schema, StructType)
        field_names = [f.name for f in schema.fields]
        assert "id" in field_names
        assert "name" in field_names
        assert "active" in field_names
    
    def test_schema_from_config(self):
        """Test using provided schema from config."""
        provided_schema = StructType([
            StructField("custom_field", StringType(), True)
        ])
        
        config = {
            "endpoint": "https://api.example.com/data",
            "method": "GET",
            "schema": provided_schema
        }
        
        datasource = RestApiDataSource(config)
        schema = datasource.schema()
        
        assert schema == provided_schema
    
    def test_create_reader(self):
        """Test creating a batch reader."""
        config = {
            "endpoint": "https://api.example.com/data",
            "method": "GET"
        }
        schema = StructType([StructField("id", IntegerType(), True)])
        
        datasource = RestApiDataSource(config)
        reader = datasource.create_reader(schema)
        
        assert reader is not None
        assert reader.config == config
        assert reader.schema_struct == schema
    
    def test_create_stream_reader(self):
        """Test creating a streaming reader."""
        config = {
            "endpoint": "https://api.example.com/data",
            "method": "GET"
        }
        schema = StructType([StructField("id", IntegerType(), True)])
        
        datasource = RestApiDataSource(config)
        stream_reader = datasource.create_stream_reader(schema)
        
        assert stream_reader is not None
        assert stream_reader.config == config
        assert stream_reader.schema_struct == schema


class TestRestApiDataSourceReader:
    """Test suite for RestApiDataSourceReader."""
    
    def test_reader_initialization(self):
        """Test reader initializes with config and schema."""
        from src.framework.connectors.rest_api_datasource import RestApiDataSourceReader
        
        config = {
            "endpoint": "https://api.example.com/data",
            "method": "GET"
        }
        schema = StructType([StructField("id", IntegerType(), True)])
        
        reader = RestApiDataSourceReader(config, schema)
        
        assert reader.config == config
        assert reader.schema_struct == schema
    
    def test_create_partitions_no_pagination(self):
        """Test partition creation without pagination."""
        from src.framework.connectors.rest_api_datasource import RestApiDataSourceReader
        
        config = {
            "endpoint": "https://api.example.com/data",
            "method": "GET",
            "pagination_type": "none"
        }
        schema = StructType([StructField("id", IntegerType(), True)])
        
        reader = RestApiDataSourceReader(config, schema)
        partitions = reader.create_partitions()
        
        assert len(partitions) == 1
        assert isinstance(partitions[0], SimpleInputPartition)
    
    def test_create_partitions_page_based(self):
        """Test partition creation with page-based pagination."""
        from src.framework.connectors.rest_api_datasource import RestApiDataSourceReader
        
        config = {
            "endpoint": "https://api.example.com/data",
            "method": "GET",
            "pagination_type": "page",
            "pagination_config": {
                "start_page": 1,
                "max_pages": 5,
                "page_size": 50
            }
        }
        schema = StructType([StructField("id", IntegerType(), True)])
        
        reader = RestApiDataSourceReader(config, schema)
        partitions = reader.create_partitions()
        
        assert len(partitions) == 5
        assert all(isinstance(p, PageInputPartition) for p in partitions)
        assert partitions[0].page_number == 1
        assert partitions[4].page_number == 5
    
    def test_create_partitions_offset_based(self):
        """Test partition creation with offset-based pagination."""
        from src.framework.connectors.rest_api_datasource import RestApiDataSourceReader
        
        config = {
            "endpoint": "https://api.example.com/data",
            "method": "GET",
            "pagination_type": "offset",
            "pagination_config": {
                "total_records": 250,
                "limit": 100
            }
        }
        schema = StructType([StructField("id", IntegerType(), True)])
        
        reader = RestApiDataSourceReader(config, schema)
        partitions = reader.create_partitions()
        
        assert len(partitions) == 3  # 0-99, 100-199, 200-249
        assert all(isinstance(p, PageInputPartition) for p in partitions)
        assert partitions[0].offset == 0
        assert partitions[1].offset == 100
        assert partitions[2].offset == 200


class TestRestApiDataSourceStreamReader:
    """Test suite for RestApiDataSourceStreamReader."""
    
    def test_stream_reader_initialization(self):
        """Test stream reader initializes with config and schema."""
        from src.framework.connectors.rest_api_datasource import RestApiDataSourceStreamReader
        
        config = {
            "endpoint": "https://api.example.com/data",
            "method": "GET"
        }
        schema = StructType([StructField("id", IntegerType(), True)])
        
        reader = RestApiDataSourceStreamReader(config, schema)
        
        assert reader.config == config
        assert reader.schema_struct == schema
    
    def test_initial_offset_timestamp(self):
        """Test initial offset with timestamp-based tracking."""
        from src.framework.connectors.rest_api_datasource import RestApiDataSourceStreamReader
        
        config = {
            "endpoint": "https://api.example.com/data",
            "method": "GET",
            "offset_type": "timestamp",
            "start_time": "2024-01-01T00:00:00"
        }
        schema = StructType([StructField("id", IntegerType(), True)])
        
        reader = RestApiDataSourceStreamReader(config, schema)
        offset = reader.get_initial_offset()
        
        assert offset["timestamp"] == "2024-01-01T00:00:00"
        assert offset["last_id"] is None
    
    def test_initial_offset_id_based(self):
        """Test initial offset with ID-based tracking."""
        from src.framework.connectors.rest_api_datasource import RestApiDataSourceStreamReader
        
        config = {
            "endpoint": "https://api.example.com/data",
            "method": "GET",
            "offset_type": "id",
            "start_id": 100
        }
        schema = StructType([StructField("id", IntegerType(), True)])
        
        reader = RestApiDataSourceStreamReader(config, schema)
        offset = reader.get_initial_offset()
        
        assert offset["timestamp"] is None
        assert offset["last_id"] == 100
    
    def test_create_stream_partitions(self):
        """Test creating partitions for streaming."""
        from src.framework.connectors.rest_api_datasource import RestApiDataSourceStreamReader
        
        config = {
            "endpoint": "https://api.example.com/data",
            "method": "GET"
        }
        schema = StructType([StructField("id", IntegerType(), True)])
        
        reader = RestApiDataSourceStreamReader(config, schema)
        partitions = reader.create_stream_partitions(
            start={"timestamp": "2024-01-01", "last_id": 100},
            end={"timestamp": "2024-01-02", "last_id": 200}
        )
        
        assert len(partitions) == 1
        assert isinstance(partitions[0], OffsetInputPartition)


class TestPartitionStrategies:
    """Test partition strategy classes."""
    
    def test_page_input_partition(self):
        """Test PageInputPartition creation."""
        partition = PageInputPartition(page_number=5, limit=100)
        
        assert partition.page_number == 5
        assert partition.limit == 100
        assert "page=5" in str(partition)
    
    def test_offset_input_partition(self):
        """Test OffsetInputPartition creation."""
        partition = OffsetInputPartition(
            start_offset={"id": 100},
            end_offset={"id": 200},
            partition_id="partition_1"
        )
        
        assert partition.start_offset == {"id": 100}
        assert partition.end_offset == {"id": 200}
        assert partition.partition_id == "partition_1"
        assert "partition_1" in str(partition)
    
    def test_simple_input_partition(self):
        """Test SimpleInputPartition creation."""
        partition = SimpleInputPartition({"key": "value"})
        
        assert partition.value == {"key": "value"}
        assert "SimpleInputPartition" in str(partition)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
