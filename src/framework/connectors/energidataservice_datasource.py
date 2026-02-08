"""
Energidataservice-specific DataSource implementation.

Handles the Danish Energy Data Service public API with simple
offset-based pagination and flat response structure. Standalone
implementation for batch reading with Energidataservice-specific logic.
"""

from typing import Dict, Any, List, Iterator, Union, TYPE_CHECKING
import requests
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType

if TYPE_CHECKING:
    from pyspark.sql import SparkSession, DataFrame

from src.framework.connectors.pyspark_datasource_adapter import (
    BasePySparkDataSource,
    BaseDataSourceReader,
    SimpleInputPartition,
)
from src.framework.connectors.api_helper import api_call_with_retry, paginated_api_call
from src.framework.helper import logging_helper

logger = logging_helper.get_logger(__name__)


class EnergidataserviceDataSource(BasePySparkDataSource):
    """
    Standalone DataSource for Energidataservice public API.
    
    Handles:
    - No authentication (public API)
    - Simple response structure with "records" array at top level
    - Offset-based pagination
    - Timestamp-based incremental loads
    - Flat JSON structure (no nesting)
    """
    
    # Class-level schema cache to persist across Spark instantiations
    _schema_cache: Dict[str, StructType] = {}
    
    def __init__(self, options: Dict[str, str]) -> None:
        """Initialize Energidataservice DataSource."""
        super().__init__(options)
        
        # If schema is in config (from builder), cache it
        if "schema" in self.config and isinstance(self.config["schema"], StructType):
            schema = self.config["schema"]
            # Use endpoint or table_name as cache key
            cache_key = self.config.get("endpoint", self.config.get("table_name", "default"))
            EnergidataserviceDataSource._schema_cache[cache_key] = schema
            logger.info(f"Cached schema for {cache_key} with {len(schema.fields)} fields")
        
        logger.info("Initialized EnergidataserviceDataSource")
    
    @classmethod
    def name(cls) -> str:
        """Return the short name for this data source."""
        return "energidataservice_api"
    
    def schema(self) -> Union[StructType, str]:
        """
        Return schema for the DataFrame.
        
        Checks cache first, then config for schema from data contract.
        Falls back to generic schema if neither available.
        """
        # Try to get schema from cache first (persists across Spark instantiations)
        cache_key = self.config.get("endpoint", self.config.get("table_name", "default"))
        
        if cache_key in EnergidataserviceDataSource._schema_cache:
            cached_schema = EnergidataserviceDataSource._schema_cache[cache_key]
            logger.info(f"Using cached schema for {cache_key} with {len(cached_schema.fields)} fields")
            return cached_schema
        
        # Check if schema is in config (from initial builder instantiation)
        if "schema" in self.config and isinstance(self.config["schema"], StructType):
            schema = self.config["schema"]
            logger.info(f"Using schema from config for {self.name()} with {len(schema.fields)} fields")
            # Cache it for future Spark instantiations
            EnergidataserviceDataSource._schema_cache[cache_key] = schema
            return schema
        
        # Fallback to generic schema
        logger.warning(f"No cached or configured schema found for {cache_key}, using fallback")
        return StructType([StructField("data", StringType(), True)])
    
    def create_reader(self, schema: StructType) -> "EnergidataserviceReader":
        """Create a batch reader for Energidataservice API."""
        return EnergidataserviceReader(self.config, schema)
    
    def read_batch(self, spark: "SparkSession") -> "DataFrame":
        """
        Read data as a batch DataFrame.
        
        Args:
            spark: Active SparkSession
            
        Returns:
            Batch DataFrame from the Energidataservice API
        """
        logger.info(f"read_batch called for {self.name()}")
        
        # Register this DataSource with Spark
        try:
            spark.dataSource.register(self.__class__)
            logger.debug(f"Registered DataSource: {self.name()}")
        except Exception as e:
            logger.debug(f"DataSource may already be registered: {e}")
        
        # Make API call and fetch data
        records = self._fetch_all_records()
        
        # Get schema for DataFrame creation
        df_schema = self.schema()
        
        # Convert records to DataFrame
        if records:
            rows = [Row(**record) for record in records]
            df = spark.createDataFrame(rows, schema=df_schema)
            logger.info(f"Created DataFrame with {df.count()} rows")
            return df
        else:
            # Return empty DataFrame with schema
            logger.warning("No records fetched from Energidataservice API")
            return spark.createDataFrame([], df_schema)
    
    def _fetch_all_records(self) -> List[Dict[str, Any]]:
        """Fetch all records from Energidataservice API with pagination."""
        try:
            url = self._build_url()
            timeout = int(self.config.get("timeout", 30))
            
            logger.info(f"Fetching from Energidataservice API: {url}")
            
            # Build base params (e.g., timestamp filters)
            params = {}
            timestamp_field = self.config.get("timestamp_field")
            initial_timestamp = self.config.get("initial_timestamp")
            if timestamp_field and initial_timestamp:
                params[timestamp_field] = initial_timestamp
            
            # Use paginated_api_call for automatic pagination with retry logic
            all_records = paginated_api_call(
                url=url,
                method="GET",
                params=params,
                timeout=timeout,
                max_retries=3,
                retry_delay=2.0,
                backoff_factor=2.0,
                page_size=1000,
                extract_records=lambda r: self._extract_records_from_response(r.json())
            )
            
            logger.info(f"Completed fetching {len(all_records)} total records")
            return all_records
            
        except Exception as e:
            logger.error(f"Error fetching from Energidataservice API: {e}")
            return []
    
    def _build_url(self) -> str:
        """Build the full API endpoint URL."""
        endpoint = self.config.get("endpoint", "")
        table_name = self.config.get("table_name", "")
        return f"{endpoint}/{table_name}"
    
    def _extract_records_from_response(self, response: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Extract records from Energidataservice API response.
        
        Energidataservice structure: {"records": [...], "total": 123}
        Records are already flat, no transformation needed.
        """
        records = []
        
        try:
            # Simple extraction - records are at top level
            if isinstance(response, dict):
                records = response.get("records", [])
                
                # Ensure it's a list
                if not isinstance(records, list):
                    logger.warning(f"Expected 'records' to be a list, got {type(records)}")
                    records = []
            else:
                logger.warning(f"Expected response to be a dict, got {type(response)}")
        
        except Exception as e:
            logger.error(f"Error extracting records from Energidataservice response: {e}")
        
        return records
    
    def _get_total_count(self, response: Dict[str, Any]) -> int:
        """
        Get total record count from response.
        
        Energidataservice includes total count in response: {"total": 123}
        """
        try:
            if isinstance(response, dict):
                total = response.get("total", 0)
                if isinstance(total, (int, float)):
                    return int(total)
        except Exception as e:
            logger.error(f"Error extracting total count: {e}")
        
        return 0


class EnergidataserviceReader(BaseDataSourceReader):
    """Reader for Energidataservice DataSource."""
    
    def create_partitions(self) -> List[SimpleInputPartition]:
        """Create a single partition for batch reading."""
        return [SimpleInputPartition(0)]
    
    def read_partition(self, partition: SimpleInputPartition) -> Iterator[Row]:
        """
        Read data from a partition.
        
        Note: Actual data reading is handled by read_batch() method.
        This is here to satisfy the DataSourceReader interface.
        """
        # Return empty iterator - actual reading happens in read_batch()
        return iter([])
