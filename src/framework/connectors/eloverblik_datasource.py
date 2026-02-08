"""
Eloverblik API connector following framework patterns.

Implements batch and streaming reading from Danish Eloverblik Customer API
using the framework's BasePySparkDataSource adapter and APIClient utilities.
"""

from typing import Dict, Any, List, Iterator, Tuple, Optional
from datetime import datetime, timedelta
import json
from pyspark.sql import Row, SparkSession, DataFrame
from pyspark.sql.datasource import InputPartition
from pyspark.sql.types import StructType

from src.framework.connectors.pyspark_datasource_adapter import (
    BasePySparkDataSource,
    BaseDataSourceReader,
    BaseSimpleDataSourceStreamReader,
    SimpleInputPartition
)
from src.framework.connectors.api_helper import APIClient, BearerTokenAuth
from src.framework.helper import logging_helper

logger = logging_helper.get_logger(__name__)


class EloverblikDataSource(BasePySparkDataSource):
    """
    DataSource for Eloverblik Customer API with OAuth2 authentication.
    
    Supports both batch reading (metering points) and streaming reading (time series data).
    """
    
    @classmethod
    def name(cls) -> str:
        """Return the short name for this data source."""
        return "eloverblik"
    
    def schema(self) -> StructType:
        """
        Return schema from configuration.
        
        Schema can be provided as:
        - StructType object directly (from read_batch/read_stream methods)
        - JSON string (from spark.readStream.format() options)
        """
        schema = self.config.get("schema")
        
        # If schema is already a StructType, return it
        if isinstance(schema, StructType):
            return schema
        
        # If schema is a JSON string, parse it
        if isinstance(schema, str):
            try:
                schema_dict = json.loads(schema)
                return StructType.fromJson(schema_dict)
            except Exception as e:
                logger.error(f"Failed to parse schema JSON: {e}")
                raise ValueError(f"Invalid schema JSON: {e}")
        
        # Schema not provided
        raise ValueError("Schema must be provided in options as StructType or JSON string")
    
    def create_reader(self, schema: StructType) -> "EloverblikBatchReader":
        """Create a batch reader for metering points."""
        return EloverblikBatchReader(self.config, schema)
    
    def create_stream_reader(self, schema: StructType) -> "EloverblikStreamReaderSimple":
        """Create a streaming reader for time series data."""
        return EloverblikStreamReaderSimple(self.config, schema)
    
    def reader(self, schema: StructType):
        """Return batch reader."""
        return self.create_reader(schema)
    
    def simpleStreamReader(self, schema: StructType):
        """Return streaming reader."""
        return EloverblikStreamReaderSimple(self.config, schema)
    
    def _get_base_url(self) -> str:
        """Build the API URL for fetching metering points."""
        base_url = self.config.get("endpoint")
        if not base_url:
            raise ValueError("Endpoint URL must be provided in config (as 'endpoint')")
        logger.debug(f"Using base URL: {base_url}")
        return base_url

    def _get_schema_url(self) -> str:
        """Get the API URL for fetching schema if needed."""
        schema_url = self.config.get("table_name")
        if not schema_url:
            raise ValueError("Schema URL must be provided in config (as 'table_name')")
        logger.debug(f"Using schema URL: {schema_url}")
        return schema_url
    
    def _get_target_endpoint(self) -> str:
        """Determine the target API endpoint based on config."""
        target_endpoint = self._get_base_url() + self._get_schema_url()
        logger.debug(f"Constructed target endpoint: {target_endpoint}")
        return target_endpoint       
    
    def read_batch(self, spark: "SparkSession") -> "DataFrame":
        """Convenience method for batch reading."""
        logger.info(f"read_batch called for {self.name()}")
        logger.debug(f"Available config keys: {list(self.config.keys())}")
        
        # Register this DataSource with Spark
        try:
            spark.dataSource.register(self.__class__)
            logger.debug(f"Registered DataSource for batch: {self.name()}")
        except Exception as e:
            logger.debug(f"DataSource may already be registered: {e}")
        
        # Get schema from config and serialize to JSON
        schema = self.config.get("schema")
        if not schema:
            raise ValueError("Schema must be provided in config for batch reading")
        
        if isinstance(schema, StructType):
            schema_json = json.dumps(schema.jsonValue())
        else:
            schema_json = schema  # Already a JSON string
        
        # Build datasource_config with required fields
        datasource_config = {
            "schema": schema_json,
            "target_endpoint": self._get_target_endpoint()
        }
        
        # Add any additional options from config (excluding framework-specific keys)
        excluded_keys = {
            'catalog', 'volume', 'source_system', 'model_name', 'format', 'schema'
        }
        for k, v in self.config.items():
            if k not in excluded_keys and not k.endswith('_catalog') and not k.endswith('_schema') and not isinstance(v, StructType):
                datasource_config[str(k)] = str(v)

        logger.info(f"DataSource config keys for batch: {datasource_config}")

        logger.info(f"DataSource config keys for batch: {list(datasource_config.keys())}")
        
        # Use Spark's read API
        df = spark.read.format(self.name()).options(**datasource_config).load()
        logger.info(f"Created batch DataFrame for {self.name()}")
        
        return df

    def read_stream(self, spark: "SparkSession") -> "DataFrame":
        """
        Read data as a streaming DataFrame.
        
        Args:
            spark: Active SparkSession
            
        Returns:
            Streaming DataFrame from the Eloverblik API
        """
        logger.info(f"read_stream called for {self.name()}")
        logger.debug(f"Available config keys: {list(self.config.keys())}")
        
        # Register this DataSource with Spark
        try:
            spark.dataSource.register(self.__class__)
            logger.debug(f"Registered DataSource for streaming: {self.name()}")
        except Exception as e:
            logger.debug(f"DataSource may already be registered: {e}")
        
        # Get schema from config and serialize to JSON
        schema = self.config.get("schema")
        if not schema:
            raise ValueError("Schema must be provided in config for streaming")
        
        if isinstance(schema, StructType):
            schema_json = json.dumps(schema.jsonValue())
        else:
            schema_json = schema  # Already a JSON string

        logger.info(f"utilizing the following config {self.config}")

        # Build datasource_config with required fields
        datasource_config = {
            "schema": schema_json,
            "target_endpoint": self._get_target_endpoint()
        }
        
        # Add any additional options from config (excluding framework-specific keys)
        excluded_keys = {
            'catalog', 'volume', 'source_system', 'model_name', 'format'
        }
        for k, v in self.config.items():
            if k not in excluded_keys and not k.endswith('_catalog') and not k.endswith('_schema') and not isinstance(v, StructType):
                datasource_config[str(k)] = str(v)

        logger.info(f"DataSource config keys for streaming: {list(datasource_config.keys())}")
        
        # Use Spark's readStream API
        df = spark.readStream.format(self.name()).options(**datasource_config).load()
        logger.info(f"Created streaming DataFrame for {self.name()}")
        
        return df    

class EloverblikBatchReader(BaseDataSourceReader):
    """
    Batch reader for Eloverblik metering points.
    
    Fetches list of metering points from the Customer API using OAuth2 authentication.
    """
    
    def __init__(self, config: Dict[str, Any], schema: StructType):
        """Initialize batch reader."""
        super().__init__(config, schema)
        logger.info(f"Initializing EloverblikBatchReader with config: {self.config}")
        self.validate_config()
        self._setup_api_client()
    
    def validate_config(self) -> None:
        """Validate required configuration fields."""
        required = ["auth_token", "token_endpoint", "endpoint"]
        missing = [f for f in required if f not in self.config]
        
        if missing:
            raise ValueError(f"Missing required config fields: {', '.join(missing)}")
    
    def _setup_api_client(self) -> None:
        """Initialize API client with OAuth2 authentication."""
        # Get access token by exchanging refresh token
        refresh_token = self.config["auth_token"]
        self.token_endpoint = self.config["token_endpoint"]
        self.endpoint = self.config["endpoint"]
        self.target_endpoint = self.config["target_endpoint"]
        
        # Exchange refresh token for access token
        token_auth = BearerTokenAuth(refresh_token)
        token_client = APIClient(
            authenticator=token_auth,
            max_retries=3,
            retry_delay=1
        )
        
        try:
            token_response = token_client.get(self.token_endpoint)
            access_token = token_response.json().get("result")
            
            if not access_token:
                raise ValueError("Failed to obtain access token from Eloverblik API")
            
            logger.info("Successfully obtained Eloverblik access token")
            
            # Create API client with access token
            auth = BearerTokenAuth(access_token)
            self.api_client = APIClient(
                authenticator=auth,
                max_retries=3,
                retry_delay=1
            )
            
        except Exception as e:
            logger.error(f"Failed to setup Eloverblik API client: {e}")
            raise
    
    def create_partitions(self) -> List[InputPartition]:
        """Create single partition for batch read."""
        return [SimpleInputPartition(0)]

    def read_partition(self, partition: InputPartition) -> Iterator[Row]:
        """
        Fetch metering points from API.
        
        Args:
            partition: Input partition (single partition)
            
        Yields:
            Row objects with metering point data
        """
   
        try:
            logger.info(f"Fetching metering points from Eloverblik API: {self.target_endpoint}")
            response = self.api_client.get(self.target_endpoint)
            
            # Check if response is empty
            if not response.text or response.text.strip() == "":
                logger.error("API returned empty response body")
                raise ValueError(f"Empty response from Eloverblik API {self.target_endpoint}")
            
            # Try to parse JSON
            try:
                data = response.json()
            except ValueError as json_err:
                logger.error(f"Failed to parse JSON. Response text: {response.text[:1000]}")
                raise ValueError(f"API returned non-JSON response: {json_err}")
            
            response = data.get("result", [])
            
            # Yield rows matching schema
            for mp in response:
                yield Row(**mp)
                
        except Exception as e:
            logger.error(f"Error fetching {self.target_endpoint} points: {e}")
            raise

class EloverblikStreamReaderSimple(BaseSimpleDataSourceStreamReader):
    """
    Streaming reader for Eloverblik time series data with date-based incremental loading.
    
    Uses BaseSimpleDataSourceStreamReader pattern for date-based offset management.
    Fetches metering points once and caches them for subsequent batches.
    """
    
    def __init__(self, config: Dict[str, Any], schema: StructType):
        """Initialize streaming reader."""
        super().__init__(config, schema)
        self.validate_config()
        self._setup_api_client()
        
        # Streaming parameters
        self.aggregation = config.get("aggregation", "Actual")

        if config.get("initial_timestamp"):
            self.start_date = config.get("initial_timestamp")
        else:
            raise ValueError("initial_timestamp must be provided in config for streaming")
        
        self.days_per_batch = int(config.get("days_per_batch", "30"))
        
        # URL template for time series endpoint
        if config.get("endpoint"):
            self.endpoint = config.get("endpoint") 
        else:
            raise ValueError("Endpoint URL must be provided in config for streaming")
        
        # Metering points dependency endpoint
        self.metering_points_url = config.get("metering_points_url",
            "https://api.eloverblik.dk/customerapi/api/meteringpoints/meteringpoints")
        
        # Cache for deterministic replay and metering points
        self._offset_cache = {}
        self._metering_points_cache = self._get_metering_points()
        
        logger.info(f"Initialized EloverblikStreamReader: start_date={self.start_date}, days_per_batch={self.days_per_batch}")

    def get_initial_offset(self) -> dict:
        """Return the initial offset (starting date)."""
        return {"date": self.start_date}            
    
    def validate_config(self) -> None:
        """Validate required configuration fields."""
        required = ["auth_token", "token_endpoint"]
        missing = [f for f in required if f not in self.config]
        
        if missing:
            raise ValueError(f"Missing required config fields: {', '.join(missing)}")
    
    def _setup_api_client(self) -> None:
        """Initialize API client with OAuth2 authentication."""
        refresh_token = self.config["auth_token"]
        self.token_endpoint = self.config["token_endpoint"]
        self.target_endpoint = self.config["target_endpoint"]        
        
        # Exchange refresh token for access token
        token_auth = BearerTokenAuth(refresh_token)
        token_client = APIClient(
            authenticator=token_auth,
            max_retries=3,
            retry_delay=1
        )
        
        try:
            token_response = token_client.get(self.token_endpoint)
            access_token = token_response.json().get("result")
            
            if not access_token:
                raise ValueError("Failed to obtain access token from Eloverblik API")
            
            logger.info("Successfully obtained Eloverblik access token for streaming")
            
            # Store access token for API calls
            self.access_token = access_token
            
        except Exception as e:
            logger.error(f"Failed to setup Eloverblik API client for streaming: {e}")
            raise
    
    def _get_metering_points(self) -> List[str]:
        """
        Fetch metering points from API with caching.
        
        Fetches metering points once and caches the result for subsequent calls.
        
        Returns:
            List of metering point IDs
        """       
        # Fetch from API
        logger.info("Fetching metering points from API (first time)")
        
        auth = BearerTokenAuth(self.access_token)
        api_client = APIClient(
            authenticator=auth,
            max_retries=3,
            retry_delay=1
        )
        
        try:
            response = api_client.get(self.metering_points_url)
            metering_points_data = response.json().get("result", [])
            
            # Extract metering point IDs
            metering_point_ids = [mp.get("meteringPointId") for mp in metering_points_data if mp.get("meteringPointId")]
            
            # Cache the result
            # self._metering_points_cache = metering_point_ids
            logger.info(f"Cached {len(metering_point_ids)} metering point IDs")
            
            return metering_point_ids
            
        except Exception as e:
            logger.error(f"Error fetching metering points: {e}")
            raise
    
    def _get_timeseries_data(self, date_from: str, date_to: str) -> List[Dict[str, Any]]:
        """
        Fetch time series data for a date range.
        
        Args:
            date_from: Start date (YYYY-MM-DD)
            date_to: End date (YYYY-MM-DD)
            
        Returns:
            List of time series records
        """
        # Get metering points (cached)
        metering_point_ids = self._metering_points_cache
        
        if not metering_point_ids:
            logger.warning("No metering points available")
            return []
        
        # Build URL with path parameters
        url = self.target_endpoint.format(
            dateFrom=date_from,
            dateTo=date_to,
            aggregation=self.aggregation
        )
        
        # Build request body
        body = {
            "meteringPoints": {
                "meteringPoint": metering_point_ids
            }
        }
        
        # Make API call
        auth = BearerTokenAuth(self.access_token)
        api_client = APIClient(
            authenticator=auth,
            max_retries=3,
            retry_delay=1
        )
        
        try:
            response = api_client.post(url, json_body=body)
            
            # Extract records from nested structure
            records = self._extract_records(response.json(), date_from, date_to)
            logger.info(f"Extracted {len(records)} records for {date_from} to {date_to}")
            
            return records
            
        except Exception as e:
            logger.error(f"Error fetching time series data: {e}")
            raise
    
    def _extract_records(self, data: dict, date_from: str, date_to: str) -> List[Dict[str, Any]]:
        """
        Extract time series points from nested API response.
        
        Navigates: result[].MyEnergyData_MarketDocument.TimeSeries[].Period[].Point[]
        
        Args:
            data: API response data
            date_from: Start date for this batch
            date_to: End date for this batch
            
        Returns:
            List of flattened records
        """
        records = []
        
        try:
            result = data.get("result", [])
            
            if not isinstance(result, list):
                result = [result] if result else []
            
            for result_item in result:
                market_doc = result_item.get("MyEnergyData_MarketDocument", {})
                
                time_series_list = market_doc.get("TimeSeries", [])
                if not isinstance(time_series_list, list):
                    time_series_list = [time_series_list] if time_series_list else []
                
                for time_series in time_series_list:
                    metering_point_id = time_series.get("mRID")
                    
                    periods = time_series.get("Period", [])
                    if not isinstance(periods, list):
                        periods = [periods] if periods else []
                    
                    for period in periods:
                        period_start = period.get("timeInterval", {}).get("start")
                        period_end = period.get("timeInterval", {}).get("end")
                        resolution = period.get("resolution")
                        
                        points = period.get("Point", [])
                        if not isinstance(points, list):
                            points = [points] if points else []
                        
                        for point in points:
                            # Extract nested out_Quantity fields
                            record = {
                                "meteringPointId": metering_point_id,
                                "position": point.get("position"),
                                "quantity": point.get("out_Quantity.quantity") ,
                                "quality": point.get("out_Quantity.quality"),
                                "period_start": period_start,
                                "period_end": period_end,
                                "resolution": resolution
                            }
                            records.append(record)
        
        except Exception as e:
            logger.error(f"Error extracting records: {e}")
        
        return records
    
    def read_data(self, partition: InputPartition) -> Iterator[Row]:
        """
        Fetch data for the partition's offset range.
        
        Args:
            partition: SimpleInputPartition containing start and end offsets
            
        Returns:
            Iterator of Row objects
        """
        # Extract offsets from partition
        start = partition.value["start"]
        end = partition.value["end"]
        
        # Handle None offsets
        if start is None or "date" not in start:
            start = self.get_initial_offset()
        
        if end is None or "date" not in end:
            end = self.get_latest_offset()
        
        current_date = start["date"]
        end_date = end["date"]
        logger.info(f"[EloverblikStream] Reading from {current_date} to {end_date}")
        
        # Calculate date range for this batch
        date_from_obj = datetime.strptime(current_date, "%Y-%m-%d")
        date_to_obj = datetime.strptime(end_date, "%Y-%m-%d")
        
        # Get current date (today) - strip time for comparison
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        
        # Check if we've reached current date
        if date_from_obj >= today:
            logger.info("[EloverblikStream] Reached current date, no new data available")
            return iter([])
        
        # Cap date_to to current date if it would go beyond
        if date_to_obj > today:
            date_to_obj = today
            logger.info(f"[EloverblikStream] Capping end date to current date: {date_to_obj.strftime('%Y-%m-%d')}")
        
        date_from = date_from_obj.strftime("%Y-%m-%d")
        date_to = date_to_obj.strftime("%Y-%m-%d")
        
        # Fetch time series data
        records = self._get_timeseries_data(date_from, date_to)
        
        logger.info(f"[EloverblikStream] Fetched {len(records)} records for {date_from} to {date_to}")
        
        # Convert records to Rows
        rows = [Row(**r) for r in records]
        
        # Cache for replay
        self._offset_cache[current_date] = rows
        
        return iter(rows)
    
    def commit(self, end: dict) -> None:
        """
        Clean up old cached data.
        
        Args:
            end: The offset that has been committed
        """
        if end is None or "date" not in end:
            return
        
        # Keep only recent batches (prevent memory growth)
        batches_to_keep = 5
        all_dates = sorted(self._offset_cache.keys())
        
        if len(all_dates) > batches_to_keep:
            dates_to_remove = all_dates[:-batches_to_keep]
            for date in dates_to_remove:
                del self._offset_cache[date]
                logger.debug(f"[EloverblikStream] Cleaned up cache for {date}")
    
    def get_latest_offset(self) -> dict:
        """
        Return the latest available offset (current date).
        
        Implementation of abstract method from BaseSimpleDataSourceStreamReader.
        Returns today's date as the latest available offset.
        
        Returns:
            dict: The latest offset as {"date": "YYYY-MM-DD"}
        """
        return {"date": datetime.now().strftime("%Y-%m-%d")}
    
    def cleanup(self) -> None:
        """
        Cleanup resources when the stream stops.
        
        Implementation of cleanup hook from BaseSimpleDataSourceStreamReader.
        Clears caches to free memory.
        """
        logger.info("[EloverblikStream] Cleaning up caches")
        self._offset_cache.clear()
        self._metering_points_cache = None
