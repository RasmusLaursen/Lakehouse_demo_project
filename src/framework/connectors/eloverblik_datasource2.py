"""
Eloverblik-specific DataSource implementation.

Handles the Danish Eloverblik Customer API with OAuth2 authentication
and complex nested JSON response structures. Standalone implementation
for batch and streaming reading with Eloverblik-specific logic.
"""

from typing import Dict, Any, List, Iterator, Union, TYPE_CHECKING, Tuple
import json
import requests
from pyspark.sql import Row
from pyspark.sql.datasource import InputPartition
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, ArrayType

if TYPE_CHECKING:
    from pyspark.sql import SparkSession, DataFrame

from src.framework.connectors.pyspark_datasource_adapter import (
    BasePySparkDataSource,
    BaseDataSourceReader,
)
from src.framework.helper import logging_helper

logger = logging_helper.get_logger(__name__)

class EloverblikDataSource(BasePySparkDataSource):
    """
    Standalone DataSource for Eloverblik Customer API metering points.
    """

    @classmethod
    def name(cls):
        return "eloverblik"

    def schema(self):
        return self.config.get("schema")

    def reader(self, schema: StructType):
        return EloverblikDataSourceReader(schema, self.options)

    def read_batch(self, spark: "SparkSession") -> "DataFrame":
        """
        Read Eloverblik data as a batch DataFrame.
        
        Expects options:
        - token: Access token for Eloverblik API
        - token_url: URL to exchange token (default: https://api.eloverblik.dk/customerapi/api/token)
        - data_url: URL to fetch metering points (default: https://api.eloverblik.dk/customerapi/api/meteringpoints/meteringpoints)
        """
        try:
            spark.dataSource.register(self.__class__)
            logger.debug(f"Registered DataSource: {self.name()}")
        except Exception as e:
            logger.debug(f"DataSource may already be registered: {e}")

        # Get token from config
        token = self.config.get("token") or self.config.get("auth_token")
        if not token:
            raise ValueError("Missing 'token' in configuration")

        # Build reader options
        token_url = self.config.get("token_url", "https://api.eloverblik.dk/customerapi/api/token")
        data_url = self.config.get("data_url", "https://api.eloverblik.dk/customerapi/api/meteringpoints/meteringpoints")

        df = spark.read.format("eloverblik")\
            .option("token", token)\
            .option("token_url", token_url)\
            .option("data_url", data_url)\
            .load()
        
        return df
            



class EloverblikDataSourceReader(BaseDataSourceReader):
    """Reader for Eloverblik DataSource."""

    def __init__(self, schema, options):
        self.schema: StructType = schema
        self.options = options
        # Get URLs from options with defaults
        self.token_url = options.get("token_url")
        self.data_url = options.get("data_url")
        
        if not self.token_url or not self.data_url:
            raise ValueError("Missing 'token_url' or 'data_url' in options")
        
        # Exchange refresh token for access token
        self.token = self._get_token(access_token=options.get("token"))

    def _build_header(self, access_token: str) -> Dict[str, str]:
        """Build authorization header."""
        return {
            "Authorization": f"Bearer {access_token}"
        }
        
    def _get_token(self, access_token: str) -> str:
        """Exchange refresh token for access token."""
        headers = self._build_header(access_token)
        token = requests.get(url=self.token_url, headers=headers)
        token.raise_for_status()
        return token.json()["result"]
    
    def _get_data(self, token: str) -> List[Dict[str, Any]]:
        """Fetch metering points data."""
        headers = self._build_header(token)
        data = requests.get(url=self.data_url, headers=headers)
        data.raise_for_status()
        return data.json()["result"]
    
    def read(self, partition: InputPartition) -> Iterator[Row]:
        """Read data from API and yield rows."""
        response = self._get_data(token=self.token)
        for item in response:
            yield Row(**item)


# class EloverblikDataSource(BasePySparkDataSource):
#     """
#     Standalone DataSource for Eloverblik Customer API.
    
#     Handles:
#     - OAuth2 refresh token authentication
#     - Complex nested JSON response structures (MyEnergyData_MarketDocument)
#     - Field flattening for nested paths (out_Quantity.quantity → quantity)
#     - Batch reading with Eloverblik-specific logic
#     """
    
#     # Class-level schema cache to persist across Spark instantiations
#     _schema_cache: Dict[str, StructType] = {}
    
#     def __init__(self, options: Dict[str, str]) -> None:
#         """Initialize Eloverblik DataSource."""
#         super().__init__(options)
        
#         # If schema is in config (from builder), cache it
#         if "schema" in self.config and isinstance(self.config["schema"], StructType):
#             schema = self.config["schema"]
#             # Use endpoint or table_name as cache key
#             cache_key = self.config.get("endpoint", self.config.get("table_name", "default"))
#             EloverblikDataSource._schema_cache[cache_key] = schema
#             logger.info(f"Cached schema for {cache_key} with {len(schema.fields)} fields")
        
#         logger.info("Initialized EloverblikDataSource")
    
#     @classmethod
#     def name(cls) -> str:
#         """Return the short name for this data source."""
#         return "eloverblik_api"
    
#     def schema(self) -> Union[StructType, str]:
#         """
#         Return schema for the DataFrame.
        
#         Checks cache first, then config for schema from data contract.
#         Falls back to generic schema if neither available.
#         """        
#         # Check if schema is in config (from initial builder instantiation)
#         try:
#             schema = self.config["schema"]
#             logger.info(f"Using schema from config for {self.name()} with {len(schema.fields)} fields")
#             return schema
#         except KeyError:
#             logger.error(f"No schema found in config for {self.name()}")
#             pass
    
    
#     def create_reader(self, schema: StructType) -> "EloverblikReader":
#         """Create a batch reader for Eloverblik API."""
#         return EloverblikReader(self.config, schema)
    
#     def create_stream_reader(self, schema: StructType) -> "EloverblikStreamReader":
#         """Create a streaming reader for Eloverblik API."""
#         return EloverblikStreamReader(self.config, schema)
    
#     def read_batch(self, spark: "SparkSession") -> "DataFrame":
#         """
#         Read data as a batch DataFrame.
        
#         Args:
#             spark: Active SparkSession
            
#         Returns:
#             Batch DataFrame from the Eloverblik API
#         """
#         logger.info(f"read_batch called for {self.name()}")
        
#         # Register this DataSource with Spark
#         try:
#             spark.dataSource.register(self.__class__)
#             logger.debug(f"Registered DataSource: {self.name()}")
#         except Exception as e:
#             logger.debug(f"DataSource may already be registered: {e}")
        
#         # Make API call and fetch data
#         records = self._fetch_all_records()
        
#         # Get schema for DataFrame creation
#         df_schema = self.schema()

#         logger.info(f"Using schema with {(df_schema)} fields for DataFrame creation")
        
#         # Convert records to DataFrame
#         if records:
#             rows = [Row(**record) for record in records]
#             logger.info(f"Created {rows} rows for DataFrame creation")
#             df = spark.createDataFrame(rows, schema=df_schema)
#             logger.info(f"Created DataFrame with {df.count()} rows")
#             return df
#         else:
#             # Return empty DataFrame with schema
#             logger.warning("No records fetched from Eloverblik API")
#             return spark.createDataFrame([], df_schema)
    
#     def read_stream(self, spark: "SparkSession") -> "DataFrame":
#         """
#         Read data as a streaming DataFrame.
        
#         Args:
#             spark: Active SparkSession
            
#         Returns:
#             Streaming DataFrame from the Eloverblik API
#         """
#         logger.info(f"read_stream called for {self.name()}")
        
#         # Register this DataSource with Spark
#         try:
#             spark.dataSource.register(self.__class__)
#             logger.debug(f"Registered DataSource for streaming: {self.name()}")
#         except Exception as e:
#             logger.debug(f"DataSource may already be registered: {e}")
        
#         # Filter config to only include DataSource-relevant options for spark.readStream
#         # Note: We keep 'schema' in self.config but exclude it from readStream options
#         # since the schema() method will be called separately by Spark
#         excluded_keys = {
#             'catalog', 'volume', 'source_system', 
#             'model_name', 'format'
#         }
#         datasource_config = {
#             str(k): str(v) for k, v in self.config.items() 
#             if k not in excluded_keys and not k.endswith('_catalog') and not k.endswith('_schema')
#             and not isinstance(v, StructType) and k != 'schema'
#         }
        
#         # Use Spark's readStream API
#         df = spark.readStream.format(self.name()).options(**datasource_config).load()
#         logger.info(f"Created streaming DataFrame for {self.name()}")
        
#         return df
    
#     def _fetch_all_records(self) -> List[Dict[str, Any]]:
#         """Fetch all records from Eloverblik API."""
#         try:
#             url = self._build_url()
#             headers = self._build_headers()
#             method = self.config.get("method", "GET").upper()
#             timeout = int(self.config.get("timeout", 30))
            
#             logger.info(f"Fetching from Eloverblik API: {url}")
            
#             # Use api_call_with_retry for automatic retry logic with exponential backoff
#             if method == "POST":
#                 body_params = self._get_body_params()
#                 response = api_call_with_retry(
#                     url=url,
#                     method="POST",
#                     headers=headers,
#                     json_body=body_params,
#                     timeout=timeout,
#                     max_retries=3,
#                     retry_delay=2.0,
#                     backoff_factor=2.0
#                 )
#             else:
#                 response = api_call_with_retry(
#                     url=url,
#                     method="GET",
#                     headers=headers,
#                     timeout=timeout,
#                     max_retries=3,
#                     retry_delay=2.0,
#                     backoff_factor=2.0
#                 )
            
#             data = response.json()
            
#             # Extract records from response
#             records = self._extract_records_from_response(data)
#             logger.info(f"Extracted {len(records)} records from Eloverblik API")
            
#             return records
            
#         except Exception as e:
#             logger.error(f"Error fetching from Eloverblik API: {e}")
#             return []
    
#     def _build_url(self) -> str:
#         """Build the full API endpoint URL with path parameter substitution."""
#         endpoint = self.config.get("endpoint", "")
#         table_name = self.config.get("table_name", "")
#         url = f"{endpoint}/{table_name}"
        
#         # Get URL params for path parameter substitution
#         url_params_template = self.config.get("url_params_template", {})
#         if isinstance(url_params_template, str):
#             try:
#                 url_params_template = json.loads(url_params_template)
#             except:
#                 url_params_template = {}
        
#         # Substitute path parameters like {dateFrom}, {dateTo}, {aggregation}
#         if isinstance(url_params_template, dict):
#             for key, value in url_params_template.items():
#                 url = url.replace(f"{{{key}}}", str(value))
        
#         return url
    
#     def _build_headers(self) -> Dict[str, str]:
#         """Build HTTP headers including OAuth2 authentication."""
#         headers = {}
        
#         # Parse custom headers from config
#         custom_headers = self.config.get("headers", {})
#         if isinstance(custom_headers, str):
#             try:
#                 custom_headers = json.loads(custom_headers)
#             except:
#                 custom_headers = {}
#         headers.update(custom_headers)
        
#         # Add OAuth2 authentication
#         auth_type = self.config.get("auth_type", "none").lower()
#         if auth_type == "oauth2_refresh":
#             refresh_token = self.config.get("auth_token")
#             source_system = "eloverblik"
            
#             if refresh_token:
#                 # Get cached access token
#                 access_token = OAuth2TokenManager.get_cached_token(refresh_token, source_system)
                
#                 if not access_token:
#                     # Exchange refresh token for access token
#                     token_endpoint = self.config.get("token_endpoint")
#                     if token_endpoint:
#                         access_token = OAuth2TokenManager.exchange_token(
#                             refresh_token=refresh_token,
#                             token_endpoint=token_endpoint,
#                             source_system=source_system,
#                             token_method=self.config.get("token_method", "GET"),
#                             token_response_path=self.config.get("token_response_path", "result")
#                         )
                
#                 if access_token:
#                     headers["Authorization"] = f"Bearer {access_token}"
        
#         return headers
    
#     def _get_body_params(self) -> Dict[str, Any]:
#         """Get body parameters for POST requests."""
#         body_params = self.config.get("body_params_template", "{}")
#         if isinstance(body_params, str):
#             try:
#                 body_params = json.loads(body_params)
#             except:
#                 body_params = {}
#         return body_params if isinstance(body_params, dict) else {}
    
#     def _extract_records_from_response(self, response: Dict[str, Any]) -> List[Dict[str, Any]]:
#         """
#         Extract records from Eloverblik API response.
        
#         Eloverblik structure can be:
#         1. Simple result array: {"result": [...]} for MeteringPoints
#         2. Nested time series: MyEnergyData_MarketDocument.TimeSeries[].Period[].Point[]
#         """
#         records = []
        
#         try:
#             # Check for simple result array (MeteringPoints)
#             if "result" in response and isinstance(response["result"], list):
#                 return response["result"]
            
#             # Handle nested time series structure (TimeSeriesData)
#             market_doc = response.get("MyEnergyData_MarketDocument", {})
#             if not market_doc:
#                 logger.warning("No result or MyEnergyData_MarketDocument found in response")
#                 return []
            
#             time_series_list = market_doc.get("TimeSeries", [])
#             if not isinstance(time_series_list, list):
#                 time_series_list = [time_series_list] if time_series_list else []
            
#             for time_series in time_series_list:
#                 periods = time_series.get("Period", [])
#                 if not isinstance(periods, list):
#                     periods = [periods] if periods else []
                
#                 for period in periods:
#                     points = period.get("Point", [])
#                     if not isinstance(points, list):
#                         points = [points] if points else []
                    
#                     for point in points:
#                         # Flatten nested structure
#                         record = self._flatten_point(point)
                        
#                         # Add period context
#                         record["period_start"] = period.get("timeInterval", {}).get("start")
#                         record["period_end"] = period.get("timeInterval", {}).get("end")
#                         record["resolution"] = period.get("resolution")
                        
#                         # Add time series context
#                         record["mrid"] = time_series.get("mRID")
#                         record["measurement_unit"] = time_series.get("measurement_Unit.name")
                        
#                         # Add document context
#                         record["document_id"] = market_doc.get("mRID")
#                         record["document_type"] = market_doc.get("type")
#                         record["created"] = market_doc.get("createdDateTime")
                        
#                         records.append(record)
        
#         except Exception as e:
#             logger.error(f"Error extracting records from Eloverblik response: {e}")
        
#         return records
    
#     def _flatten_point(self, point: Dict[str, Any]) -> Dict[str, Any]:
#         """
#         Flatten nested Point structure.
        
#         Eloverblik has fields like:
#         - out_Quantity.quantity → quantity
#         - out_Quantity.quality → quality
#         """
#         flattened = {}
        
#         for key, value in point.items():
#             if isinstance(value, dict):
#                 # Flatten nested dict (e.g., out_Quantity)
#                 for nested_key, nested_value in value.items():
#                     # Use just the nested key as the field name
#                     flattened[nested_key] = nested_value
#             else:
#                 flattened[key] = value
        
#         return flattened


# class EloverblikReader(BaseDataSourceReader):
#     """Reader for Eloverblik DataSource."""
    
#     def create_partitions(self) -> List[SimpleInputPartition]:
#         """Create a single partition for batch reading."""
#         return [SimpleInputPartition(0)]
    
#     def read_partition(self, partition: SimpleInputPartition) -> Iterator[Row]:
#         """
#         Read data from a partition.
        
#         Note: Actual data reading is handled by read_batch() method.
#         This is here to satisfy the DataSourceReader interface.
#         """
#         # Return empty iterator - actual reading happens in read_batch()
#         return iter([])


# class EloverblikStreamReader(BaseDataSourceStreamReader):
#     """Streaming reader for Eloverblik DataSource with timestamp-based incremental loading."""
    
#     def get_initial_offset(self) -> dict:
#         """
#         Return the initial offset for streaming.
        
#         Uses initial_timestamp from config or defaults to current time.
#         """
#         initial_timestamp = self.config.get("initial_timestamp", datetime.utcnow().isoformat())
#         return {"timestamp": initial_timestamp}
    
#     def get_latest_offset(self) -> dict:
#         """
#         Return the latest available offset.
        
#         For time-series data, returns current timestamp.
#         """
#         return {"timestamp": datetime.utcnow().isoformat()}
    
#     def create_stream_partitions(self, start: dict, end: dict) -> Sequence[InputPartition]:
#         """
#         Create partitions for the given offset range.
        
#         For Eloverblik, we create a single partition per time range.
#         """
#         return [SimpleInputPartition(0)]
    
#     def read_stream_partition(self, partition: InputPartition) -> Iterator[Row]:
#         """
#         Read data from a streaming partition.
        
#         This is called for each micro-batch with the current offset range.
#         """
#         logger.info("Reading streaming partition from Eloverblik API")
        
#         # try:
#         #     # Fetch records for current offset range
#         #     url = self._build_url()
#         #     headers = self._build_headers()
#         #     method = self.config.get("method", "GET").upper()
#         #     timeout = int(self.config.get("timeout", 30))
            
#         #     logger.info(f"Streaming fetch from Eloverblik API: {url}")
            
#         #     if method == "POST":
#         #         body_params = self._get_body_params()
#         #         response = requests.post(url, headers=headers, json=body_params, timeout=timeout)
#         #     else:
#         #         response = requests.get(url, headers=headers, timeout=timeout)
            
#         #     response.raise_for_status()
#         #     data = response.json()
            
#         #     # Extract records from response
#         #     records = self._extract_records_from_response(data)
#         #     logger.info(f"Extracted {len(records)} records from Eloverblik API stream")
            
#         #     # Yield rows
#         #     for record in records:
#         #         yield Row(**record)
                
#         # except Exception as e:
#         #     logger.error(f"Error reading streaming partition from Eloverblik API: {e}")
#         #     # Return empty iterator on error
#         #     return iter([])
    
#     def _build_url(self) -> str:
#         """Build the full API endpoint URL with parameter substitution."""
#         endpoint = self.config.get("endpoint", "")
#         table_name = self.config.get("table_name", "")
#         url = f"{endpoint}/{table_name}"
        
#         # Get URL params for path parameter substitution
#         url_params_template = self.config.get("url_params_template", {})
#         if isinstance(url_params_template, str):
#             try:
#                 url_params_template = json.loads(url_params_template)
#             except:
#                 url_params_template = {}
        
#         # Substitute path parameters
#         if isinstance(url_params_template, dict):
#             for key, value in url_params_template.items():
#                 url = url.replace(f"{{{key}}}", str(value))
        
#         return url
    
#     def _build_headers(self) -> Dict[str, str]:
#         """Build HTTP headers including OAuth2 authentication."""
#         headers = {}
        
#         # Parse custom headers
#         custom_headers = self.config.get("headers", {})
#         if isinstance(custom_headers, str):
#             try:
#                 custom_headers = json.loads(custom_headers)
#             except:
#                 custom_headers = {}
#         headers.update(custom_headers)
        
#         # Add OAuth2 authentication
#         auth_type = self.config.get("auth_type", "none").lower()
#         if auth_type == "oauth2_refresh":
#             refresh_token = self.config.get("auth_token")
#             source_system = "eloverblik"
            
#             if refresh_token:
#                 access_token = OAuth2TokenManager.get_cached_token(refresh_token, source_system)
                
#                 if not access_token:
#                     token_endpoint = self.config.get("token_endpoint")
#                     if token_endpoint:
#                         access_token = OAuth2TokenManager.exchange_token(
#                             refresh_token=refresh_token,
#                             token_endpoint=token_endpoint,
#                             source_system=source_system,
#                             token_method=self.config.get("token_method", "GET"),
#                             token_response_path=self.config.get("token_response_path", "result")
#                         )
                
#                 if access_token:
#                     headers["Authorization"] = f"Bearer {access_token}"
        
#         return headers
    
#     def _get_body_params(self) -> Dict[str, Any]:
#         """Get body parameters for POST requests."""
#         body_params = self.config.get("body_params_template", "{}")
#         if isinstance(body_params, str):
#             try:
#                 body_params = json.loads(body_params)
#             except:
#                 body_params = {}
#         return body_params if isinstance(body_params, dict) else {}
    
#     def _extract_records_from_response(self, response: Dict[str, Any]) -> List[Dict[str, Any]]:
#         """Extract records from Eloverblik API response."""
#         records = []
        
#         try:
#             # Check for simple result array (MeteringPoints)
#             if "result" in response and isinstance(response["result"], list):
#                 return response["result"]
            
#             # Handle nested time series structure
#             market_doc = response.get("MyEnergyData_MarketDocument", {})
#             if not market_doc:
#                 return []
            
#             time_series_list = market_doc.get("TimeSeries", [])
#             if not isinstance(time_series_list, list):
#                 time_series_list = [time_series_list] if time_series_list else []
            
#             for time_series in time_series_list:
#                 periods = time_series.get("Period", [])
#                 if not isinstance(periods, list):
#                     periods = [periods] if periods else []
                
#                 for period in periods:
#                     points = period.get("Point", [])
#                     if not isinstance(points, list):
#                         points = [points] if points else []
                    
#                     for point in points:
#                         record = self._flatten_point(point)
#                         record["period_start"] = period.get("timeInterval", {}).get("start")
#                         record["period_end"] = period.get("timeInterval", {}).get("end")
#                         record["resolution"] = period.get("resolution")
#                         record["mrid"] = time_series.get("mRID")
#                         record["measurement_unit"] = time_series.get("measurement_Unit.name")
#                         record["document_id"] = market_doc.get("mRID")
#                         record["document_type"] = market_doc.get("type")
#                         record["created"] = market_doc.get("createdDateTime")
#                         records.append(record)
        
#         except Exception as e:
#             logger.error(f"Error extracting records from Eloverblik response: {e}")
        
#         return records
    
#     def _flatten_point(self, point: Dict[str, Any]) -> Dict[str, Any]:
#         """Flatten nested Point structure."""
#         flattened = {}
        
#         for key, value in point.items():
#             if isinstance(value, dict):
#                 for nested_key, nested_value in value.items():
#                     flattened[nested_key] = nested_value
#             else:
#                 flattened[key] = value
        
#         return flattened
