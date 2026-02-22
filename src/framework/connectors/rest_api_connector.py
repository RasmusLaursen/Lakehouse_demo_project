"""
Eloverblik API connector following framework patterns.

Implements batch and streaming reading from Danish Eloverblik Customer API
using the framework's BasePySparkDataSource adapter and APIClient utilities.
"""

from typing import Dict, Any, List, Iterator, Tuple, Optional
from datetime import datetime, timedelta, timezone
import json
import time
import requests
from pyspark.sql import Row, SparkSession, DataFrame
from pyspark.sql.datasource import InputPartition
from pyspark.sql.types import IntegerType, StructType, StructField, TimestampType
import os
os.environ["DATABRICKS_ENABLE_EXPERIMENTAL_FILES_API_CLIENT"] = "True" # set to true to enable new file features in sdk 
from databricks import sdk
from pyspark.sql.datasource import DataSource, DataSourceStreamReader, DataSourceReader, SimpleDataSourceStreamReader

from src.framework.connectors.api_helper import APIClient, BearerTokenAuth, NoAuth
from src.framework.connectors.json_response_extractor import JSONResponseExtractor
from src.framework.helper import logging_helper

logger = logging_helper.get_logger(__name__)

class RESTAPIDataSource(DataSource):
    """
    DataSource for Eloverblik Customer API with OAuth2 authentication.
    
    Supports both batch reading (metering points) and streaming reading (time series data).
    """

    def __init__(self, options: Dict[str, Any]):
        self.config = dict(options)  # Convert to regular dict
    
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
        
        Automatically adds _load_timestamp field if not present.
        """
        schema = self.config.get("schema")
        
        # If schema is already a StructType, return it
        if isinstance(schema, StructType):
            result_schema = schema
        # If schema is a JSON string, parse it
        elif isinstance(schema, str):
            try:
                schema_dict = json.loads(schema)
                result_schema = StructType.fromJson(schema_dict)
            except Exception as e:
                logger.error(f"Failed to parse schema JSON: {e}")
                raise ValueError(f"Invalid schema JSON: {e}")
        else:
            # Schema not provided
            raise ValueError("Schema must be provided in options as StructType or JSON string")
        
        # Add _load_timestamp field if not already present
        field_names = [f.name for f in result_schema.fields]
        if "_load_timestamp" not in field_names:
            result_schema = StructType(
                result_schema.fields + [StructField("_load_timestamp", TimestampType(), nullable=False)]
            )
            logger.debug("Added _load_timestamp field to schema")
        if "_partition_id" not in field_names:
            result_schema = StructType(
                result_schema.fields + [StructField("_partition_id", IntegerType(), nullable=False)]
            )
            logger.debug("Added _partition_id field to schema")            
        
        return result_schema
    
    def create_reader(self, schema: StructType) -> "RESTAPIBatchReader":
        """Create a batch reader for metering points."""
        return RESTAPIBatchReader(self.config, schema)
    
    def create_stream_reader(self, schema: StructType) -> "RESTAPIStreamReaderSimple":
        """Create a streaming reader for time series data."""
        return RESTAPIStreamReaderSimple(self.config, schema)
    
    def reader(self, schema: StructType):
        """Return batch reader."""
        return self.create_reader(schema)
    
    def streamReader(self, schema: StructType):
        """Return streaming reader."""
        return RESTAPIStreamReaderSimple(self.config, schema)
    
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
                # Serialize dicts/lists as JSON so they survive Spark options
                if isinstance(v, (dict, list)):
                    datasource_config[str(k)] = json.dumps(v)
                else:
                    datasource_config[str(k)] = str(v)

        # Use Spark's read API
        logger.info(f"running with self.config {self.config}")
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

        logger.info(f"utilizing the following config  {self.config}")

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
                # Serialize dicts/lists as JSON so they survive Spark options
                if isinstance(v, (dict, list)):
                    datasource_config[str(k)] = json.dumps(v)
                else:
                    datasource_config[str(k)] = str(v)

        logger.info(f"Using datasource_config for streaming: {self.config.get('endpoint')}, {self.config.get('table_name')}, {self.config.get('url_params_template')} ")
        logger.info(f"DataSource config keys for streaming: {list(datasource_config.keys())}")
        
        # Use Spark's readStream API
        df = spark.readStream.format(self.name()).options(**datasource_config).load()
        logger.info(f"Created streaming DataFrame for {self.name()}")
        
        return df    

class RESTAPIBatchReader(DataSourceReader):
    """
    Batch reader for Eloverblik metering points.
    
    Fetches list of metering points from the Customer API using OAuth2 authentication.
    """
    
    def __init__(self, config: Dict[str, Any], schema: StructType):
        """Initialize batch reader."""
        # super().__init__(config, schema)
        self.config = config
        self.schema_struct = schema
        self._setup_api_client()
    
    def _setup_api_client(self) -> None:
        """Initialize API client with OAuth2 authentication."""
        # Get access token by exchanging refresh token
        self.endpoint = self.config["endpoint"]
        self.target_endpoint = self.config["target_endpoint"]
        
        # Exchange refresh token for access token
        if self.config.get("auth_type") == "oauth2_refresh":
            self.token_endpoint = self.config["token_endpoint"]            
            refresh_token = self.config["auth_token"]            
            auth = BearerTokenAuth(refresh_token)

            token_client = APIClient(
                authenticator=auth,
                max_retries=3,
                retry_delay=1
            )            
        elif self.config.get("auth_type") == "none":
            auth = NoAuth()
        else:
            raise ValueError(f"Unsupported auth_type: {self.config.get('auth_type')}")
        
        try:
           
            logger.info("Successfully obtained Eloverblik access token")
            
            # Exchange refresh token for access token
            if self.config.get("auth_type") == "oauth2_refresh":
                token_response = token_client.get(self.token_endpoint)
                access_token = token_response.json().get("result")
                
                if not access_token:
                    raise ValueError("Failed to obtain access token from Eloverblik API")                
                auth = BearerTokenAuth(access_token)
            elif self.config.get("auth_type") == "none":
                auth = NoAuth()
            else:
                raise ValueError(f"Unsupported auth_type: {self.config.get('auth_type')}")
        
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

    def _build_extractor(self) -> JSONResponseExtractor:
        """Build a JSONResponseExtractor from config."""
        data_path = self.config.get("data_path", "result")
        field_mapping = self.config.get("field_mapping")
        
        # field_mapping may arrive as JSON string from Spark options
        if isinstance(field_mapping, str):
            try:
                field_mapping = json.loads(field_mapping)
            except (json.JSONDecodeError, TypeError):
                field_mapping = None
        
        # Get column names from schema so only contract-defined fields survive
        schema_fields = [f.name for f in self.schema_struct.fields] if self.schema_struct else None
        
        # Get array_field_mapping if provided (for APIs like OpenSky that return arrays)
        array_field_mapping = self.config.get("array_field_mapping")
        if isinstance(array_field_mapping, str):
            try:
                array_field_mapping = json.loads(array_field_mapping)
            except (json.JSONDecodeError, TypeError):
                array_field_mapping = None
        
        return JSONResponseExtractor(
            data_path=data_path,
            field_mapping=field_mapping,
            store_raw="_raw_json" in (schema_fields or []),
            schema_fields=schema_fields,
            array_to_object_mapping=array_field_mapping,
        )

    def read_partition(self, partition: InputPartition) -> Iterator[Row]:
        """
        Fetch data from API and extract using JSONResponseExtractor.
        
        Args:
            partition: Input partition (single partition)
            
        Yields:
            Row objects with extracted data
        """
        # Capture load timestamp at the start of batch read
        load_timestamp = datetime.now()
        
        try:
            logger.info(f"Fetching data from Eloverblik API: {self.target_endpoint}")
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
            
            # Use generic extractor driven by data_path from config
            extractor = self._build_extractor()
            records = extractor.extract(data)
            
            logger.info(f"Extracted {len(records)} records from {self.target_endpoint}")
            
            for rec in records:
                # Update _load_timestamp in the record dict
                rec['_load_timestamp'] = load_timestamp
                yield Row(**rec)
                
        except Exception as e:
            logger.error(f"Error fetching {self.target_endpoint}: {e}")
            raise

class TimeRangePartition(InputPartition):
    def __init__(self, pid, url):
        self.pid = pid
        self.url = url

class RESTAPIStreamReaderSimple(DataSourceStreamReader):
    """
    Streaming reader for Eloverblik time series data with date-based incremental loading.
    
    Uses BaseSimpleDataSourceStreamReader pattern for date-based offset management.
    Fetches metering points once and caches them for subsequent batches.
    """
    def __init__(self, config: Dict[str, Any], schema: StructType):
        """Initialize streaming reader."""
        self.config = config
        self.schema_struct = schema

        # Volume path for saving logs (e.g., /Volumes/catalog/schema/volume_name/logs/)
        # self.log_volume_path = config.get("log_volume_path", "/Volumes/raw_dev/dev_rasmuslaursen_eloverblik/eloverblik_log/logs")

        # self.databricks_token = config.get("databricks_token")
        
        # # Generate log file name from source system and table
        # source_system = config.get("source_system", "eloverblik")
        # table_name = config.get("table_name", "unknown").replace("/", "_")
        # timestamp = time.strftime("%Y%m%d_%H%M%S")

        self._setup_api_client()

        if self.config.get("initial_timestamp"):
            initial_ts = self.config.get("initial_timestamp")
            # Parse initial_timestamp as datetime if it's a string
            if isinstance(initial_ts, str):
                self.initial_timestamp = self._parse_dt_utc(initial_ts)
            else:
                self.initial_timestamp = initial_ts
        else:
            raise ValueError("initial_timestamp must be provided in config for streaming")
        
        # URL template for time series endpoint
        if self.config.get("endpoint"):
            self.endpoint = self.config.get("endpoint") 
        else:
            raise ValueError("Endpoint URL must be provided in config for streaming")   

        # URL template for time series endpoint
        if self.config.get("method"):
            self.method = self.config.get("method").upper()
        else: 
            raise ValueError("HTTP method must be provided in config for streaming (e.g., GET or POST)")

        if self.config.get("dependency_table"):
            self.dependency_table = self.config.get("dependency_table")
            self.dependency_url = self._dependency_url()
            self._body_params = self._get_dependency_url()
        
        if self.config.get("max_pr_partition"):
            self.max_pr_partition = int(self.config.get("max_pr_partition"))
        else:
            logger.info("max_pr_partition not provided in config, defaulting to 1")
            self.max_pr_partition = 1

        if self.config.get("max_pr_partition_code"):
            self.max_pr_partition_code = str(self.config.get("max_pr_partition_code")).lower()
        else:
            logger.info("max_pr_partition_code not provided in config, defaulting to 'months'")
            self.max_pr_partition_code = "months"

        if self.config.get("timestamp_format_code"):        
            self.timestamp_format_code = str(self.config.get("timestamp_format_code")).lower()
        else:
            logger.info("timestamp_format_code not provided in config, defaulting to 'days'")
            self.timestamp_format_code = "days"          

        self.timestamp_format = self._format_timestamp()       

        logger.info(f"Initialized EloverblikStreamReader: initial_timestamp={self.initial_timestamp}")

    # def _save_logs_to_volume2(self, offset) -> None:
    #     """Save executor logs to Databricks volume using Files API."""      
    #     import json
    #     import io
        
    #     # Construct full file path
    #     # file_path = f"/Volumes/raw_dev/dev_rasmuslaursen_eloverblik/eloverblik_log/logs/test.json"
    #     file_path = self.log_volume_path + "/" + self.log_file_name
        
    #     # Prepare log payload
    #     log_payload = {
    #         "source_system": self.config.get("source_system", "unknown"),
    #         "table_name": self.config.get("table_name", "unknown"),
    #         "execution_timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
    #         "logs": self.executor_logs,
    #         "current": offset
    #     }

    #     f = io.BytesIO(json.dumps(log_payload).encode('utf-8'))
        
    #     # Create workspace client lazily to avoid module-import serialization issues
    #     workspace_client = sdk.WorkspaceClient(
    #         host = 'https://dbc-e3d11267-f1e6.cloud.databricks.com/',
    #         token = '<redacted-databricks-token>'
    #     )    
    #     workspace_client.files.upload(file_path=file_path, contents=f, overwrite=True)           
    
    def _setup_api_client(self) -> None:
        """Initialize API client with OAuth2 authentication."""
        self.target_endpoint = self.config["target_endpoint"]        
        
        # Exchange refresh token for access token
        try:
            if self.config.get("auth_type") == "oauth2_refresh":
                self.token_endpoint = self.config["token_endpoint"]
                refresh_token = self.config["auth_token"]
                auth = BearerTokenAuth(refresh_token)
                token_client = APIClient(
                    authenticator=auth,
                    max_retries=3,
                    retry_delay=1
                )                
                token_response = token_client.get(self.token_endpoint)
                access_token = token_response.json().get("result")
                
                if not access_token:
                    raise ValueError("Failed to obtain access token from Eloverblik API")
                
                logger.info("Successfully obtained Eloverblik access token for streaming")
                
                # Store access token for API calls
                self.access_token = access_token
            elif self.config.get("auth_type") == "none":
                auth = NoAuth()
            else:
                raise ValueError(f"Unsupported auth_type: {self.config.get('auth_type')}")                    
            
        except Exception as e:
            logger.error(f"Failed to setup Eloverblik API client for streaming: {e}")
            raise
    
    def _get_dependency_url(self) -> List[str]:
        """
        Fetch metering points from API with caching.
        
        Fetches metering points once and caches the result for subsequent calls.
        
        Returns:
            List of metering point IDs
        """       
        # Fetch from API
        logger.info("Fetching metering points from API (first time)")

        # Exchange refresh token for access token
        if self.config.get("auth_type") == "oauth2_refresh":
            auth = BearerTokenAuth(self.access_token)
        elif self.config.get("auth_type") == "none":
            auth = NoAuth()
        else:
            raise ValueError(f"Unsupported auth_type: {self.config.get('auth_type')}")        
                
        api_client = APIClient(
            authenticator=auth,
            max_retries=3,
            retry_delay=60
        )

        logger.info(f"Using dependency URL to fetch metering points: {self.dependency_url}")
        
        try:
            response = api_client.get(self.dependency_url)
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

    def _dependency_url(self) -> Optional[str]:
        """Get the API URL for fetching dependencies if needed."""
        dependency_table = str(self.endpoint) + str(self.dependency_table)
        if str(self.dependency_table) == "None" or not self.dependency_table:
            return None
        logger.debug(f"Using dependency URL: {dependency_table}")
        return dependency_table
    
    def _resolve_template(self, template: Any, values: List[str]) -> Any:
        """
        Recursively walk *template* and replace the placeholder
        string ``"body_params"`` with *values* (the actual list).

        Works regardless of nesting depth so the YAML template
        can have any shape.
        """
        if isinstance(template, str):
            if template == "body_params":
                return values
            return template
        if isinstance(template, dict):
            return {k: self._resolve_template(v, values) for k, v in template.items()}
        if isinstance(template, list):
            return [self._resolve_template(item, values) for item in template]
        return template
    
    def _build_extractor(self) -> JSONResponseExtractor:
        """Build a JSONResponseExtractor from config."""
        data_path = self.config.get("data_path", "result")
        field_mapping = self.config.get("field_mapping")
        
        # field_mapping may arrive as JSON string from Spark options
        if isinstance(field_mapping, str):
            try:
                field_mapping = json.loads(field_mapping)
            except (json.JSONDecodeError, TypeError):
                field_mapping = None
        
        # Get column names from schema so only contract-defined fields survive
        schema_fields = [f.name for f in self.schema_struct.fields] if self.schema_struct else None
        
        # Get array_field_mapping if provided (for APIs like OpenSky that return arrays)
        array_field_mapping = self.config.get("array_field_mapping")
        if isinstance(array_field_mapping, str):
            try:
                array_field_mapping = json.loads(array_field_mapping)
            except (json.JSONDecodeError, TypeError):
                array_field_mapping = None
        
        return JSONResponseExtractor(
            data_path=data_path,
            field_mapping=field_mapping,
            store_raw="_raw_json" in (schema_fields or []),
            schema_fields=schema_fields,
            array_to_object_mapping=array_field_mapping,
        )

    def _build_url(self, **overrides) -> str:
        """
        Build the API URL by merging url_params_template defaults
        with runtime overrides, then formatting the target_endpoint.

        Handles both path parameters (e.g., {dateFrom}) and query parameters.

        Args:
            **overrides: Runtime values that take precedence over template defaults
                         (e.g. dateFrom, dateTo)

        Returns:
            Fully formatted URL string
        """
        from urllib.parse import urlencode
        
        url_params_template = self.config.get("url_params_template", {})

        # Deserialize if it arrived as JSON string from Spark options
        if isinstance(url_params_template, str):
            url_params_template = json.loads(url_params_template)

        # Merge: template defaults ← runtime overrides
        params = {**url_params_template, **overrides}

        # Check if target_endpoint has path parameters (curly braces)
        if '{' in self.target_endpoint and '}' in self.target_endpoint:
            # Path parameters: format directly into URL path
            url = self.target_endpoint.format(**params)
        else:
            # Query parameters: append as query string
            if params:
                query_string = urlencode(params)
                url = f"{self.target_endpoint}?{query_string}"
            else:
                url = self.target_endpoint
        
        logger.debug(f"Built URL: {url}")
        return url

    def _get_url_data(self, url: str) -> List[Dict[str, Any]]:
        """
        Fetch time series data for a date range.
        
        Uses JSONResponseExtractor with data_path from config to navigate
        and explode the nested response structure automatically.
        
        Args:
            date_from: Start date (YYYY-MM-DD)
            date_to: End date (YYYY-MM-DD)
            
        Returns:
            List of extracted records
        """             
        # Exchange refresh token for access token
        if self.config.get("auth_type") == "oauth2_refresh":
            auth = BearerTokenAuth(self.access_token)
        elif self.config.get("auth_type") == "none":
            auth = NoAuth()
        else:
            raise ValueError(f"Unsupported auth_type: {self.config.get('auth_type')}")        
                
        api_client = APIClient(
            authenticator=auth,
            max_retries=1,
            retry_delay=1
        )        
        
        try:
            if self.method == "GET":
                response = api_client.get(url)
            elif self.method == "POST":         
                body_params = self._body_params
        
                if not body_params:
                    logger.error("No metering points available")
                    return []

                body_params_template = self.config.get("body_params_template")
                if body_params_template:
                    # Template arrives as JSON string from Spark options or dict from config
                    if isinstance(body_params_template, str):
                        body_params_template = json.loads(body_params_template)
                    # Recursively replace "body_params" placeholder with actual list
                    body = self._resolve_template(body_params_template, body_params)
                else:    
                    raise ValueError("body_params_template must be provided in config")
                response = api_client.post(url, json_body=body)

            else:
                raise ValueError(f"Unsupported HTTP method: {self.method}")

            # Use generic extractor driven by data_path from config
            extractor = self._build_extractor()
            response_json = response.json()
            
            # Handle empty response
            if not response_json:
                return []
            
            records = extractor.extract(response_json)
            
            return records
            
        except Exception as e:
            logger.error(f"Error fetching time series data: {e}")

    def partitions(self, start: dict, end: dict):
        logger.info("*****Partitions start*****")

        start = self._parse_dt_utc(start["offset"])
        end = self._parse_dt_utc(end["offset"])

        logger.info(f"start: {start}")
        logger.info(f"end: {end}")

        total_seconds = (end - start).total_seconds()

        seconds_per_partition = self._extrapolate_seconds_per_partition()

        import math

        num_partitions = math.ceil(total_seconds / seconds_per_partition)
        num_partitions = min(num_partitions, self.max_pr_partition)
        logger.info(f"Calculated num_partitions: {num_partitions} with seconds_per_partition: {seconds_per_partition} and total_seconds: {total_seconds}")
        if num_partitions <= 0:
            logger.info(f"No partitions to read with start: {start}, end: {end}, num_partitions: {num_partitions}, max_pr_partition: {self.max_pr_partition}, seconds_per_partition: {seconds_per_partition}")
            return []

        parts = []
        for i in range(num_partitions):
            partition_start = start + timedelta(seconds=seconds_per_partition * i)
            partition_end = start + timedelta(seconds=seconds_per_partition * (i + 1))
            if partition_end > end:
                partition_end = end
            logger.info(f"Adding Partition {i}: {self._to_usgs_iso(partition_start)} to {self._to_usgs_iso(partition_end)}")

            url = self._build_url(dateFrom=self._to_usgs_iso(partition_start), dateTo=self._to_usgs_iso(partition_end))
            logger.info(f"Built URL for partition {i}: {url}")
            parts.append(
                TimeRangePartition(
                    # start=self._to_usgs_iso(partition_start),
                    # end=self._to_usgs_iso(partition_end),
                    pid=i,
                    url=url
                )
            )
        logger.info("*****Partitions end*****")
        return parts

    def _parse_dt_utc(self, s: str) -> datetime:
        # Accept 'YYYY-MM-DD' (UTC midnight) or ISO8601.
        if len(s) == 10 and s[4] == "-" and s[7] == "-":
            return datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=timezone.utc)

        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)

    def _to_usgs_iso(self, dt: datetime) -> str:
        # USGS accepts this well, plus we pass timezone=UTC.
        return dt.strftime(self.timestamp_format)
    
    def _format_timestamp(self) -> str:
        if self.timestamp_format_code == "minutes":
            timestamp_format = "%Y-%m-%d %H:%M"
        elif self.timestamp_format_code == "hours":
            timestamp_format = "%Y-%m-%d %H"
        elif self.timestamp_format_code == "days":
            timestamp_format = "%Y-%m-%d"
        elif self.timestamp_format_code == "months":
            timestamp_format = "%Y-%m"
        elif self.timestamp_format_code == "years":
            timestamp_format = "%Y"
        else:
            timestamp_format = "%Y-%m-%d %H:%M:%S"
        return timestamp_format
    
    def _extrapolate_seconds_per_partition(self) -> int:
        if self.max_pr_partition_code == "years":
            return 365 * 24 * 60 * 60
        elif self.max_pr_partition_code == "months":
            return 30 * 24 * 60 * 60
        elif self.max_pr_partition_code == "days":
            return 24 * 60 * 60
        elif self.max_pr_partition_code == "hours":
            return 60 * 60
        elif self.max_pr_partition_code == "minutes":
            return 60
        elif self.max_pr_partition_code == "seconds":
            return 1
        else:
            raise ValueError(f"Unsupported max_pr_partition_code: {self.max_pr_partition_code}")    
    
    def initialOffset(self) -> dict:
        """Return the initial offset (starting date)."""
        initial_offset = self.initial_timestamp.strftime(self.timestamp_format)
        
        logger.info(f"Returning initial offset: {initial_offset} with timestamp format: {self.timestamp_format}")
        return {"offset": initial_offset}
        
    def read(self, partition: TimeRangePartition) -> Iterator[Row]:
        """Fetch data for the partition's offset range."""
        records = self._get_url_data(partition.url)
        
        if not records:
            return iter([])

        #Update _load_timestamp in each record
        for r in records:
            r['_load_timestamp'] = datetime.now()
            r['_partition_id'] = partition.pid
        
        return iter([Row(**r) for r in records])

    def latestOffset(self) -> dict:
        """
        Return the latest available offset.
        
        This calculates the end of the NEXT batch to process.
        It's called by Spark to determine if new data is available.
        
        Returns:
            dict: End date of next batch as {"date": "YYYY-MM-DD"}
        """
        # If _next_offset is already set, return it (idempotent within same micro-batch)
        latest =  (time.strftime(self.timestamp_format, time.localtime(time.time() )))
        logger.info("[get_latest_offset] Returning latest offset: %s", latest)
        
        return {"offset": latest}
    
    def commit(self, end: dict) -> None:
        """
        Commit processed offset and save executor logs.
        
        Called by PySpark after a batch is successfully processed.
        This is the ideal place to save logs once per batch.
        
        Args:
            end: The offset that has been processed
        """
        logger.info(f"Committed offset: {end['offset']}")
        # self._save_logs_to_volume2(offset = end['offset'])