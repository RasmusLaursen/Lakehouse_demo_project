"""
REST API DataSource using PySpark DataSource API (Spark 4.0+).

This module provides a native Spark DataSource implementation for reading
from REST APIs with built-in pagination, authentication, and rate limiting.
"""

from typing import Dict, Any, Union, Iterator, Sequence, List, Optional, TYPE_CHECKING
import json
import ast
import datetime
import time
import requests
import datetime
from pyspark.sql import Row
from pyspark.sql.datasource import InputPartition
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    DoubleType,
    BooleanType,
    TimestampType,
)

if TYPE_CHECKING:
    from pyspark.sql import SparkSession, DataFrame

from src.framework.connectors.pyspark_datasource_adapter import (
    BasePySparkDataSource,
    BaseDataSourceReader,
    BaseDataSourceStreamReader,
    SimpleInputPartition,
)
from src.framework.connectors.partition_strategies import PageInputPartition, OffsetInputPartition
from src.framework.helper import logging_helper

logger = logging_helper.get_logger(__name__)

def flatten_json(nested, parent_key="", sep="."):
    """
    Basic recursive flatten of a JSON object (dict) into a one-level dict.
    E.g. {"a": {"b": 123, "c": 456}} -> {"a.b": 123, "a.c": 456}
    Arrays (lists) remain as raw JSON strings.
    """
    items = []
    if isinstance(nested, dict):
        for k, v in nested.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(flatten_json(v, new_key, sep=sep).items())
            elif isinstance(v, list):
                items.append((new_key, json.dumps(v)))
            else:
                items.append((new_key, v))
    elif isinstance(nested, list):
        items.append((parent_key, json.dumps(nested)))
    else:
        items.append((parent_key, nested))
    return dict(items)

def get_nested_value(data, json_path):
    """
    Extracts a nested value from data following a path like "data.items".
    If any level is missing, returns None.
    """
    if not json_path:
        return data
    keys = json_path.split(".")
    for key in keys:
        if not isinstance(data, dict):
            return None
        data = data.get(key)
        if data is None:
            return None
    return data

def infer_spark_type(value):
    """
    Infer Spark DataType from a given Python value.
    For strings, we try to detect an ISO-formatted datetime.
    For lists or dicts, we fallback to StringType since these are flattened to JSON strings.
    """
    if value is None:
        return StringType()
    if isinstance(value, bool):
        return BooleanType()
    if isinstance(value, int):
        return LongType()
    if isinstance(value, float):
        return DoubleType()
    if isinstance(value, str):
        try:
            # Attempt to parse ISO formatted datetime
            datetime.datetime.fromisoformat(value)
            return TimestampType()
        except ValueError:
            return StringType()
    return StringType()

def convert_value_to_type(value, spark_type):
    return str(value)


class RestApiDataSource(BasePySparkDataSource):
    """
    PySpark DataSource for REST APIs.
    
    Provides native Spark integration for reading from REST endpoints with:
    - Automatic pagination (offset, cursor, page-based)
    - Authentication (Bearer, API Key, OAuth, Basic)
    - Rate limiting
    - Parallel requests via partitioning
    - Incremental loading via streaming
    """
    
    # Default to streaming for incremental loading (override with mode='batch' in config)
    prefer_batch = False
    
    def __init__(self, options: Dict[str, str]) -> None:
        """Initialize REST API DataSource and validate authentication.
        
        Args:
            options: Configuration options for the data source
            
        Raises:RestApiDataSourceReader._access_token_cache[cache_key]
            ValueError: If required auth configuration is missing
        """
        super().__init__(options)

        logger.info("running with options: %s", options)
        
        # Validate and log auth configuration early
        auth_type = self.config.get("auth_type", "none").lower()
        
        if auth_type in ["bearer", "api_key", "oauth2_refresh"]:
            auth_token = self.config.get("auth_token")
            if not auth_token:
                raise ValueError(
                    f"Auth type '{auth_type}' requires 'auth_token' in configuration. "
                    f"Ensure the secret is declared in data contract 'secret_keys' and resolved by PipelineConfig."
                )
            logger.info(f"Validated {auth_type} authentication token in RestApiDataSource.__init__")
        
        logger.debug(f"RestApiDataSource initialized with config keys: {list(self.config.keys())}")
        
    @classmethod
    def name(cls) -> str:
        """Return the short name for this data source."""
        return "rest_api_ds"
    
    def schema(self) -> Union[StructType, str]:
        """
        Spark calls this method to get a schema (StructType)
        for the DataFrame.
        
        We perform a quick API call to infer the columns by examining the first JSON object.
        Each field is flattened and its type is inferred (if enabled) or set as a string.
        """
        try:
            url = self._build_endpoint()
            # auth_token = self.config.get("auth_token")
            auth_token = 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ0b2tlblR5cGUiOiJDdXN0b21lckFQSV9EYXRhQWNjZXNzIiwidG9rZW5pZCI6ImNlNmYzODIwLTUxOTItNDkyOS1hY2Q1LTg2MWE5N2YzMzBhOSIsIndlYkFwcCI6IkN1c3RvbWVyQXBwIiwidmVyc2lvbiI6IjIiLCJpZGVudGl0eVRva2VuIjoiby9lUkhCNXI5WGpCam5mQWVjUFFRcHdZNUpMeEZBK002M24wOFQyUFFuMHhWRzdIWWp4NFh1N1NRaEhjK3ZwcWtMM2pxN3BZTUJzZXIzUzNwOGxJMmc0WXQyVVE5Z1dEZHdPZ1dOd2F2TkNOZGdRZG9mRllPV3dwbWhVZWFURThLcjhVbnI3dUVyZEJjT0hnbmZ6UStIZVZTYzF3M29kTGtrK1h2d1Fxc3RKcjFybTBCSW9ESmZPY1VRbG1jbWM4SFZKRTBROU1Lc3g3STFIckVKNC9oRnBTMGRVUnJncmRXSFp3YlhqL3JBd0tudWlteXlLaHpmczNJbERMRWdlTFVlVFpIbEpPOUlLeTJQUkM3QXBBSUl2YXBNZmlCU3BXa0gyVitGOGxxaHYvUSt5UWswdDloeDE5OG5HYTZ6aXVjdlZIZ2xSSXlFMzBGcVo1Q3dGemhtRkNxNWhFcjZhQlRtSHJQNDltdVlZU0s5UXIyaXYzWFJkc3NHQVpqcWpkcDlFV1kzamdBcTFjWHN2OWVoZ2wwQXBXMXEwYlBPc3hhSEZuMXRXY0dhOXRmVmlxUlU4VWYwTlR3YjIvMGdJTXJnMU1iR0hnN2RwU3FNV1k0ZWRJSk5wYTRZM1k2cDJUUVVOd0N1Y1dtb0pnVUxScGhBRmtuYkU0aExHSG9XdWZrTTEybnRJT1hOT1pkdk50bCtuQ29FUEpINm9RanV3MkthTEIzbWhXNGlkNm5xcWVoN1pzeWxDRkQ0SlA2dGM4SmZPTnJPWkQ2cWs1dWlETlpEUkdKNFM2ZGM0ek43N1huREtpYi9DcW9rTVBoZGZlNm4rZ2ZzS25uT0NRYk9uUDhFbGx0L3VqcmViZ21tc1lGNFhXSk9oOGJjV3E4VkJBWWhyRTFFUHZjQ250UGVLUXg0a25Gc3luUEJlUDh5MGhQZzVDR25OeEsxN0E3a0FPMVc4UGhSK0dOL1d4NE9QNWp4WnRUWUJZTUVtcHBGTVlSYXVvY3FDSHhxZkMwSHQxekpaM2JSeTZ5T2FvQjVNVDc4bUo4by9JbDNlVWkwSkhzM3FDRnlsWEIvUzdxYm1ibVZsd1FXUXFRYmZFL0tFK3BVNzJSazBnUVV0dmk2OXhrSkVTdlpiOFVSRlAvbXpocVd5NzBNZzc3ck9JbFJEMnZOdE90RVBLbUVrVjJ6OS9YU1N0YWhuUXZRa0ozeEhWazdaN2FGdWVMUngrT1BFTHZyT1dmSkE5Rm53SXJ2K3hQajhQY0tqaFZkaWhzRHBwUTROdzE5bHhwdGJKRGQ3ZXhKQ0loTkdTMmxPb1Ntc0lvWVZ0cE11QTF0ZGloWDJkUWdpWDVlRlg5N3krVi9LUG9uNDVHMEE3YW9uRjVBaVU2Ump3ZXViNWt5UzJYSG53UUZyeU1sdnNpa05jMUNXSVoyL1pVcEsrVHFZUE1td3pTM24vZU9tZ0hSZmdXUzhUNUtHL0hzcm1CbnFMRm0zTzcyNHQ3cGRuMWdFT2VVL1llRCtxRWpsTXFQY1dndk0wdWNFR01oV0FvczhhbHdkcXJoSXNCbGRqclRXVjhkT3VYTUUxQnJKWjlqVm96V0VSRUdOUm81U0ZwYWVJU0NieUZNRmpISndZN29LaktMVDJjMnlGYlp1NHN1LzFzVmVzY2hoVFc5SnpwVUtIU09YbVBSOTQyY3pTOUlEU3RUdlAzRkZGMzRuWkpKR2tWb3NiekpSbjhRbUhSYW1XT1R5ZE9VYmxmUFdRYUpJWjg1a1BGL3lRb2NGY2hBbmZXU01sL1hFNDMxb2ozS2lRSTRVOEVjYUsxYzU4dFhWZUpHYVd2VDdrdVNtTFNBS1VDeGpzZkY0QXdCd3luZEg2cURhaWwvR2lxYkJlSnQydURoY2lLU0FtS3l1QkZlejFvM2YveGI5TlFsSjRqaUpPVk1KWDRxbXgxY29yOWlCQ2s4QytxeDQ3Z1IzdStBMU1raVpwVzgwME13M2l5blV6b1hJS3BScHJ0NkRvT0wvU2ZmWlJYa0dYRHhFVVhUYVVVakRiVTdodkF6QTlBWXh0MmxWT1VUbTdnbm5kNDIvOGpCVUV3Vk5jT2VFZlorVzJuRXMwUllDUHF4QUI2TjNUbGJycEUyNEc4UkpZbUdnSi9KejBTSTRZRW5iaTFGRndzWnlEcFNrTmNuczBDVUVDbXFxWVBlY0hQMkxqMCs1aHhybFJyVnpRN1lZV2t6RzlPTmVpWVByY1lNYllkUWFFdmxuQVJaNm1WVVNTcXp3alZHRVhyRlVjYjMvU2VuM0pEQStlVkFLczJQeWsrVmJXMVQ3SGlobndPK0p3QnFNcmRUejFrME5MamY4MnpWaEsxaVg5MC9HR3E0eXEwSUhGOXBteVZPMTF4bkZCQ0FTazRoQzFPRENXcUJpL2J4YlpNOG5vTmxUbk1VQ3NPbFRLeHAxRTRHR0o3dzUvMjNTaXB1aDgxdVJMRlFwbkdubFo2RWhLVmdRYUhMQjNDMUh5RER2L2tjTEE4SUlIRlcrWk1HWU9VTzJZRWFVWUUrTkVKMGV1a05jV1lyYW1XOHVzeDBGWTJ4NDBZRG1tck9hcXVrakZqSGx1YnhBblpQRGpYdE1ibmNzQVNTUzYzNG9xOFc3YSIsImh0dHA6Ly9zY2hlbWFzLnhtbHNvYXAub3JnL3dzLzIwMDUvMDUvaWRlbnRpdHkvY2xhaW1zL2dpdmVubmFtZSI6IlJhc211cyBIb2xtIExhdXJzZW4iLCJsb2dpblR5cGUiOiJLZXlDYXJkIiwiYjNmIjoiMndtZjh1MWpoM0M2OU8vM1lQZlo2UUhwMmozcUZvYk82cXhWL1NEY2VOYz0iLCJwaWQiOiJQSUQ6OTIwOC0yMDAyLTItNzAyODQyOTk5MzUwIiwidXNlcklkIjoiNDUzMzU4IiwiaHR0cDovL3NjaGVtYXMueG1sc29hcC5vcmcvd3MvMjAwNS8wNS9pZGVudGl0eS9jbGFpbXMvbmFtZWlkZW50aWZpZXIiOiJQSUQ6OTIwOC0yMDAyLTItNzAyODQyOTk5MzUwIiwiZXhwIjoxNzY1ODc2OTI0LCJpc3MiOiJFbmVyZ2luZXQiLCJqdGkiOiJjZTZmMzgyMC01MTkyLTQ5MjktYWNkNS04NjFhOTdmMzMwYTkiLCJ0b2tlbk5hbWUiOiJ0ZXN0LXRva2VuIiwiYXVkIjoiRW5lcmdpbmV0In0.sv9koJNfE8R_-I3lsyLjX48kbw-6AjYALsjh4xf5Wrk'


            pagination_type = self.config.get("pagination_type", "none").lower()
            pagination_config = RestApiDataSource._parse_dict_config(
                self.config.get("pagination_config", {}), "pagination_config"
            )
            page_param = pagination_config.get("page_param", "page")
            start_page = int(RestApiDataSource._parse_numeric_config(
                pagination_config.get("start_page", 1), 1, "start_page"
            ))
            infer_types_flag = self.config.get("infer_types", "false").lower() == "true"

            params = {}
            # Use a requests.Session for improved performance and connection reuse
            with requests.Session() as session:
                if auth_token:
                    session.headers.update({"Authorization": auth_token})
                # Set a timeout to avoid hanging indefinitely
                resp = session.get(url, params=params, timeout=10)
                resp.raise_for_status()
                data = resp.json()
            
            logger.info("Inferring schema from REST API response: {}".format(data))

            # Apply json_path if present, otherwise try common result paths
            json_path = self.config.get("json_path")
            if json_path:
                data = get_nested_value(data, json_path)
            else:
                # If no json_path, try to detect common result paths
                if isinstance(data, dict):
                    if "result" in data and isinstance(data["result"], list):
                        data = data["result"]
                    elif "data" in data and isinstance(data["data"], list):
                        data = data["data"]
                    elif "results" in data and isinstance(data["results"], list):
                        data = data["results"]
                    elif "items" in data and isinstance(data["items"], list):
                        data = data["items"]
                    elif "records" in data and isinstance(data["records"], list):
                        data = data["records"]                        
            
            if data is None:
                # No data returns an empty schema
                return StructType([])

            # If the root is a single object, wrap it in a list
            if isinstance(data, dict):
                data = [data]
            if not isinstance(data, list) or len(data) == 0:
                return StructType([])

            # Infer columns based on the first element
            first_elem = data[0]
            if not isinstance(first_elem, dict):
                return StructType([])

            flattened = flatten_json(first_elem)
            fields = []
            for key, value in flattened.items():
                if infer_types_flag:
                    spark_type = infer_spark_type(value)
                else:
                    spark_type = StringType()
                fields.append(StructField(key, spark_type, True))
            logger.info("Using the following fields: {}".format(fields))
            return StructType(fields)
        except Exception as e:
            logger.error(f"Error inferring schema from REST API: {str(e)}")
            raise
    
    @staticmethod
    def _parse_dict_config(config_value: Any, config_name: str) -> Dict[str, Any]:
        """Parse configuration value that may be a string or dict.
        
        Handles both JSON strings and Python dict literal strings from Spark options.
        
        Args:
            config_value: The config value to parse
            config_name: Name of the config for logging
            
        Returns:
            Dictionary parsed from config value
        """
        # If already a dict, return a copy
        if isinstance(config_value, dict):
            return config_value.copy()
        
        # If string, try to parse
        if isinstance(config_value, str):
            # First try JSON (double quotes)
            try:
                return json.loads(config_value)
            except json.JSONDecodeError:
                pass
            
            # Then try Python dict literal (single quotes) - common from Spark options
            try:
                parsed = ast.literal_eval(config_value)
                if isinstance(parsed, dict):
                    logger.info(f"Parsed {config_name} using ast.literal_eval")
                    return parsed
            except (ValueError, SyntaxError) as e:
                logger.warning(f"Could not parse {config_name} as dict: {config_value}, error: {e}")
                return {}
        
        # For any other type, log and return empty dict
        logger.warning(f"Unexpected type for {config_name}: {type(config_value)}, value: {config_value}")
        return {}
    
    @staticmethod
    def _parse_numeric_config(config_value: Any, default: Union[int, float], config_name: str) -> Union[int, float]:
        """Parse numeric configuration value that may be a string.
        
        Args:
            config_value: The config value to parse
            default: Default value if parsing fails
            config_name: Name of the config for logging
            
        Returns:
            Numeric value (int or float)
        """
        if config_value is None:
            return default
        
        try:
            if isinstance(default, int):
                return int(config_value)
            else:
                return float(config_value)
        except (ValueError, TypeError):
            logger.warning(f"Could not parse {config_name} as {type(default).__name__}: {config_value}, using default: {default}")
            return default
    
    @staticmethod
    def _extract_data_from_response(response: Dict[str, Any], data_path: Union[str, None] = None) -> List[Dict[str, Any]]:
        """Extract data from API response using data_path.
        
        Args:
            response: The API response
            data_path: Dot-separated path to data in response
            
        Returns:
            List of records extracted from response
        """
        if not data_path:
            return response if isinstance(response, list) else [response]
        
        data = response
        for key in data_path.split("."):
            data = data.get(key, [])
        
        return data if isinstance(data, list) else [data]
    
    def _build_endpoint(self) -> str:
        """Build full endpoint URL, appending table_name if provided.
        
        Returns:
            Complete endpoint URL
        """
        base_endpoint = self.config["endpoint"]
        table_name = self.config.get("table_name")
        
        if table_name:
            # Ensure no double slashes
            if base_endpoint.endswith("/"):
                return f"{base_endpoint}{table_name}"
            else:
                return f"{base_endpoint}/{table_name}"
        
        return base_endpoint
    
    def _get_access_token_from_refresh(self, refresh_token: str) -> str:
        """Exchange refresh token for access token.
        
        Used for OAuth2 refresh token flow (e.g., Eloverblik API).
        
        Args:
            refresh_token: The refresh token
            
        Returns:
            Access token
            
        Raises:
            requests.HTTPError: If token exchange fails
        """
        token_endpoint = self.config.get("token_endpoint")
        if not token_endpoint:
            raise ValueError("token_endpoint must be configured for oauth2_refresh auth type")
        
        token_method = self.config.get("token_method", "GET").upper()
        token_response_path = self.config.get("token_response_path", "result")
        
        logger.info(f"Exchanging refresh token for access token at: {token_endpoint}")
        
        try:
            response = requests.request(
                method=token_method,
                url=token_endpoint,
                headers={"Authorization": f"Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ0b2tlblR5cGUiOiJDdXN0b21lckFQSV9EYXRhQWNjZXNzIiwidG9rZW5pZCI6ImNmMWZmZjU5LTc2OTktNGFkMC1hYzE3LWJkMWU1N2Y5NGMxYSIsIndlYkFwcCI6IkN1c3RvbWVyQXBwIiwidmVyc2lvbiI6IjIiLCJpZGVudGl0eVRva2VuIjoiby9lUkhCNXI5WGpCam5mQWVjUFFRcHdZNUpMeEZBK002M24wOFQyUFFuMHhWRzdIWWp4NFh1N1NRaEhjK3ZwcWtMM2pxN3BZTUJzZXIzUzNwOGxJMmc0WXQyVVE5Z1dEZHdPZ1dOd2F2TkNOZGdRZG9mRllPV3dwbWhVZWFURThLcjhVbnI3dUVyZEJjT0hnbmZ6UStIZVZTYzF3M29kTGtrK1h2d1Fxc3RKcjFybTBCSW9ESmZPY1VRbG1jbWM4SFZKRTBROU1Lc3g3STFIckVKNC9oRnBTMGRVUnJncmRXSFp3YlhqL3JBd0tudWlteXlLaHpmczNJbERMRWdlTFVlVFpIbEpPOUlLeTJQUkM3QXBBSUl2YXBNZmlCU3BXa0gyVitGOGxxaHYvUSt5UWswdDloeDE5OG5HYTZ6aXVjdlZIZ2xSSXlFMzBGcVo1Q3dGemhtRkNxNWhFcjZhQlRtSHJQNDltdVlZU0s5UXIyaXYzWFJkc3NHQVpqcWpkcDlFV1kzamdBcTFjWHN2OWVoZ2wwQXBXMXEwYlBPc3hhSEZuMXRXY0dhOXRmVmlxUlU4VWYwTlR3YjIvMGdJTXJnMU1iR0hnN2RwU3FNV1k0ZWRJSk5wYTRZM1k2cDJUUVVOd0N1Y1dtb0pnVUxScGhBRmtuYkU0aExHSG9XdWZrTTEybnRJT1hOT1pkdk50bCtuQ29FUEpINm9RanV3MkthTEIzbWhXNGlkNm5xcWVoN1pzeWxDRkQ0SlA2dGM4SmZPTnJPWkQ2cWs1dWlETlpEUkdKNFM2ZGM0ek43N1huREtpYi9DcW9rTVBoZGZlNm4rZ2ZzS25uT0NRYk9uUDhFbGx0L3VqcmViZ21tc1lGNFhXSk9oOGJjV3E4VkJBWWhyRTFFUHZjQ250UGVLUXg0a25Gc3luUEJlUDh5MGhQZzVDR25OeEsxN0E3a0FPMVc4UGhSK0dOL1d4NE9QNWp4WnRUWUJZTUVtcHBGTVlSYXVvY3FDSHhxZkMwSHQxekpaM2JSeTZ5T2FvQjVNVDc4bUo4by9JbDNlVWkwSkhzM3FDRnlsWEIvUzdxYm1ibVZsd1FXUXFRYmZFL0tFK3BVNzJSazBnUVV0dmk2OXhrSkVTdlpiOFVSRlAvbXpocVd5NzBNZzc3ck9JbFJEMnZOdE90RVBLbUVrVjJ6OS9YU1N0YWhuUXZRa0ozeEhWazdaN2FGdWVMUngrT1BFTHZyT1dmSkE5Rm53SXJ2K3hQajhQY0tqaFZkaWhzRHBwUTROdzE5bHhwdGJKRGQ3ZXhKQ0loTkdTMmxPb1Ntc0lvWVZ0cE11QTF0ZGloWDJkUWdpWDVlRlg5N3krVi9LUG9uNDVHMEE3YW9uRjVBaVU2Ump3ZXViNWt5UzJYSG53UUZyeU1sdnNpa05jMUNXSVoyL1pVcEsrVHFZUE1td3pTM24vZU9tZ0hSZmdXUzhUNUtHL0hzcm1CbnFMRm0zTzcyNHQ3cGRuMWdFT2VVL1llRCtxRWpsTXFQY1dndk0wdWNFR01oV0FvczhhbHdkcXJoSXNCbGRqclRXVjhkT3VYTUUxQnJKWjlqVm96V0VSRUdOUm81U0ZwYWVJU0NieUZNRmpISndZN29LaktMVDJjMnlGYlp1NHN1LzFzVmVzY2hoVFc5SnpwVUtIU09YbVBSOTQyY3pTOUlEU3RUdlAzRkZGMzRuWkpKR2tWb3NiekpSbjhRbUhSYW1XT1R5ZE9VYmxmUFdRYUpJWjg1a1BGL3lRb2NGY2hBbmZXU01sL1hFNDMxb2ozS2lRSTRVOEVjYUsxYzU4dFhWZUpHYVd2VDdrdVNtTFNBS1VDeGpzZkY0QXdCd3luZEg2cURhaWwvR2lxYkJlSnQydURoY2lLU0FtS3l1QkZlejFvM2YveGI5TlFsSjRqaUpPVk1KWDRxbXgxY29yOWlCQ2s4QytxeDQ3Z1IzdStBMU1raVpwVzgwME13M2l5blV6b1hJS3BScHJ0NkRvT0wvU2ZmWlJYa0dYRHhFVVhUYVVVakRiVTdodkF6QTlBWXh0MmxWT1VUbTdnbm5kNDIvOGpCVUV3Vk5jT2VFZlorVzJuRXMwUllDUHF4QUI2TjNUbGJycEUyNEc4UkpZbUdnSi9KejBTSTRZRW5iaTFGRndzWnlEcFNrTmNuczBDVUVDbXFxWVBlY0hQMkxqMCs1aHhybFJyVnpRN1lZV2t6RzlPTmVpWVByY1lNYllkUWFFdmxuQVJaNm1WVVNTcXp3alZHRVhyRlVjYjMvU2VuM0pEQStlVkFLczJQeWsrVmJXMVQ3SGlobndPK0p3QnFNcmRUejFrME5MamY4MnpWaEsxaVg5MC9HR3E0eXEwSUhGOXBteVZPMTF4bkZCQ0FTazRoQzFPRENXcUJpL2J4YlpNOG5vTmxUbk1VQ3NPbFRLeHAxRTRHR0o3dzUvMjNTaXB1aDgxdVJMRlFwbkdubFo2RWhLVmdRYUhMQjNDMUh5RER2L2tjTEE4SUlIRlcrWk1HWU9VTzJZRWFVWUUrTkVKMGV1a05jV1lyYW1XOHVzeDBGWTJ4NDBZRG1tck9hcXVrakZqSGx1YnhBblpQRGpYdE1ibmNzQVNTUzYzNG9xOFc3YSIsImh0dHA6Ly9zY2hlbWFzLnhtbHNvYXAub3JnL3dzLzIwMDUvMDUvaWRlbnRpdHkvY2xhaW1zL2dpdmVubmFtZSI6IlJhc211cyBIb2xtIExhdXJzZW4iLCJsb2dpblR5cGUiOiJLZXlDYXJkIiwiYjNmIjoiMndtZjh1MWpoM0M2OU8vM1lQZlo2UUhwMmozcUZvYk82cXhWL1NEY2VOYz0iLCJwaWQiOiJQSUQ6OTIwOC0yMDAyLTItNzAyODQyOTk5MzUwIiwidXNlcklkIjoiNDUzMzU4IiwiaHR0cDovL3NjaGVtYXMueG1sc29hcC5vcmcvd3MvMjAwNS8wNS9pZGVudGl0eS9jbGFpbXMvbmFtZWlkZW50aWZpZXIiOiJQSUQ6OTIwOC0yMDAyLTItNzAyODQyOTk5MzUwIiwiZXhwIjoxNzY1NjI5NDg4LCJpc3MiOiJFbmVyZ2luZXQiLCJqdGkiOiJjZjFmZmY1OS03Njk5LTRhZDAtYWMxNy1iZDFlNTdmOTRjMWEiLCJ0b2tlbk5hbWUiOiJ0ZXN0LXRva2VuIiwiYXVkIjoiRW5lcmdpbmV0In0.hy4nq3Vr3oVNDb1dyaJCxkCEeCkbk5EA9DHgxyEKq_Q"},
                timeout=30
            )
            response.raise_for_status()
            
            data = response.json()
            
            # Extract access token using response path
            access_token = data
            for key in token_response_path.split("."):
                access_token = access_token.get(key)
            
            if not access_token:
                raise ValueError(f"Could not extract access token from response using path: {token_response_path}")
            
            logger.info("Successfully obtained access token")
            return access_token
            
        except requests.HTTPError as e:
            logger.error(f"Failed to exchange refresh token: {e}")
            raise
    
    def _build_headers(self) -> Dict[str, str]:
        """Build HTTP headers including static headers only.
        
        NOTE: Secret resolution happens during data read phase (in RestApiDataSourceReader._build_headers())
        when dbutils is guaranteed to be available. This method only parses static headers.
        
        Returns:
            Dict of HTTP headers (without authentication secrets)
        """
        # Only parse static headers - secret resolution happens at read time
        headers = RestApiDataSource._parse_dict_config(self.config.get("headers", {}), "headers")
        logger.debug("Built static headers. Authentication secrets will be resolved during data read phase.")
        return headers
    
    def create_reader(self, schema: StructType) -> "RestApiDataSourceReader":
        """
        Create a batch reader for the REST API.
        
        Args:
            schema: The response schema
            
        Returns:
            RestApiDataSourceReader instance
        """
        return RestApiDataSourceReader(self.config, schema)
    
    def create_stream_reader(self, schema: StructType) -> "RestApiDataSourceStreamReader":
        """
        Create a streaming reader for the REST API.
        
        Args:
            schema: The response schema
            
        Returns:
            RestApiDataSourceStreamReader instance
        """
        return RestApiDataSourceStreamReader(self.config, schema)
    
    def read_batch(self, spark: "SparkSession") -> "DataFrame":
        """
        Read data as a batch DataFrame using Spark's format API.
        
        This method provides compatibility with BaseConnector interface
        and uses the native Spark DataSource API for optimized execution.
        
        Args:
            spark: Active SparkSession
            
        Returns:
            Batch DataFrame from the REST API
        """
        # Register the DataSource with Spark if not already registered
        try:
            spark.dataSource.register(RestApiDataSource)
            logger.debug(f"Registered DataSource: {self.name()}")
        except Exception as e:
            logger.debug(f"DataSource may already be registered: {e}")
        
        # Filter config to only include DataSource-relevant options
        # Exclude framework-specific keys and non-string values (like StructType schema)
        # Spark's DataSource API options() only accepts string values
        excluded_keys = {
            'catalog', 'volume', 'source_system', 
            'model_name', 'format', 'schema'  # schema handled separately via schema() method
        }
        datasource_config = {
            str(k): str(v) for k, v in self.config.items() 
            if k not in excluded_keys and not k.endswith('_catalog') and not k.endswith('_schema')
            and not isinstance(v, StructType)  # Don't pass StructType objects to options
        }
        
        # Use Spark's format API to read data
        df = spark.read.format(self.name()).options(**datasource_config).load()
        
        logger.info(f"Read DataFrame with schema: {df.schema}")
        logger.info(f"DataFrame columns: {df.columns}")
        
        # The DataSource API returns records from read_partition() as individual rows
        # If schema was properly provided, rows will have all fields expanded
        # If schema fell back to generic (data: String), records are wrapped as Row(data=...)
        # In that case, we need to parse the data
        
        if len(df.columns) == 1 and df.columns[0] == "data":
            # Generic fallback schema - data column contains string representation
            logger.warning("Generic schema detected. Cannot properly unwrap data.")
            logger.info("Ensure data contract schema is provided to connector via PipelineConfig.get_connector()")
        
        logger.info(f"Created DataFrame from REST API using Spark format API")
        return df
    
    def read_stream(self, spark: "SparkSession") -> "DataFrame":
        """
        Read data as a streaming DataFrame using Spark's format API.
        
        This method provides compatibility with BaseConnector interface.
        Note: Streaming is supported but the API must provide incremental data.
        
        Args:
            spark: Active SparkSession
            
        Returns:
            Streaming DataFrame from the REST API
        """
        # Register the DataSource with Spark if not already registered
        try:
            spark.dataSource.register(RestApiDataSource)
            logger.debug(f"Registered DataSource: {self.name()}")
        except Exception as e:
            logger.debug(f"DataSource may already be registered: {e}")
        
        # Filter config to only include DataSource-relevant options
        # Exclude framework-specific keys and non-string values (like StructType schema)
        # Spark's DataSource API options() only accepts string values
        excluded_keys = {
            'catalog', 'volume', 'source_system', 
            'model_name', 'format', 'schema'  # schema handled separately via schema() method
        }
        datasource_config = {
            str(k): str(v) for k, v in self.config.items() 
            if k not in excluded_keys and not k.endswith('_catalog') and not k.endswith('_schema')
            and not isinstance(v, StructType)  # Don't pass StructType objects to options
        }
        
        # Use Spark's format API for streaming
        df = spark.readStream.format(self.name()).options(**datasource_config).load()
        
        logger.info(f"Read streaming DataFrame with schema: {df.schema}")
        logger.info(f"DataFrame columns: {df.columns}")
        
        # The DataSource API returns records from read_partition() as individual rows
        # If schema was properly provided, rows will have all fields expanded
        # If schema fell back to generic (data: String), records are wrapped as Row(data=...)
        
        if len(df.columns) == 1 and df.columns[0] == "data":
            # Generic fallback schema - data column contains string representation
            logger.warning("Generic schema detected. Cannot properly unwrap data.")
            logger.info("Ensure data contract schema is provided to connector via PipelineConfig.get_connector()")
        
        logger.info(f"Created streaming DataFrame from REST API using Spark format API")
        return df
    
    def validate_config(self, config: Dict[str, Any]) -> None:
        """
        Validate REST API connector configuration.
        
        Args:
            config: Configuration dictionary to validate
            
        Raises:
            ValueError: If configuration is invalid or missing required fields
        """
        if "endpoint" not in config:
            raise ValueError("REST API connector requires 'endpoint' in configuration")
        
        if "method" not in config:
            config["method"] = "GET"  # Default to GET
        
        # Validate auth configuration if present
        auth_type = config.get("auth_type", "none").lower()
        if auth_type in ["bearer", "api_key"]:
            if "auth_token" not in config:
                raise ValueError(f"Auth type '{auth_type}' requires 'auth_token' in configuration")
    
    def supports_streaming(self) -> bool:
        """Check if this connector supports streaming ingestion."""
        return True


class RestApiDataSourceReader(BaseDataSourceReader):
    """
    Batch reader for REST APIs with pagination-aware partitioning.
    
    Creates one partition per page/offset for parallel API requests.
    
    Access tokens for OAuth2 are cached per reader instance to avoid
    repeated token exchanges and hitting rate limits.
    """
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        
        # Pre-fetch and cache access token for OAuth2 to catch errors early
        auth_type = self.config.get("auth_type", "none").lower()
        if auth_type == "oauth2_refresh":
            refresh_token: str = self.config.get("auth_token")  # type: ignore
            if not refresh_token:
                raise ValueError("OAuth2 refresh requires 'auth_token' to be configured")
            
            logger.info("Pre-fetching OAuth2 access token in RestApiDataSourceReader.__init__")
            try:
                cache_key = self._get_token_cache_key(refresh_token)

                logger.info(f"OAuth2 token cache key: {cache_key}")
                
                # Check if already cached from a previous reader instance
                if cache_key in RestApiDataSourceReader._access_token_cache:
                    logger.info("Using cached OAuth2 access token from previous reader")
                else:
                    # Exchange refresh token for new access token
                    access_token = self._get_access_token_from_refresh(refresh_token)
                    RestApiDataSourceReader._access_token_cache[cache_key] = access_token
                    logger.info(f"Cached new OAuth2 access token for reader instance {access_token}")
                    logger.info("Successfully pre-fetched and cached OAuth2 access token")
            except Exception as e:
                raise ValueError(
                    f"Failed to obtain OAuth2 access token during reader initialization: {e}. "
                    f"Check token_endpoint, token_method, and token_response_path configuration."
                )
        
        logger.debug(f"RestApiDataSourceReader initialized with auth_type: {auth_type}")
    
    def create_partitions(self) -> Sequence[InputPartition]:
        """
        Create partitions based on pagination strategy.
        
        Returns:
            Sequence of page/offset partitions for parallel requests
        """
        pagination_type = self.config.get("pagination_type", "none").lower()
        pagination_config_raw = self.config.get("pagination_config", {})
        logger.info(f"pagination_config_raw type: {type(pagination_config_raw)}, isinstance dict: {isinstance(pagination_config_raw, dict)}, value: {pagination_config_raw}")
        pagination_config = RestApiDataSource._parse_dict_config(pagination_config_raw, "pagination_config")
        logger.info(f"After parsing - pagination_config type: {type(pagination_config)}, value: {pagination_config}")
        
        if pagination_type == "none":
            return [SimpleInputPartition({})]
        
        if pagination_type == "page":
            # Create partitions for known page range
            start_page = int(RestApiDataSource._parse_numeric_config(pagination_config.get("start_page", 1), 1, "start_page"))
            max_pages = int(RestApiDataSource._parse_numeric_config(pagination_config.get("max_pages", 10), 10, "max_pages"))
            page_size = int(RestApiDataSource._parse_numeric_config(pagination_config.get("page_size", 100), 100, "page_size"))
            
            partitions = [
                PageInputPartition(page_number=page, limit=page_size)
                for page in range(start_page, start_page + max_pages)
            ]
            logger.info(f"Created {len(partitions)} page partitions")
            return partitions
        
        elif pagination_type == "offset":
            # Create partitions for offset ranges
            pagination_config_raw = self.config.get("pagination_config", {})
            logger.info(f"Raw pagination_config type: {type(pagination_config_raw)}, value: {pagination_config_raw}")
            logger.info(f"Parsed pagination_config: {pagination_config}")
            
            # If total_records not specified, try to get it from API response
            total_records_config = pagination_config.get("total_records")
            logger.info(f"total_records from config: {total_records_config}")
            
            if total_records_config:
                total_records = int(RestApiDataSource._parse_numeric_config(total_records_config, 1000, "total_records"))
                logger.info(f"Using configured total_records: {total_records}")
            else:
                # Try to get total from API metadata
                total_records = self._get_total_records_from_api(pagination_config)
                logger.info(f"Got total_records from API: {total_records}")
            
            limit_config = pagination_config.get("limit")
            logger.info(f"limit from config: {limit_config}")
            limit = int(RestApiDataSource._parse_numeric_config(limit_config, 100, "limit"))
            
            partitions = [
                PageInputPartition(offset=offset, limit=limit)
                for offset in range(0, total_records, limit)
            ]
            logger.info(f"Created {len(partitions)} offset partitions for {total_records} total records with limit {limit}")
            return partitions
        
        else:
            # For cursor-based pagination, we can't pre-partition
            # Use single partition and handle pagination sequentially
            return [SimpleInputPartition({"pagination_type": pagination_type})]
    
    def read_partition(self, partition: InputPartition) -> Iterator[Row]:
        """
        Read data from a specific page/offset with automatic retry on timeout.
        
        If a timeout occurs, automatically retries with progressively smaller limits:
        - First retry: 50% of original limit
        - Second retry: 25% of original limit
        - Third retry: 10% of original limit
        
        Args:
            partition: The partition (page/offset) to read
            
        Yields:
            Row objects from the API response
        """
        endpoint = self._build_endpoint()
        method = self.config.get("method", "GET").upper()
        headers = self._build_headers()
        
        # Parse params and pagination config
        base_params = RestApiDataSource._parse_dict_config(self.config.get("params", {}), "params")
        pagination_config = RestApiDataSource._parse_dict_config(self.config.get("pagination_config", {}), "pagination_config")

        logger.debug(f"fetching data from API partition {partition} endpoint: {endpoint} with base_params: {base_params} and pagination_config: {pagination_config}")
        
        # Apply rate limiting
        self._apply_rate_limit()
        
        # Build request params based on partition and log progress
        current_position = None
        total_records = None
        limit = 1000  # default
        if isinstance(partition, PageInputPartition):
            if partition.page_number is not None:
                page_param = pagination_config.get("page_param", "page")
                size_param = pagination_config.get("size_param", "size")
                base_params[page_param] = partition.page_number
                base_params[size_param] = partition.limit
                current_position = partition.page_number * partition.limit
                total_records = pagination_config.get("total_records")
                limit = partition.limit
            elif partition.offset is not None:
                offset_param = pagination_config.get("offset_param", "offset")
                limit_param = pagination_config.get("limit_param", "limit")
                base_params[offset_param] = partition.offset
                base_params[limit_param] = partition.limit
                current_position = partition.offset
                total_records = pagination_config.get("total_records")
                limit = partition.limit
        
        # Log pagination progress
        if current_position is not None and total_records:
            progress_pct = (current_position / total_records) * 100
            logger.info(f"Fetching records {current_position}-{current_position + limit} of {total_records} ({progress_pct:.1f}% complete)")
        
        # Retry with decreasing limits on timeout
        retry_limits = [limit, int(limit * 0.5), int(limit * 0.25), int(limit * 0.1)]
        last_error = None
        
        for attempt, retry_limit in enumerate(retry_limits):
            try:
                # Update limit in params for this attempt
                if isinstance(partition, PageInputPartition):
                    if partition.page_number is not None:
                        size_param = pagination_config.get("size_param", "size")
                        base_params[size_param] = retry_limit
                    elif partition.offset is not None:
                        limit_param = pagination_config.get("limit_param", "limit")
                        base_params[limit_param] = retry_limit
                
                if attempt > 0:
                    logger.warning(f"Retry attempt {attempt} with reduced limit: {retry_limit} (original: {limit})")
                
                logger.info(f"API Call [read_partition]: {method} {endpoint} with params: {base_params}")
                logger.debug(f"Making API request: {method} {endpoint} with params: {base_params}")
                
                timeout = RestApiDataSource._parse_numeric_config(self.config.get("timeout", 30), 30.0, "timeout")
                
                response = requests.request(
                    method=method,
                    url=endpoint,
                    headers=headers,
                    params=base_params,
                    timeout=timeout
                )
                logger.error(response)
                response.raise_for_status()
                logger.info(f"API Response [read_partition]: {response.status_code} from {response.url}")
                data = response.json()
                
                # Extract data using data_path
                records = RestApiDataSource._extract_data_from_response(data, self.config.get("data_path"))
                
                if attempt > 0:
                    logger.info(f"Successfully retrieved {len(records)} records after reducing limit to {retry_limit}")
                else:
                    logger.info(f"Retrieved {len(records)} records from offset {current_position}")
                
                # Log structure of first record for debugging
                if records and isinstance(records[0], dict):
                    logger.info(f"First record has {len(records[0])} fields: {list(records[0].keys())}")
                
                # Convert to rows and yield
                for record in records:
                    if isinstance(record, dict):
                        # If schema is defined, only include schema fields to avoid mismatch
                        if "schema" in self.config and isinstance(self.config["schema"], StructType):
                            schema_fields = {f.name for f in self.config["schema"].fields}
                            filtered_record = {k: v for k, v in record.items() if k in schema_fields}
                            if len(filtered_record) != len(schema_fields):
                                missing_fields = schema_fields - set(filtered_record.keys())
                                logger.warning(f"Record missing fields from schema: {missing_fields}. Using None for missing fields.")
                                for field_name in schema_fields:
                                    filtered_record.setdefault(field_name, None)
                            yield Row(**filtered_record)
                        else:
                            yield Row(**record)
                    else:
                        yield Row(data=str(record))
                
                # Success - exit retry loop
                return
                
            except requests.exceptions.Timeout as e:
                last_error = e
                if attempt < len(retry_limits) - 1:
                    logger.warning(f"Timeout at offset {current_position} with limit {retry_limit}: {e}. Will retry with smaller limit.")
                    time.sleep(2)  # Brief delay before retry
                else:
                    logger.error(f"All retry attempts exhausted at offset {current_position}. Final limit tried: {retry_limit}")
                    raise
            
            except Exception as e:
                logger.error(f"API request failed at offset {current_position}: {e}")
                raise
    
    def _build_endpoint(self) -> str:
        """Build full endpoint URL, appending table_name if provided."""
        base_endpoint = self.config["endpoint"]
        table_name = self.config.get("table_name")
        
        if table_name:
            if base_endpoint.endswith("/"):
                return f"{base_endpoint}{table_name}"
            else:
                return f"{base_endpoint}/{table_name}"
        
        return base_endpoint
    
    def _get_access_token_from_refresh(self, refresh_token: str) -> str:
        """Exchange refresh token for access token.
        
        Used for OAuth2 refresh token flow (e.g., Eloverblik API).
        
        Args:
            refresh_token: The refresh token
            
        Returns:
            Access token
            
        Raises:
            requests.HTTPError: If token exchange fails
        """
        token_endpoint = self.config.get("token_endpoint")
        if not token_endpoint:
            raise ValueError("token_endpoint must be configured for oauth2_refresh auth type")
        
        token_method = self.config.get("token_method", "GET").upper()
        token_response_path = self.config.get("token_response_path", "result")
        
        logger.info(f"Exchanging refresh token for access token at: {token_endpoint}")

        logger.info(f"usinbg : {refresh_token}")

        headers={"Authorization": f"Bearer {refresh_token}"},
        logger.info(f"running with asgvb {headers}")
     
        try:
            response = requests.request(
                method=token_method,
                url=token_endpoint,
                headers={"Authorization": f"Bearer {refresh_token}"},
                timeout=30
            )
            response.raise_for_status()
            
            data = response.json()
            
            # Extract access token using response path
            access_token = data
            for key in token_response_path.split("."):
                access_token = access_token.get(key)
            
            if not access_token:
                raise ValueError(f"Could not extract access token from response using path: {token_response_path}")
            
            logger.info("Successfully obtained access token")
            return access_token
            
        except requests.HTTPError as e:
            logger.error(f"Failed to exchange refresh token: {e}")
            raise
    
    # Class-level cache for pre-fetched OAuth2 access tokens (refresh_token_hash -> access_token)
    # Populated during __init__ for oauth2_refresh auth type
    _access_token_cache: Dict[str, str] = {}
    
    def _build_headers(self) -> Dict[str, str]:
        """Build HTTP headers including authentication.
        
        Auth tokens are pre-fetched and validated during __init__.
        This method simply assembles the headers using cached/configured tokens.
        
        Supports auth types:
        - bearer: Direct bearer token from config
        - api_key: API key in custom header from config
        - oauth2_refresh: Pre-fetched and cached access token
        
        Returns:
            Dict of HTTP headers including authentication
        """
        headers = RestApiDataSource._parse_dict_config(self.config.get("headers", {}), "headers")
        auth_type = self.config.get("auth_type", "none").lower()
        
        if auth_type == "bearer":
            token = self.config.get("auth_token")
            logger.debug("Using bearer token from config")
            headers["Authorization"] = f"Bearer {token}"
                
        elif auth_type == "api_key":
            token = self.config.get("auth_token")
            header_name = self.config.get("auth_header", "X-API-Key")
            logger.debug(f"Using API key in header: {header_name}")
            headers[header_name] = token
                
        elif auth_type == "oauth2_refresh":
            # Get the cached access token that was pre-fetched in __init__
            refresh_token: str = self.config.get("auth_token")  # type: ignore
            cache_key = self._get_token_cache_key(refresh_token)
            
            access_token = RestApiDataSourceReader._access_token_cache.get(cache_key)
            if not access_token:
                # Fallback: re-fetch if not in cache (shouldn't happen if __init__ succeeded)
                logger.warning("Access token not found in cache, re-fetching")
                access_token = self._get_access_token_from_refresh(refresh_token)
                RestApiDataSourceReader._access_token_cache[cache_key] = access_token
            else:
                logger.debug("Using pre-fetched OAuth2 access token")
            
            headers["Authorization"] = f"Bearer {access_token}"
        
        return headers
    
    @staticmethod
    def _get_token_cache_key(refresh_token: str) -> str:
        """Get cache key for a refresh token (hash for security)."""
        import hashlib
        return hashlib.sha256(refresh_token.encode()).hexdigest()
    
    def _apply_rate_limit(self) -> None:
        """Apply rate limiting delay if configured."""
        if "rate_limit_delay" in self.config:
            delay = RestApiDataSource._parse_numeric_config(
                self.config["rate_limit_delay"], 0.0, "rate_limit_delay"
            )
            time.sleep(delay)
    
    def _get_total_records_from_api(self, pagination_config: Dict[str, Any]) -> int:
        """Get total record count from API response metadata.
        
        Args:
            pagination_config: Pagination configuration
            
        Returns:
            Total number of records, defaults to 1000 if not found
        """
        try:
            endpoint = self._build_endpoint()
            headers = self._build_headers()
            timeout = RestApiDataSource._parse_numeric_config(self.config.get("timeout", 30), 30.0, "timeout")
            
            # Make a minimal request to get metadata
            params = RestApiDataSource._parse_dict_config(self.config.get("params", {}), "params")
            limit_param = pagination_config.get("limit_param", "limit")
            params[limit_param] = 1  # Only fetch 1 record to get metadata
            
            logger.info(f"API Call [get_total_records]: GET {endpoint} with params: {params}")
            response = requests.get(
                endpoint,
                headers=headers,
                params=params,
                timeout=timeout
            )
            response.raise_for_status()
            logger.info(f"API Response [get_total_records]: {response.status_code} from {response.url}")
            data = response.json()
            
            # Try common metadata fields for total count
            total_keys = ["total", "total_count", "totalRecords", "count", "total_records"]
            for key in total_keys:
                if key in data:
                    total = int(data[key])
                    logger.info(f"Found total records in API response: {total}")
                    return total
            
            logger.warning("Could not find total record count in API response, using default: 1000")
            return 1000
            
        except Exception as e:
            logger.warning(f"Error getting total records from API: {e}, using default: 1000")
            return 1000


class RestApiDataSourceStreamReader(BaseDataSourceStreamReader):
    """
    Streaming reader for REST APIs with timestamp-based incremental loading.
    
    Uses offset-based polling to fetch only new data since last checkpoint.
    Supports timestamp filtering for efficient incremental loads.
    """
    
    def __init__(self, *args, **kwargs):
        """Initialize streaming reader with same OAuth2 token caching as batch reader."""
        super().__init__(*args, **kwargs)
        
        # Pre-fetch and cache access token for OAuth2 (same as RestApiDataSourceReader)
        auth_type = self.config.get("auth_type", "none").lower()
        if auth_type == "oauth2_refresh":
            refresh_token: str = self.config.get("auth_token")  # type: ignore
            if not refresh_token:
                raise ValueError("OAuth2 refresh requires 'auth_token' to be configured")
            
            logger.info("Pre-fetching OAuth2 access token in RestApiDataSourceStreamReader.__init__")
            try:
                cache_key = self._get_token_cache_key(refresh_token)
                
                # Check if already cached from a previous reader instance
                if cache_key in RestApiDataSourceStreamReader._access_token_cache:
                    logger.info("Using cached OAuth2 access token from previous reader")
                else:
                    # Exchange refresh token for new access token
                    access_token = self._get_access_token_from_refresh(refresh_token)
                    RestApiDataSourceStreamReader._access_token_cache[cache_key] = access_token
                    logger.info("Successfully pre-fetched and cached OAuth2 access token")
            except Exception as e:
                raise ValueError(
                    f"Failed to obtain OAuth2 access token during streaming reader initialization: {e}. "
                    f"Check token_endpoint, token_method, and token_response_path configuration."
                )
        
        logger.debug(f"RestApiDataSourceStreamReader initialized with auth_type: {auth_type}")
    
    def get_initial_offset(self) -> dict:
        """
        Return the initial offset for streaming based on timestamp.
        
        Uses timestamp_field from config to track incremental progress.
        On first run (no checkpoint), uses initial_timestamp or defaults to 30 days back.
        
        Returns:
            Dictionary with last_timestamp (ISO format string)
        """
        # Get initial timestamp from config or default to 30 days back
        initial_timestamp = self.config.get("initial_timestamp")
        if not initial_timestamp:
            # Default to 30 days back from now
            default_start = datetime.datetime.now() - datetime.timedelta(days=30)
            initial_timestamp = default_start.isoformat()
            logger.info(f"No initial_timestamp configured, defaulting to 30 days back: {initial_timestamp}")
        else:
            logger.info(f"Using configured initial_timestamp: {initial_timestamp}")
        
        return {"last_timestamp": initial_timestamp}
    
    def get_latest_offset(self) -> dict:
        """
        Get the latest available offset by querying the API for most recent data.
        
        Queries API sorted by timestamp DESC to get the most recent record's timestamp.
        This becomes the new checkpoint for the next streaming batch.
        
        Returns:
            Dictionary with last_timestamp (ISO format string)
        """
        try:
            endpoint = self._build_endpoint()
            headers = self._build_headers()
            timeout = RestApiDataSource._parse_numeric_config(self.config.get("timeout", 30), 30.0, "timeout")
            params = RestApiDataSource._parse_dict_config(self.config.get("params", {}), "params").copy()
            
            # Get timestamp field from config
            timestamp_field = self.config.get("timestamp_field")
            if not timestamp_field:
                logger.warning("timestamp_field not configured, cannot determine latest offset")
                return self.get_initial_offset()
            
            # Query for most recent record by sorting DESC and limiting to 1
            params["sort"] = f"{timestamp_field} DESC"
            params["limit"] = 1
            
            logger.info(f"API Call [get_latest_offset]: GET {endpoint} with params: {params}")
            response = requests.get(
                endpoint,
                headers=headers,
                params=params,
                timeout=timeout
            )
            response.raise_for_status()
            logger.info(f"API Response [get_latest_offset]: {response.status_code} from {response.url}")
            data = response.json()
            
            records = RestApiDataSource._extract_data_from_response(data, self.config.get("data_path"))
            if records and len(records) > 0:
                latest_record = records[0]
                latest_timestamp = latest_record.get(timestamp_field)
                if latest_timestamp:
                    logger.info(f"Latest timestamp from API: {latest_timestamp}")
                    return {"last_timestamp": latest_timestamp}
            
            logger.warning("Could not find latest timestamp in API response")
        except Exception as e:
            logger.warning(f"Error querying latest offset: {e}")
        
        # Fallback to initial offset if we can't determine latest
        return self.get_initial_offset()
    
    def create_stream_partitions(self, start: dict, end: dict) -> Sequence[InputPartition]:
        """
        Create partitions for the timestamp range between start and end offsets.
        
        For now, creates a single partition for the entire time range.
        Future enhancement: Could split large time ranges into multiple partitions
        for parallel processing (e.g., partition by day/week/month).
        
        Args:
            start: Start offset with last_timestamp
            end: End offset with last_timestamp
            
        Returns:
            Sequence of offset partitions
        """
        start_ts = start.get("last_timestamp")
        end_ts = end.get("last_timestamp")
        
        logger.info(f"Creating stream partition for timestamp range: {start_ts} to {end_ts}")
        
        # Single partition for the entire time range
        return [OffsetInputPartition(
            start_offset=start,
            end_offset=end,
            partition_id=f"ts_{start_ts}_to_{end_ts}"
        )]
    
    def read_stream_partition(self, partition: InputPartition) -> Iterator[Row]:
        """
        Read new data from the API for the timestamp range with pagination.
        
        Strategy: 
        1. If API supports timestamp filtering (timestamp_param configured),
           add start date filter to reduce server-side data
        2. Fetch data sorted DESC (newest first) to only get recent records
        3. Filter client-side and reverse to chronological order
        
        Args:
            partition: The partition with timestamp offset range
            
        Yields:
            Row objects with new data in chronological order
        """
        if not isinstance(partition, OffsetInputPartition):
            return
        
        endpoint = self._build_endpoint()
        headers = self._build_headers()
        base_params = RestApiDataSource._parse_dict_config(self.config.get("params", {}), "params").copy()
        
        # Get timestamp field for filtering
        timestamp_field = self.config.get("timestamp_field")
        if not timestamp_field:
            logger.error("timestamp_field not configured for streaming")
            return
        
        start_ts = partition.start_offset.get("last_timestamp")
        end_ts = partition.end_offset.get("last_timestamp")
        
        logger.info(f"Fetching incremental data: timestamp > {start_ts} and <= {end_ts}")
        
        # If API supports timestamp filtering, add start date parameter
        # This reduces server-side data processing before sorting/pagination
        timestamp_param = self.config.get("timestamp_param")
        if timestamp_param and start_ts:
            # Convert ISO 8601 format to API-accepted format
            # API expects: yyyy-MM-dd or yyyy-MM-ddTHH:mm (no seconds)
            # From: 2025-11-01T00:00:00 -> To: 2025-11-01T00:00
            if 'T' in start_ts:
                # Remove seconds portion: 2025-11-01T00:00:00 -> 2025-11-01T00:00
                formatted_ts = start_ts.rsplit(':', 1)[0]
            else:
                formatted_ts = start_ts
            base_params[timestamp_param] = formatted_ts
            logger.info(f"Added API filter: {timestamp_param}={formatted_ts}")
        
        # Sort by timestamp DESC to get newest records first
        # This way we only fetch recent data, not all historical records
        if "sort" in base_params:
            del base_params["sort"]
        base_params["sort"] = f"{timestamp_field} DESC"
        
        # Handle pagination
        pagination_config = RestApiDataSource._parse_dict_config(
            self.config.get("pagination_config", {}), 
            "pagination_config"
        )
        limit = int(RestApiDataSource._parse_numeric_config(
            pagination_config.get("limit", 10000), 10000, "limit"
        ))
        offset = 0
        total_records_fetched = 0
        records_in_window = []  # Buffer to reverse at the end
        reached_start_boundary = False
        
        while True:
            try:
                # Add pagination params
                params = base_params.copy()
                offset_param = pagination_config.get("offset_param", "offset")
                limit_param = pagination_config.get("limit_param", "limit")
                params[offset_param] = offset
                params[limit_param] = limit
                
                logger.info(f"API Call [read_stream_partition]: GET {endpoint} with params: {params}")
                logger.debug(f"Streaming API request: offset={offset}, limit={limit}, sort=DESC")
                
                timeout = RestApiDataSource._parse_numeric_config(self.config.get("timeout", 30), 30.0, "timeout")
                
                response = requests.get(
                    endpoint,
                    headers=headers,
                    params=params,
                    timeout=timeout
                )
                response.raise_for_status()
                data = response.json()
                logger.info(f"API Response [read_stream_partition]: {response.status_code} from {response.url}")
                
                records = RestApiDataSource._extract_data_from_response(data, self.config.get("data_path"))
                
                if not records or len(records) == 0:
                    logger.info(f"No more records available. Total in window: {len(records_in_window)}")
                    break
                
                logger.info(f"Fetched {len(records)} records at offset {offset}")
                total_records_fetched += len(records)
                
                # Process records (they're in DESC order, newest first)
                for record in records:
                    if not isinstance(record, dict):
                        continue
                    
                    record_ts = record.get(timestamp_field)
                    if not record_ts:
                        logger.warning(f"Record missing {timestamp_field}, skipping")
                        continue
                    
                    # If record is after end_ts, skip it (too new)
                    if record_ts > end_ts:
                        continue
                    
                    # If we've reached records at or before start_ts, we're done
                    if record_ts <= start_ts:
                        reached_start_boundary = True
                        logger.info(f"Reached start boundary {start_ts}. Records in window: {len(records_in_window)}")
                        break
                    
                    # Record is within window (start_ts < record_ts <= end_ts)
                    records_in_window.append(record)
                
                # Stop if we've reached the start boundary
                if reached_start_boundary:
                    break
                
                # If we got fewer records than limit, we've reached the end of available data
                if len(records) < limit:
                    logger.info(f"Reached end of available data. Total fetched: {total_records_fetched}, in window: {len(records_in_window)}")
                    break
                
                # Move to next page
                offset += limit
                
                # Apply rate limiting between requests
                if "rate_limit_delay" in self.config:
                    delay = RestApiDataSource._parse_numeric_config(
                        self.config["rate_limit_delay"], 0.1, "rate_limit_delay"
                    )
                    time.sleep(delay)
            
            except Exception as e:
                logger.error(f"Stream API request failed at offset {offset}: {e}")
                raise
        
        # Reverse records to chronological order (oldest first) and yield
        logger.info(f"Yielding {len(records_in_window)} records in chronological order")
        for record in reversed(records_in_window):
            if isinstance(record, dict):
                # If schema is defined, only include schema fields to avoid mismatch
                if "schema" in self.config and isinstance(self.config["schema"], StructType):
                    schema_fields = {f.name for f in self.config["schema"].fields}
                    filtered_record = {k: v for k, v in record.items() if k in schema_fields}
                    if len(filtered_record) != len(schema_fields):
                        missing_fields = schema_fields - set(filtered_record.keys())
                        logger.warning(f"Record missing fields from schema: {missing_fields}. Using None for missing fields.")
                        for field_name in schema_fields:
                            filtered_record.setdefault(field_name, None)
                    yield Row(**filtered_record)
                else:
                    yield Row(**record)
            else:
                yield Row(data=str(record))
        
        logger.info(f"Streaming complete. Fetched {total_records_fetched} total, yielded {len(records_in_window)} new records")
    
    def _build_endpoint(self) -> str:
        """Build full endpoint URL, appending table_name if provided."""
        base_endpoint = self.config["endpoint"]
        table_name = self.config.get("table_name")
        
        if table_name:
            if base_endpoint.endswith("/"):
                return f"{base_endpoint}{table_name}"
            else:
                return f"{base_endpoint}/{table_name}"
        
        return base_endpoint
    
    def _get_access_token_from_refresh(self, refresh_token: str) -> str:
        """Exchange refresh token for access token (same as RestApiDataSourceReader)."""
        token_endpoint = self.config.get("token_endpoint")
        if not token_endpoint:
            raise ValueError("token_endpoint must be configured for oauth2_refresh auth type")
        
        token_method = self.config.get("token_method", "GET").upper()
        token_response_path = self.config.get("token_response_path", "result")
        
        logger.info(f"Exchanging refresh token for access token at: {token_endpoint}")
        
        try:
            response = requests.request(
                method=token_method,
                url=token_endpoint,
                headers={"Authorization": f"Bearer {refresh_token}"},
                timeout=30
            )
            response.raise_for_status()
            
            data = response.json()
            
            # Extract access token using response path
            access_token = data
            for key in token_response_path.split("."):
                access_token = access_token.get(key)
            
            if not access_token:
                raise ValueError(f"Could not extract access token from response using path: {token_response_path}")
            
            logger.info("Successfully obtained access token")
            return access_token
            
        except requests.HTTPError as e:
            logger.error(f"Failed to exchange refresh token: {e}")
            raise
    
    # Class-level cache for pre-fetched OAuth2 access tokens
    _access_token_cache: Dict[str, str] = {}
    
    def _build_headers(self) -> Dict[str, str]:
        """Build HTTP headers including authentication (same as RestApiDataSourceReader)."""
        headers = RestApiDataSource._parse_dict_config(self.config.get("headers", {}), "headers")
        auth_type = self.config.get("auth_type", "none").lower()
        
        if auth_type == "bearer":
            token = self.config.get("auth_token")
            logger.debug("Using bearer token from config")
            headers["Authorization"] = f"Bearer {token}"
                
        elif auth_type == "api_key":
            token = self.config.get("auth_token")
            header_name = self.config.get("auth_header", "X-API-Key")
            logger.debug(f"Using API key in header: {header_name}")
            headers[header_name] = token
                
        elif auth_type == "oauth2_refresh":
            # Get the cached access token that was pre-fetched in __init__
            refresh_token: str = self.config.get("auth_token")  # type: ignore
            cache_key = self._get_token_cache_key(refresh_token)
            
            access_token = RestApiDataSourceStreamReader._access_token_cache.get(cache_key)
            if not access_token:
                # Fallback: re-fetch if not in cache
                logger.warning("Access token not found in cache, re-fetching")
                access_token = self._get_access_token_from_refresh(refresh_token)
                RestApiDataSourceStreamReader._access_token_cache[cache_key] = access_token
            else:
                logger.debug("Using pre-fetched OAuth2 access token")
            
            headers["Authorization"] = f"Bearer {access_token}"
        
        return headers
    
    @staticmethod
    def _get_token_cache_key(refresh_token: str) -> str:
        """Get cache key for a refresh token (hash for security)."""
        import hashlib
        return hashlib.sha256(refresh_token.encode()).hexdigest()
    
    def _apply_rate_limit(self) -> None:
        """Apply rate limiting delay if configured."""
        if "rate_limit_delay" in self.config:
            delay = RestApiDataSource._parse_numeric_config(
                self.config["rate_limit_delay"], 0.0, "rate_limit_delay"
            )
            time.sleep(delay)
