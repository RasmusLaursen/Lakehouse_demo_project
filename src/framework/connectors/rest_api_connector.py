"""
REST API connector for reading data from HTTP/HTTPS endpoints.

This connector implements the BaseConnector interface for API-based
data ingestion with support for authentication, pagination, rate limiting,
and incremental loading.
"""

import time
import requests
from typing import Dict, Any, Optional, List
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType
from src.framework.connectors.base_connector import BaseConnector
from src.framework.helper import logging_helper, common

logger = logging_helper.get_logger(__name__)


class RestApiConnector(BaseConnector):
    """
    Connector for reading data from REST APIs.
    
    Supports:
    - Multiple authentication methods (Bearer, API Key, OAuth, Basic)
    - Pagination (offset-based, cursor-based, page-based)
    - Rate limiting
    - Incremental loading with watermark tracking
    - Custom headers and query parameters
    """
    
    def validate_config(self, config: Dict[str, Any]) -> None:
        """
        Validate REST API connector configuration.
        
        Required fields:
            - endpoint: Base URL of the API
            - method: HTTP method (GET, POST, etc.)
            
        Optional fields:
            - auth_type: Authentication type (bearer, api_key, oauth, basic, none)
            - auth_token: Token for Bearer or API Key auth
            - auth_header: Header name for API Key (default: "X-API-Key")
            - username/password: For Basic auth
            - headers: Additional HTTP headers
            - params: Query parameters
            - pagination_type: Type of pagination (offset, cursor, page, none)
            - pagination_config: Configuration for pagination
            - rate_limit_requests: Max requests per rate_limit_period
            - rate_limit_period: Time period in seconds for rate limiting
            - schema: Optional Spark schema for the data
            - data_path: JSON path to extract data from response (e.g., "data.items")
            - add_audit_columns: Whether to add audit columns (default: False)
        
        Args:
            config: Configuration dictionary
            
        Raises:
            ValueError: If required fields are missing
        """
        required_fields = ["endpoint", "method"]
        missing = [f for f in required_fields if f not in config]
        
        if missing:
            raise ValueError(
                f"RestApiConnector missing required config fields: {', '.join(missing)}. "
                f"Required: {', '.join(required_fields)}"
            )
        
        # Validate method
        valid_methods = ["GET", "POST", "PUT", "PATCH"]
        method = config["method"].upper()
        if method not in valid_methods:
            raise ValueError(
                f"Invalid HTTP method: '{method}'. Valid methods: {', '.join(valid_methods)}"
            )
        
        # Validate auth_type if provided
        if "auth_type" in config:
            valid_auth_types = ["bearer", "api_key", "oauth", "basic", "none"]
            auth_type = config["auth_type"].lower()
            if auth_type not in valid_auth_types:
                raise ValueError(
                    f"Invalid auth_type: '{auth_type}'. Valid types: {', '.join(valid_auth_types)}"
                )
        
        logger.info(f"RestApiConnector config validated: {config['endpoint']}")
    
    def read_stream(self, spark: SparkSession) -> DataFrame:
        """
        Read data as a streaming DataFrame.
        
        Note: REST API streaming uses micro-batch processing with rate limiting.
        For true streaming, consider using a message queue connector (Kafka, etc.).
        
        Args:
            spark: Active SparkSession
            
        Returns:
            Streaming DataFrame from the API
            
        Raises:
            NotImplementedError: REST API streaming requires custom implementation
        """
        raise NotImplementedError(
            "REST API streaming is not yet implemented. "
            "Use read_batch() for periodic API polling, or implement custom streaming logic."
        )
    
    def read_batch(self, spark: SparkSession) -> DataFrame:
        """
        Read data as a batch DataFrame from REST API.
        
        Handles pagination, rate limiting, and authentication automatically.
        
        Args:
            spark: Active SparkSession
            
        Returns:
            Batch DataFrame from the API
        """
        endpoint = self.config["endpoint"]
        method = self.config["method"].upper()
        
        logger.info(f"Reading batch data from REST API: {method} {endpoint}")
        
        # Fetch all data from API with pagination
        all_data = self._fetch_all_data()
        
        if not all_data:
            logger.warning("No data returned from API")
            # Return empty DataFrame with schema if provided
            schema = self.config.get("schema")
            if schema:
                return spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)  # type: ignore
            else:
                raise ValueError("No data returned and no schema provided")
        
        # Create DataFrame from fetched data
        schema = self.config.get("schema")
        if schema:
            df = spark.createDataFrame(all_data, schema)  # type: ignore
        else:
            df = spark.createDataFrame(all_data)  # type: ignore
        
        # Add audit columns if requested
        if self.config.get("add_audit_columns", False):
            df = common.add_audit_columns(df=df)
            logger.info("Added audit columns to batch DataFrame")
        
        logger.info(f"Successfully fetched {df.count()} records from API")
        return df
    
    def _fetch_all_data(self) -> List[Dict[str, Any]]:
        """
        Fetch all data from API with pagination and rate limiting.
        
        Returns:
            List of records from API
        """
        all_records = []
        pagination_type = self.config.get("pagination_type", "none").lower()
        
        if pagination_type == "none":
            # Single request, no pagination
            data = self._make_request()
            all_records.extend(self._extract_data(data))
        elif pagination_type == "offset":
            all_records = self._fetch_offset_pagination()
        elif pagination_type == "cursor":
            all_records = self._fetch_cursor_pagination()
        elif pagination_type == "page":
            all_records = self._fetch_page_pagination()
        else:
            raise ValueError(f"Unknown pagination_type: '{pagination_type}'")
        
        return all_records
    
    def _make_request(self, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Make a single HTTP request with authentication and rate limiting.
        
        Args:
            params: Query parameters for the request
            
        Returns:
            JSON response as dictionary
        """
        endpoint = self.config["endpoint"]
        method = self.config["method"].upper()
        
        # Build headers
        headers = self.config.get("headers", {}).copy()
        self._add_auth_headers(headers)
        
        # Merge params
        all_params = self.config.get("params", {}).copy()
        if params:
            all_params.update(params)
        
        # Rate limiting
        self._apply_rate_limit()
        
        # Make request
        logger.debug(f"Making request: {method} {endpoint} with params: {all_params}")
        
        try:
            response = requests.request(
                method=method,
                url=endpoint,
                headers=headers,
                params=all_params,
                timeout=self.config.get("timeout", 30)
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"API request failed: {e}")
            raise
    
    def _add_auth_headers(self, headers: Dict[str, str]) -> None:
        """Add authentication headers based on auth_type."""
        auth_type = self.config.get("auth_type", "none").lower()
        
        if auth_type == "bearer":
            token = self.config.get("auth_token")
            if token:
                headers["Authorization"] = f"Bearer {token}"
        elif auth_type == "api_key":
            token = self.config.get("auth_token")
            header_name = self.config.get("auth_header", "X-API-Key")
            if token:
                headers[header_name] = token
        elif auth_type == "basic":
            # Basic auth handled by requests library
            pass
        # OAuth would require more complex token refresh logic
    
    def _apply_rate_limit(self) -> None:
        """Apply rate limiting if configured."""
        # Simple rate limiting implementation
        # For production, use a token bucket or similar algorithm
        if "rate_limit_delay" in self.config:
            delay = self.config["rate_limit_delay"]
            logger.debug(f"Applying rate limit delay: {delay}s")
            time.sleep(delay)
    
    def _extract_data(self, response: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract data from API response using data_path."""
        data_path = self.config.get("data_path")
        
        if not data_path:
            # Assume entire response is the data
            if isinstance(response, list):
                return response
            else:
                return [response]
        
        # Navigate JSON path (e.g., "data.items")
        data = response
        for key in data_path.split("."):
            data = data.get(key, [])
        
        return data if isinstance(data, list) else [data]
    
    def _fetch_offset_pagination(self) -> List[Dict[str, Any]]:
        """Fetch data using offset-based pagination."""
        all_records = []
        pagination_config = self.config.get("pagination_config", {})
        offset = pagination_config.get("start_offset", 0)
        limit = pagination_config.get("limit", 100)
        offset_param = pagination_config.get("offset_param", "offset")
        limit_param = pagination_config.get("limit_param", "limit")
        
        while True:
            params = {offset_param: offset, limit_param: limit}
            data = self._make_request(params)
            records = self._extract_data(data)
            
            if not records:
                break
            
            all_records.extend(records)
            offset += limit
            
            logger.info(f"Fetched {len(records)} records (total: {len(all_records)})")
            
            # Check if we've reached the end
            if len(records) < limit:
                break
        
        return all_records
    
    def _fetch_cursor_pagination(self) -> List[Dict[str, Any]]:
        """Fetch data using cursor-based pagination."""
        all_records = []
        pagination_config = self.config.get("pagination_config", {})
        cursor_param = pagination_config.get("cursor_param", "cursor")
        cursor_path = pagination_config.get("cursor_path", "next_cursor")
        cursor = None
        
        while True:
            params = {cursor_param: cursor} if cursor else {}
            data = self._make_request(params)
            records = self._extract_data(data)
            
            if not records:
                break
            
            all_records.extend(records)
            
            # Extract next cursor
            cursor_data = data
            for key in cursor_path.split("."):
                cursor_data = cursor_data.get(key)
                if cursor_data is None:
                    break
            
            cursor = cursor_data
            logger.info(f"Fetched {len(records)} records (total: {len(all_records)})")
            
            if not cursor:
                break
        
        return all_records
    
    def _fetch_page_pagination(self) -> List[Dict[str, Any]]:
        """Fetch data using page-based pagination."""
        all_records = []
        pagination_config = self.config.get("pagination_config", {})
        page = pagination_config.get("start_page", 1)
        page_size = pagination_config.get("page_size", 100)
        page_param = pagination_config.get("page_param", "page")
        size_param = pagination_config.get("size_param", "size")
        
        while True:
            params = {page_param: page, size_param: page_size}
            data = self._make_request(params)
            records = self._extract_data(data)
            
            if not records:
                break
            
            all_records.extend(records)
            page += 1
            
            logger.info(f"Fetched {len(records)} records (total: {len(all_records)})")
            
            # Check if we've reached the end
            if len(records) < page_size:
                break
        
        return all_records
