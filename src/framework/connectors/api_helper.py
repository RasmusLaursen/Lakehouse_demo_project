"""
Common API call utilities with retry logic and error handling.

Provides reusable HTTP request functionality with:
- Exponential backoff retry logic
- Configurable retry conditions (status codes, exceptions)
- Request/response logging with sensitive header masking
- Timeout handling
- Support for GET, POST, PUT, DELETE, PATCH methods
- Automatic pagination support
- Flexible authentication mechanisms
"""
from typing import Dict, Any, Optional, List, Callable, Protocol
from abc import ABC, abstractmethod
from enum import Enum
import time
import base64
import requests
from requests.exceptions import RequestException, Timeout, ConnectionError

from src.framework.helper import logging_helper

logger = logging_helper.get_logger(__name__)


class AuthType(Enum):
    """Supported authentication types."""
    NONE = "none"
    API_KEY = "api_key"
    BEARER = "bearer"
    BASIC = "basic"
    OAUTH2 = "oauth2"
    CUSTOM = "custom"


class APICallError(Exception):
    """Custom exception for API call failures with detailed error context."""
    
    def __init__(
        self, 
        message: str, 
        status_code: Optional[int] = None, 
        response_text: Optional[str] = None
    ):
        """
        Initialize API call error.
        
        Args:
            message: Error message
            status_code: HTTP status code if available
            response_text: Response body text if available
        """
        super().__init__(message)
        self.status_code = status_code
        self.response_text = response_text


class APIAuthenticator(ABC):
    """
    Abstract base class for API authentication handlers.
    
    Implement this class to create custom authentication mechanisms
    that can be used with APIClient.
    """
    
    @abstractmethod
    def apply(self, headers: Dict[str, str]) -> Dict[str, str]:
        """
        Apply authentication to request headers.
        
        Args:
            headers: Existing request headers
            
        Returns:
            Headers with authentication applied
        """
        pass
    
    @abstractmethod
    def refresh_if_needed(self) -> None:
        """
        Refresh authentication credentials if needed.
        
        Called before each request to ensure credentials are valid.
        """
        pass


class NoAuth(APIAuthenticator):
    """No authentication - pass through headers unchanged."""
    
    def apply(self, headers: Dict[str, str]) -> Dict[str, str]:
        return headers
    
    def refresh_if_needed(self) -> None:
        pass


class APIKeyAuth(APIAuthenticator):
    """API Key authentication with configurable header name."""
    
    def __init__(self, api_key: str, header_name: str = "X-API-Key"):
        """
        Initialize API Key authentication.
        
        Args:
            api_key: The API key value
            header_name: Header name for the API key (default: "X-API-Key")
        """
        self.api_key = api_key
        self.header_name = header_name
    
    def apply(self, headers: Dict[str, str]) -> Dict[str, str]:
        headers[self.header_name] = self.api_key
        return headers
    
    def refresh_if_needed(self) -> None:
        pass


class BearerTokenAuth(APIAuthenticator):
    """Bearer token authentication (e.g., JWT tokens)."""
    
    def __init__(self, token: str):
        """
        Initialize Bearer token authentication.
        
        Args:
            token: The bearer token
        """
        self.token = token
    
    def apply(self, headers: Dict[str, str]) -> Dict[str, str]:
        headers["Authorization"] = f"Bearer {self.token}"
        return headers
    
    def refresh_if_needed(self) -> None:
        pass
    
    def update_token(self, new_token: str) -> None:
        """Update the bearer token."""
        self.token = new_token


class BasicAuth(APIAuthenticator):
    """HTTP Basic authentication."""
    
    def __init__(self, username: str, password: str):
        """
        Initialize Basic authentication.
        
        Args:
            username: Username
            password: Password
        """
        self.username = username
        self.password = password
        self._encoded_credentials = None
        self._encode_credentials()
    
    def _encode_credentials(self) -> None:
        """Encode credentials to base64."""
        credentials = f"{self.username}:{self.password}"
        encoded = base64.b64encode(credentials.encode()).decode()
        self._encoded_credentials = f"Basic {encoded}"
    
    def apply(self, headers: Dict[str, str]) -> Dict[str, str]:
        headers["Authorization"] = self._encoded_credentials
        return headers
    
    def refresh_if_needed(self) -> None:
        pass


class OAuth2Auth(APIAuthenticator):
    """
    OAuth2 authentication with automatic token refresh.
    
    Supports OAuth2 flows with refresh token capability.
    """
    
    def __init__(
        self,
        token_endpoint: str,
        refresh_token: str,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
        source_system: str = "default",
        token_manager: Optional[Any] = None
    ):
        """
        Initialize OAuth2 authentication.
        
        Args:
            token_endpoint: URL for token exchange
            refresh_token: Refresh token for obtaining access tokens
            client_id: OAuth2 client ID (optional)
            client_secret: OAuth2 client secret (optional)
            source_system: Source system identifier for token caching
            token_manager: Custom token manager instance (optional)
        """
        self.token_endpoint = token_endpoint
        self.refresh_token = refresh_token
        self.client_id = client_id
        self.client_secret = client_secret
        self.source_system = source_system
        self.access_token: Optional[str] = None
        
        # Use provided token manager or import default
        if token_manager:
            self.token_manager = token_manager
        else:
            from src.framework.connectors.oauth2_token_manager import OAuth2TokenManager
            self.token_manager = OAuth2TokenManager
    
    def apply(self, headers: Dict[str, str]) -> Dict[str, str]:
        if not self.access_token:
            self.refresh_if_needed()
        headers["Authorization"] = f"Bearer {self.access_token}"
        return headers
    
    def refresh_if_needed(self) -> None:
        """Refresh access token using refresh token."""
        try:
            # Try to get cached token first
            cached_token = self.token_manager.get_cached_token(
                self.refresh_token, 
                self.source_system
            )
            
            if cached_token:
                self.access_token = cached_token
                logger.debug(f"Using cached OAuth2 token for {self.source_system}")
                return
            
            # Exchange refresh token for access token
            logger.info(f"Exchanging refresh token for access token for {self.source_system}")
            self.access_token = self.token_manager.exchange_token(
                refresh_token=self.refresh_token,
                token_endpoint=self.token_endpoint,
                source_system=self.source_system,
                client_id=self.client_id,
                client_secret=self.client_secret
            )
        except Exception as e:
            logger.error(f"Failed to refresh OAuth2 token: {e}")
            raise


class CustomAuth(APIAuthenticator):
    """
    Custom authentication using a user-provided function.
    
    Allows for flexible authentication schemes not covered by standard types.
    """
    
    def __init__(self, auth_function: Callable[[Dict[str, str]], Dict[str, str]]):
        """
        Initialize custom authentication.
        
        Args:
            auth_function: Function that takes headers dict and returns modified headers
        """
        self.auth_function = auth_function
    
    def apply(self, headers: Dict[str, str]) -> Dict[str, str]:
        return self.auth_function(headers)
    
    def refresh_if_needed(self) -> None:
        pass


class APIClient:
    """
    HTTP API client with automatic retry logic and exponential backoff.
    
    Provides a reusable interface for making HTTP requests with:
    - Configurable retry behavior with exponential backoff
    - Automatic handling of transient failures
    - Request/response logging with sensitive header masking
    - Support for all common HTTP methods
    - Automatic pagination support
    
    Example:
        >>> # Create client with custom settings
        >>> client = APIClient(max_retries=5, timeout=60)
        >>> response = client.get("https://api.example.com/data")
        >>> data = response.json()
        >>> 
        >>> # Paginated requests
        >>> records = client.paginated_call(
        ...     url="https://api.example.com/data",
        ...     extract_records=lambda r: r.json()["items"]
        ... )
    """
    
    def __init__(
        self,
        timeout: int = 30,
        max_retries: int = 3,
        retry_delay: float = 1.0,
        backoff_factor: float = 2.0,
        retry_on_status: Optional[List[int]] = None,
        raise_on_error: bool = True,
        authenticator: Optional[APIAuthenticator] = None
    ):
        """
        Initialize API client with default configuration.
        
        Args:
            timeout: Default request timeout in seconds (default: 30)
            max_retries: Maximum number of retry attempts (default: 3)
            retry_delay: Initial delay between retries in seconds (default: 1.0)
            backoff_factor: Multiplier for exponential backoff (default: 2.0)
            retry_on_status: List of status codes to retry on (default: [429, 500, 502, 503, 504])
            raise_on_error: Whether to raise exception on failure (default: True)
            authenticator: Authentication handler (default: NoAuth)
        """
        self.timeout = timeout
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.backoff_factor = backoff_factor
        self.retry_on_status = retry_on_status or [429, 500, 502, 503, 504]
        self.raise_on_error = raise_on_error
        self.authenticator = authenticator or NoAuth()
    
    def request(
        self,
        url: str,
        method: str = "GET",
        headers: Optional[Dict[str, str]] = None,
        params: Optional[Dict[str, Any]] = None,
        json_body: Optional[Dict[str, Any]] = None,
        data: Optional[Any] = None,
        timeout: Optional[int] = None,
        max_retries: Optional[int] = None,
        retry_delay: Optional[float] = None,
        backoff_factor: Optional[float] = None,
        retry_on_status: Optional[List[int]] = None,
        raise_on_error: Optional[bool] = None
    ) -> requests.Response:
        """
        Make an HTTP API call with automatic retry logic and exponential backoff.
        
        Args:
            url: The URL to call
            method: HTTP method (GET, POST, PUT, DELETE, PATCH)
            headers: Optional request headers
            params: Optional query parameters
            json_body: Optional JSON body for POST/PUT/PATCH requests
            data: Optional data payload (alternative to json_body)
            timeout: Request timeout in seconds (uses instance default if None)
            max_retries: Maximum retry attempts (uses instance default if None)
            retry_delay: Initial retry delay (uses instance default if None)
            backoff_factor: Exponential backoff multiplier (uses instance default if None)
            retry_on_status: Status codes to retry (uses instance default if None)
            raise_on_error: Whether to raise on failure (uses instance default if None)
            
        Returns:
            requests.Response object
            
        Raises:
            APICallError: If the request fails after all retries (when raise_on_error=True)
        """
        # Use instance defaults if not provided
        timeout = timeout if timeout is not None else self.timeout
        max_retries = max_retries if max_retries is not None else self.max_retries
        retry_delay = retry_delay if retry_delay is not None else self.retry_delay
        backoff_factor = backoff_factor if backoff_factor is not None else self.backoff_factor
        retry_on_status = retry_on_status if retry_on_status is not None else self.retry_on_status
        raise_on_error = raise_on_error if raise_on_error is not None else self.raise_on_error
        
        method = method.upper()
        headers = headers or {}
        
        # Refresh authentication if needed
        self.authenticator.refresh_if_needed()
        
        # Apply authentication to headers
        headers = self.authenticator.apply(headers.copy())
        
        last_exception: Optional[Exception] = None
        last_status_code: Optional[int] = None
        last_response_text: Optional[str] = None
        
        for attempt in range(max_retries + 1):
            try:
                # Log the request (exclude sensitive headers)
                safe_headers = {k: ("***" if k.lower() in ["authorization", "api-key", "x-api-key"] else v) 
                              for k, v in headers.items()}
                
                if attempt == 0:
                    logger.info(f"API {method} request to: {url}")
                    logger.debug(f"Headers: {safe_headers}, Params: {params}")
                else:
                    logger.info(f"Retry attempt {attempt}/{max_retries} for {method} {url}")
                
                # Make the request
                response = requests.request(
                    method=method,
                    url=url,
                    headers=headers,
                    params=params,
                    json=json_body,
                    data=data,
                    timeout=timeout
                )
                
                # Log response status
                logger.debug(f"Response status: {response.status_code}")
                
                # Check if we should retry based on status code
                if response.status_code in retry_on_status:
                    last_status_code = response.status_code
                    last_response_text = response.text[:500]  # First 500 chars
                    
                    if attempt < max_retries:
                        delay = retry_delay * (backoff_factor ** attempt)
                        logger.warning(
                            f"Received status {response.status_code}, retrying in {delay:.2f}s "
                            f"(attempt {attempt + 1}/{max_retries})"
                        )
                        time.sleep(delay)
                        continue
                    
                    # Max retries reached
                    error_msg = f"API call failed after {max_retries} retries. Status: {response.status_code}, URL: {url}"
                    logger.error(error_msg)
                    if raise_on_error:
                        raise APICallError(error_msg, response.status_code, response.text)
                    return response
                
                # Success!
                logger.info(f"API call successful: {method} {url} -> {response.status_code}")
                return response
                
            except Timeout as e:
                last_exception = e
                logger.warning(f"Request timeout after {timeout}s: {url}")
                
                if attempt < max_retries:
                    delay = retry_delay * (backoff_factor ** attempt)
                    logger.info(f"Retrying after timeout in {delay:.2f}s (attempt {attempt + 1}/{max_retries})")
                    time.sleep(delay)
                    continue
                
                error_msg = f"Request timeout after {max_retries} retries: {url}"
                logger.error(error_msg)
                if raise_on_error:
                    raise APICallError(error_msg) from e
                raise
                    
            except (ConnectionError, RequestException) as e:
                last_exception = e
                logger.warning(f"Connection error: {type(e).__name__}: {str(e)}")
                
                if attempt < max_retries:
                    delay = retry_delay * (backoff_factor ** attempt)
                    logger.info(f"Retrying after connection error in {delay:.2f}s (attempt {attempt + 1}/{max_retries})")
                    time.sleep(delay)
                    continue
                
                error_msg = f"Connection failed after {max_retries} retries: {url}"
                logger.error(error_msg)
                if raise_on_error:
                    raise APICallError(error_msg) from e
                raise
        
        # Fallback: should not reach here
        if last_exception:
            error_msg = f"API call failed after {max_retries} retries"
            raise APICallError(error_msg) from last_exception if raise_on_error else last_exception
        
        if last_status_code:
            error_msg = f"API call failed after {max_retries} retries"
            if raise_on_error:
                raise APICallError(error_msg, last_status_code, last_response_text)
        
        raise APICallError(f"API call failed after {max_retries} retries")
    
    def get(
        self, 
        url: str, 
        headers: Optional[Dict[str, str]] = None, 
        params: Optional[Dict[str, Any]] = None, 
        **kwargs
    ) -> requests.Response:
        """Make a GET request with retry logic."""
        return self.request(url, method="GET", headers=headers, params=params, **kwargs)
    
    def post(
        self, 
        url: str, 
        headers: Optional[Dict[str, str]] = None,
        json_body: Optional[Dict[str, Any]] = None, 
        **kwargs
    ) -> requests.Response:
        """Make a POST request with retry logic."""
        return self.request(url, method="POST", headers=headers, json_body=json_body, **kwargs)
    
    def put(
        self, 
        url: str, 
        headers: Optional[Dict[str, str]] = None,
        json_body: Optional[Dict[str, Any]] = None, 
        **kwargs
    ) -> requests.Response:
        """Make a PUT request with retry logic."""
        return self.request(url, method="PUT", headers=headers, json_body=json_body, **kwargs)
    
    def delete(
        self, 
        url: str, 
        headers: Optional[Dict[str, str]] = None, 
        **kwargs
    ) -> requests.Response:
        """Make a DELETE request with retry logic."""
        return self.request(url, method="DELETE", headers=headers, **kwargs)
    
    def patch(
        self, 
        url: str, 
        headers: Optional[Dict[str, str]] = None,
        json_body: Optional[Dict[str, Any]] = None, 
        **kwargs
    ) -> requests.Response:
        """Make a PATCH request with retry logic."""
        return self.request(url, method="PATCH", headers=headers, json_body=json_body, **kwargs)
    
    def paginated_call(
        self,
        url: str,
        method: str = "GET",
        headers: Optional[Dict[str, str]] = None,
        params: Optional[Dict[str, Any]] = None,
        json_body: Optional[Dict[str, Any]] = None,
        max_pages: int = 100,
        page_size_param: str = "limit",
        page_offset_param: str = "offset",
        page_size: int = 100,
        extract_records: Optional[Callable[[requests.Response], List[Dict]]] = None,
        **retry_kwargs
    ) -> List[Dict[str, Any]]:
        """
        Make paginated API calls and collect all results.
        
        Args:
            url: Base URL for the API
            method: HTTP method
            headers: Request headers
            params: Base query parameters
            json_body: Request body (for POST)
            max_pages: Maximum number of pages to fetch
            page_size_param: Query parameter name for page size (default: "limit")
            page_offset_param: Query parameter name for offset (default: "offset")
            page_size: Number of records per page
            extract_records: Function to extract records from response (default: response.json())
            **retry_kwargs: Additional arguments for request method
            
        Returns:
            List of all records from all pages
            
        Example:
            >>> client = APIClient()
            >>> records = client.paginated_call(
            ...     url="https://api.example.com/data",
            ...     page_size=100,
            ...     extract_records=lambda r: r.json().get("records", [])
            ... )
        """
        all_records = []
        params = params or {}
        offset = 0
        
        for page_num in range(max_pages):
            # Update pagination parameters
            params[page_size_param] = page_size
            params[page_offset_param] = offset
            
            logger.info(f"Fetching page {page_num + 1}, offset={offset}, limit={page_size}")
            
            # Make the request
            response = self.request(
                url=url,
                method=method,
                headers=headers,
                params=params,
                json_body=json_body,
                **retry_kwargs
            )
            
            # Extract records
            if extract_records:
                records = extract_records(response)
            else:
                data = response.json()
                if isinstance(data, list):
                    records = data
                elif isinstance(data, dict) and "records" in data:
                    records = data["records"]
                else:
                    records = [data]
            
            if not records:
                logger.info(f"No more records found at offset {offset}, stopping pagination")
                break
            
            logger.debug(f"Retrieved {len(records)} records from page {page_num + 1}")
            all_records.extend(records)
            
            # Check if we got fewer records than requested (last page)
            if len(records) < page_size:
                logger.info(f"Retrieved {len(records)} records (< {page_size}), assuming last page")
                break
            
            offset += page_size
        
        logger.info(f"Total records retrieved: {len(all_records)} across {page_num + 1} pages")
        return all_records


# Default client instance for backward compatibility
_default_client = APIClient()


# Backward compatible standalone functions
def api_call_with_retry(
    url: str,
    method: str = "GET",
    headers: Optional[Dict[str, str]] = None,
    params: Optional[Dict[str, Any]] = None,
    json_body: Optional[Dict[str, Any]] = None,
    data: Optional[Any] = None,
    timeout: int = 30,
    max_retries: int = 3,
    retry_delay: float = 1.0,
    backoff_factor: float = 2.0,
    retry_on_status: Optional[List[int]] = None,
    raise_on_error: bool = True
) -> requests.Response:
    """
    Make an HTTP API call with automatic retry logic and exponential backoff.
    
    This is a backward compatible wrapper around APIClient.request().
    For new code, consider using APIClient directly.
    
    Args:
        url: The URL to call
        method: HTTP method (GET, POST, PUT, DELETE, PATCH)
        headers: Optional request headers
        params: Optional query parameters
        json_body: Optional JSON body for POST/PUT/PATCH requests
        data: Optional data payload (alternative to json_body)
        timeout: Request timeout in seconds
        max_retries: Maximum number of retry attempts (default: 3)
        retry_delay: Initial delay between retries in seconds (default: 1.0)
        backoff_factor: Multiplier for exponential backoff (default: 2.0)
        retry_on_status: List of status codes to retry on (default: [429, 500, 502, 503, 504])
        raise_on_error: Whether to raise exception on failure (default: True)
        
    Returns:
        requests.Response object
        
    Raises:
        APICallError: If the request fails after all retries (when raise_on_error=True)
    """
    return _default_client.request(
        url=url,
        method=method,
        headers=headers,
        params=params,
        json_body=json_body,
        data=data,
        timeout=timeout,
        max_retries=max_retries,
        retry_delay=retry_delay,
        backoff_factor=backoff_factor,
        retry_on_status=retry_on_status,
        raise_on_error=raise_on_error
    )


def api_get(
    url: str, 
    headers: Optional[Dict[str, str]] = None, 
    params: Optional[Dict[str, Any]] = None, 
    **kwargs
) -> requests.Response:
    """Convenience method for GET requests with retry logic."""
    return _default_client.get(url, headers=headers, params=params, **kwargs)


def api_post(
    url: str, 
    headers: Optional[Dict[str, str]] = None,
    json_body: Optional[Dict[str, Any]] = None, 
    **kwargs
) -> requests.Response:
    """Convenience method for POST requests with retry logic."""
    return _default_client.post(url, headers=headers, json_body=json_body, **kwargs)


def api_put(
    url: str, 
    headers: Optional[Dict[str, str]] = None,
    json_body: Optional[Dict[str, Any]] = None, 
    **kwargs
) -> requests.Response:
    """Convenience method for PUT requests with retry logic."""
    return _default_client.put(url, headers=headers, json_body=json_body, **kwargs)


def api_delete(
    url: str, 
    headers: Optional[Dict[str, str]] = None, 
    **kwargs
) -> requests.Response:
    """Convenience method for DELETE requests with retry logic."""
    return _default_client.delete(url, headers=headers, **kwargs)


def paginated_api_call(
    url: str,
    method: str = "GET",
    headers: Optional[Dict[str, str]] = None,
    params: Optional[Dict[str, Any]] = None,
    json_body: Optional[Dict[str, Any]] = None,
    max_pages: int = 100,
    page_size_param: str = "limit",
    page_offset_param: str = "offset",
    page_size: int = 100,
    extract_records: Optional[Callable[[requests.Response], List[Dict]]] = None,
    **retry_kwargs
) -> List[Dict[str, Any]]:
    """
    Make paginated API calls and collect all results.
    
    This is a backward compatible wrapper around APIClient.paginated_call().
    For new code, consider using APIClient directly.
    
    Args:
        url: Base URL for the API
        method: HTTP method
        headers: Request headers
        params: Base query parameters
        json_body: Request body (for POST)
        max_pages: Maximum number of pages to fetch
        page_size_param: Query parameter name for page size (default: "limit")
        page_offset_param: Query parameter name for offset (default: "offset")
        page_size: Number of records per page
        extract_records: Function to extract records from response (default: response.json())
        **retry_kwargs: Additional arguments for api_call_with_retry
        
    Returns:
        List of all records from all pages
    """
    return _default_client.paginated_call(
        url=url,
        method=method,
        headers=headers,
        params=params,
        json_body=json_body,
        max_pages=max_pages,
        page_size_param=page_size_param,
        page_offset_param=page_offset_param,
        page_size=page_size,
        extract_records=extract_records,
        **retry_kwargs
    )
