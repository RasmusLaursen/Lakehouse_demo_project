"""
OAuth2 Token Manager for REST API authentication.

This module provides centralized OAuth2 token exchange and caching,
enabling pre-loading of tokens before DataSource initialization.

Features:
- Token exchange via refresh token flow
- Global token cache with consistent hashing
- Error handling and logging
- Support for configurable token endpoints and response paths
"""

from typing import Dict, Optional
import requests
import hashlib
from src.framework.helper import get_logger

logger = get_logger(__name__)


class OAuth2TokenManager:
    """
    Centralized manager for OAuth2 token exchange and caching.
    
    This singleton-like class manages:
    - Token exchange from refresh tokens to access tokens
    - Global token cache shared across all connectors
    - Consistent cache key generation
    - Error handling and validation
    
    Can be used at factory/pipeline level to pre-load tokens
    before DataSource/Reader initialization.
    """
    
    # Global token cache: refresh_token_hash -> access_token
    # Shared across all datasources, readers, and workflows
    _token_cache: Dict[str, str] = {}
    
    @staticmethod
    def exchange_token(
        refresh_token: str,
        token_endpoint: str,
        token_method: str = "GET",
        token_response_path: str = "result",
        timeout: int = 30
    ) -> str:
        """
        Exchange a refresh token for an access token via OAuth2 flow.
        
        Caches the access token globally for reuse across all readers
        and data sources in this process/session.
        
        Args:
            refresh_token: The OAuth2 refresh token
            token_endpoint: URL to token endpoint that exchanges refresh token
            token_method: HTTP method for token endpoint (GET or POST)
            token_response_path: Dot-separated path to access token in response
            timeout: Request timeout in seconds
            
        Returns:
            Access token string
            
        Raises:
            ValueError: If token endpoint is missing or response invalid
            requests.HTTPError: If token endpoint returns error
            
        Example:
            >>> token = OAuth2TokenManager.exchange_token(
            ...     refresh_token="abc123",
            ...     token_endpoint="https://api.example.com/token",
            ...     token_method="GET",
            ...     token_response_path="result"
            ... )
        """
        if not token_endpoint:
            raise ValueError("token_endpoint must be provided for OAuth2 token exchange")
        
        # Generate cache key from refresh token hash
        cache_key = OAuth2TokenManager.get_cache_key(refresh_token)
        
        # Check if token already cached
        if cache_key in OAuth2TokenManager._token_cache:
            logger.info(f"OAuth2 token already cached, returning existing token")
            return OAuth2TokenManager._token_cache[cache_key]
        
        logger.info(f"Exchanging OAuth2 refresh token at: {token_endpoint}")
        
        try:
            # Make token exchange request
            response = requests.request(
                method=token_method.upper(),
                url=token_endpoint,
                headers={"Authorization": f"Bearer {refresh_token}"},
                timeout=timeout
            )
            response.raise_for_status()
            
            # Parse response
            data = response.json()
            logger.debug(f"Token exchange response received from {token_endpoint}")
            
            # Extract access token using response path
            access_token = data
            for key in token_response_path.split("."):
                if not isinstance(access_token, dict):
                    raise ValueError(
                        f"Invalid token response structure: expected dict at '{key}', "
                        f"got {type(access_token).__name__}"
                    )
                access_token = access_token.get(key)
            
            if not access_token:
                raise ValueError(
                    f"Could not extract access token from response using path: {token_response_path}. "
                    f"Response keys: {list(data.keys()) if isinstance(data, dict) else 'not a dict'}"
                )
            
            # Cache the token
            OAuth2TokenManager._token_cache[cache_key] = access_token
            logger.info(f"Successfully obtained and cached OAuth2 access token")
            
            return access_token
            
        except requests.HTTPError as e:
            logger.error(f"Token endpoint returned error: {e.response.status_code}")
            raise
        except Exception as e:
            logger.error(f"Failed to exchange OAuth2 refresh token: {e}")
            raise
    
    @staticmethod
    def get_cached_token(refresh_token: str) -> Optional[str]:
        """
        Retrieve cached access token for a refresh token.
        
        Returns None if token not in cache (indicating token exchange needed).
        
        Args:
            refresh_token: The OAuth2 refresh token
            
        Returns:
            Cached access token, or None if not cached
            
        Example:
            >>> token = OAuth2TokenManager.get_cached_token("abc123")
            >>> if token is None:
            ...     # Token not cached, exchange needed
        """
        cache_key = OAuth2TokenManager.get_cache_key(refresh_token)
        token = OAuth2TokenManager._token_cache.get(cache_key)
        
        if token:
            logger.debug(f"Retrieved cached OAuth2 access token")
        else:
            logger.debug(f"OAuth2 access token not in cache for key: {cache_key}")
        
        return token
    
    @staticmethod
    def cache_token(refresh_token: str, access_token: str) -> None:
        """
        Manually cache an access token.
        
        Useful for externally obtained tokens or testing.
        
        Args:
            refresh_token: The OAuth2 refresh token
            access_token: The access token to cache
            
        Example:
            >>> OAuth2TokenManager.cache_token("abc123", "token_xyz")
        """
        cache_key = OAuth2TokenManager.get_cache_key(refresh_token)
        OAuth2TokenManager._token_cache[cache_key] = access_token
        logger.info(f"Manually cached OAuth2 access token for key: {cache_key}")
    
    @staticmethod
    def get_cache_key(refresh_token: str) -> str:
        """
        Generate consistent cache key from refresh token.
        
        Uses SHA256 hash to:
        - Prevent token exposure in logs
        - Provide consistent key for same token
        - Support different tokens with different keys
        
        Args:
            refresh_token: The OAuth2 refresh token
            
        Returns:
            Hexadecimal string hash of token
            
        Example:
            >>> key = OAuth2TokenManager.get_cache_key("my_refresh_token")
            >>> len(key)
            64  # SHA256 produces 64 hex characters
        """
        return hashlib.sha256(refresh_token.encode()).hexdigest()
    
    @staticmethod
    def clear_cache() -> None:
        """
        Clear all cached tokens.
        
        Useful for testing or between runs.
        
        Example:
            >>> OAuth2TokenManager.clear_cache()
        """
        count = len(OAuth2TokenManager._token_cache)
        OAuth2TokenManager._token_cache.clear()
        logger.info(f"Cleared OAuth2 token cache ({count} tokens removed)")
    
    @staticmethod
    def get_cache_stats() -> Dict[str, int]:
        """
        Get statistics about the token cache.
        
        Returns:
            Dictionary with cache metrics
            
        Example:
            >>> stats = OAuth2TokenManager.get_cache_stats()
            >>> print(f"Cached tokens: {stats['count']}")
        """
        return {
            "count": len(OAuth2TokenManager._token_cache),
            "keys": list(OAuth2TokenManager._token_cache.keys())
        }
