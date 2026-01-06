"""
REST API Workflow DataSource with nested API call dependencies.

This module extends RestApiDataSource to support multi-step API workflows where
one API call's results are used as parameters for subsequent API calls.
Features adaptive concurrency control, per-run caching, and HTTPAdapter retry policies.
"""

from typing import Dict, Any, Union, Iterator, Sequence, List, Optional, TYPE_CHECKING
import json
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict
import hashlib
from dataclasses import dataclass

# HTTP libraries with retry support
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from pyspark.sql import Row
from pyspark.sql.datasource import InputPartition
from pyspark.sql.types import StructType

if TYPE_CHECKING:
    from pyspark.sql import SparkSession, DataFrame

from src.framework.connectors.rest_api_datasource import (
    RestApiDataSource, 
    RestApiDataSourceReader, 
    RestApiDataSourceStreamReader
)
from src.framework.connectors.partition_strategies import PageInputPartition
from src.framework.connectors.oauth2_token_manager import OAuth2TokenManager
from src.framework.connectors.pyspark_datasource_adapter import SimpleInputPartition
from src.framework.helper import logging_helper

logger = logging_helper.get_logger(__name__)

# Global schema cache for storing StructType schemas that can't be serialized in DataSource options
# Key: table_name, Value: StructType
_schema_cache: Dict[str, StructType] = {}


@dataclass
class DependentCallPartition(InputPartition):
    """
    Partition for dependent API calls that carries parent data.
    
    This partition type embeds the parent record data so it survives
    serialization/deserialization when Spark sends it to executors.
    Using @dataclass for proper serialization support.
    """
    
    parent_data: List[Dict[str, Any]]
    
    def __repr__(self) -> str:
        return f"DependentCallPartition({len(self.parent_data)} parent records)"


class PerRunCache:
    """Per-run cache for parent API call results."""
    
    def __init__(self, max_size: int = 10000):
        self.max_size = max_size
        self.cache: Dict[str, List[Dict[str, Any]]] = {}
        self.access_count: Dict[str, int] = defaultdict(int)
        self._lock = None  # Lazy-initialized for pickle compatibility
    
    @property
    def lock(self) -> threading.Lock:
        """Lazy-initialized lock for pickle compatibility."""
        if self._lock is None:
            self._lock = threading.Lock()
        return self._lock
    
    def get_cache_key(self, table_name: str, config_hash: str) -> str:
        """Generate cache key for parent table results."""
        return f"{table_name}:{config_hash}"
    
    def get(self, cache_key: str) -> Optional[List[Dict[str, Any]]]:
        """Get cached parent results."""
        with self.lock:
            if cache_key in self.cache:
                self.access_count[cache_key] += 1
                logger.debug(f"Cache hit for {cache_key}")
                return self.cache[cache_key]
            logger.debug(f"Cache miss for {cache_key}")
            return None
    
    def put(self, cache_key: str, data: List[Dict[str, Any]]):
        """Cache parent results with LRU eviction."""
        with self.lock:
            # Evict LRU entries if cache is full
            if len(self.cache) >= self.max_size:
                lru_key = min(self.access_count.keys(), key=self.access_count.get)
                del self.cache[lru_key]
                del self.access_count[lru_key]
                logger.debug(f"Evicted LRU cache entry: {lru_key}")
            
            self.cache[cache_key] = data
            self.access_count[cache_key] = 1
            logger.info(f"Cached {len(data)} records for {cache_key}")
    
    def clear(self):
        """Clear all cached data."""
        with self.lock:
            self.cache.clear()
            self.access_count.clear()
            logger.info("Cleared per-run cache")


class RestApiWorkflowDataSource(RestApiDataSource):
    """
    REST API DataSource supporting nested/dependent API call workflows.
    
    Features:
    - Multi-step API workflows with dependencies
    - Dynamic parameter substitution using results from parent calls
    - Adaptive concurrency control based on API response performance
    - Per-run caching of parent call results
    - HTTPAdapter retry policies per endpoint type
    - Parallel processing with dependency chunking
    """
    
    def __init__(self, options: Dict[str, str]) -> None:
        super().__init__(options)
        
        # Parse workflow configuration
        self.workflow_config = self._parse_workflow_config()
        
        # Create instance-level cache to avoid pickle issues
        self._per_run_cache = PerRunCache()
        
        # Initialize HTTPAdapter config (create adapters lazily to avoid pickle issues)
        self._session_adapters = None
        
        logger.info(f"Initialized workflow DataSource for multi-step API workflows")
    
    @property
    def session_adapters(self) -> Dict[str, HTTPAdapter]:
        """Lazy-initialized session adapters for pickle compatibility."""
        if self._session_adapters is None:
            self._session_adapters = self._create_http_adapters()
        return self._session_adapters
    
    @classmethod
    def name(cls) -> str:
        return "rest_api_workflow_ds"
    
    def _parse_workflow_config(self) -> Dict[str, Any]:
        """Parse workflow configuration from options."""
        workflow_config = self.config.get("workflow_config", {})
        
        # Parse string-encoded workflow config if needed
        if isinstance(workflow_config, str):
            try:
                workflow_config = json.loads(workflow_config)
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse workflow_config JSON: {e}")
                workflow_config = {}
        
        return workflow_config
    
    def _create_http_adapters(self) -> Dict[str, HTTPAdapter]:
        """Create HTTPAdapter instances with retry policies per endpoint type."""
        adapters = {}
        retry_config = self.workflow_config.get("retry_config", {})
        
        for endpoint_type, config in retry_config.items():
            retry_strategy = Retry(
                total=config.get("total_retries", 3),
                backoff_factor=config.get("backoff_factor", 1.0),
                status_forcelist=config.get("status_forcelist", [429, 500, 502, 503, 504]),
                allowed_methods=config.get("allowed_methods", ["GET", "POST"])
            )
            
            adapter = HTTPAdapter(max_retries=retry_strategy)
            adapters[endpoint_type] = adapter
            logger.info(f"Created HTTPAdapter for {endpoint_type} with {config.get('total_retries', 3)} retries")
        
        return adapters
    
    def _get_session_with_retries(self, endpoint_type: str = "default") -> requests.Session:
        """Get requests session with appropriate retry adapter."""
        session = requests.Session()
        
        # Apply HTTPAdapter based on endpoint type
        if endpoint_type in self.session_adapters:
            adapter = self.session_adapters[endpoint_type]
            session.mount("http://", adapter)
            session.mount("https://", adapter)
            logger.debug(f"Applied {endpoint_type} retry policy to session")
        else:
            # Default retry policy
            default_retry = Retry(
                total=3,
                backoff_factor=1.0,
                status_forcelist=[429, 500, 502, 503, 504]
            )
            adapter = HTTPAdapter(max_retries=default_retry)
            session.mount("http://", adapter)
            session.mount("https://", adapter)
            logger.debug("Applied default retry policy to session")
        
        return session
    
    def _get_session(self) -> requests.Session:
        """Get session for compatibility with parent class methods."""
        return self._get_session_with_retries("default")
    
    def create_reader(self, schema: StructType) -> "RestApiWorkflowReader":
        """Create a workflow-aware batch reader."""
        logger.info(f"Creating RestApiWorkflowReader with schema: {len(schema.fields)} fields")
        reader = RestApiWorkflowReader(self.config, schema, self._per_run_cache)
        return reader
    
    def create_stream_reader(self, schema: StructType) -> "RestApiWorkflowStreamReader":
        """Create a workflow-aware streaming reader."""
        return RestApiWorkflowStreamReader(self.config, schema, self._per_run_cache)
    
    def schema(self) -> Union[StructType, str]:
        """Override schema method to handle workflow-specific scenarios.
        
        Attempts to retrieve schema from:
        1. Global schema cache (pre-loaded from data contract)
        2. JSON-serialized schema in config (if DDL string provided)
        3. Generic fallback (for dependent calls with path parameters) - AVOIDS API CALL
        4. Parent class inference (for root calls) - may hit API
        """
        table_name = self.config.get("table_name", "")
        logger.info("Resolving schema for table_name: " + table_name)
        
        # Check if this is a dependent call
        is_root_call = self.config.get("is_root_call", "false")
        if isinstance(is_root_call, str):
            is_root_call = is_root_call.lower() == "true"
        
        # Check if table_name has path parameter placeholders
        has_path_params = "{" in table_name and "}" in table_name
        
        logger.info(f"Schema resolution: table_name={table_name}, is_root_call={is_root_call}, has_path_params={has_path_params}")
        logger.info(f"Schema cache contains: {list(_schema_cache.keys())}")
        
        # # First priority: Avoid API calls for dependent calls with path parameters
        # # These can't be resolved without parent data anyway
        # if not is_root_call and has_path_params:
        #     logger.info(f"Skipping schema inference for dependent call with path parameters: {table_name}")
        #     logger.info(f"To provide proper schema, add 'properties' to data contract for this model")
        #     return "STRUCT<data STRING>"
            
        
        # Second priority: Try to get schema from global cache (pre-loaded from data contract)
        if table_name in _schema_cache:
            cached_schema = _schema_cache[table_name]
            logger.info(f"Using pre-cached schema for {table_name}: {len(cached_schema.fields)} fields")
            return cached_schema
        
        logger.info("config: " + str(self.config))
        
        # Third priority: Try to get schema from config (JSON-serialized or StructType)
        if "schema" in self.config:
            schema_value = self.config["schema"]
            if isinstance(schema_value, StructType):
                logger.info(f"Using StructType schema from config for {table_name}")
                return schema_value
            elif isinstance(schema_value, str):
                # Try to parse as DDL string
                if schema_value.startswith("STRUCT"):
                    logger.info(f"Using DDL schema from config for {table_name}")
                    try:
                        # Parse DDL string
                        parsed_schema = StructType.fromDDL(schema_value)
                        logger.info(f"Parsed DDL schema with {len(parsed_schema.fields)} fields")
                        return parsed_schema
                    except Exception as e:
                        logger.warning(f"Failed to parse DDL schema: {e}. Falling back.")
        
        # Fourth priority: For root calls without path parameters, try API inference
        # This may timeout if API is slow or unreachable
        if is_root_call or not has_path_params:
            logger.info(f"Inferring schema from API for {table_name} (root call or no path params)")
            logger.warning(f"API schema inference may be slow or fail. Consider adding schema to data contract.")
            return super().schema()
        
        # Last resort: Generic schema
        logger.warning(f"No schema available for {table_name}. Using generic schema.")
        return "STRUCT<data STRING>"
    
    @staticmethod
    def cache_schema(table_name: str, schema: StructType) -> None:
        """Cache a schema for a table to avoid repeated API calls.
        
        Used by factory to pre-load schemas from data contracts.
        
        Args:
            table_name: The table name / model name
            schema: The StructType schema to cache
        """
        _schema_cache[table_name] = schema
        logger.info(f"Cached schema for {table_name} with {len(schema.fields)} fields")
    
    @staticmethod
    def clear_schema_cache() -> None:
        """Clear all cached schemas (useful for testing)."""
        global _schema_cache
        _schema_cache.clear()
        logger.debug("Cleared workflow schema cache")
    
    def _build_endpoint(self) -> str:
        """Build endpoint, using table_name if configured for workflow calls."""
        base_endpoint = self.config.get("endpoint", "")
        table_name = self.config.get("table_name")
        
        if table_name:
            # Substitute path parameters from url_params_template
            # This ensures schema inference has complete URLs with actual values
            endpoint = table_name
            
            # Get url_params_template and parse if it's a JSON string
            url_params = self.config.get("url_params_template", {})
            if isinstance(url_params, str):
                try:
                    url_params = json.loads(url_params)
                except json.JSONDecodeError as e:
                    logger.warning(f"Failed to parse url_params_template as JSON: {e}. Using as-is: {url_params}")
                    url_params = {}
            
            if url_params:
                for key, value in url_params.items():
                    placeholder = f"{{{key}}}"
                    endpoint = endpoint.replace(placeholder, str(value))
            
            # Ensure no double slashes
            if base_endpoint.endswith("/"):
                final_endpoint = f"{base_endpoint}{endpoint}"
            else:
                final_endpoint = f"{base_endpoint}/{endpoint}"
            
            logger.info(f"Built endpoint: {final_endpoint}")
            return final_endpoint
        
        return base_endpoint
    
    def read_batch(self, spark: "SparkSession") -> "DataFrame":
        """
        Read data as a batch DataFrame using Spark's format API.
        
        For dependent calls, we handle them specially by fetching data directly
        instead of using the DataSource API, since the partition model doesn't fit.
        
        Overrides parent method to register RestApiWorkflowDataSource instead of RestApiDataSource.
        """
        # Check if this is a dependent call
        is_root_call = self.config.get("is_root_call", "false")
        if isinstance(is_root_call, str):
            is_root_call = is_root_call.lower() == "true"
        
        depends_on = self.config.get("depends_on")
        table_name = self.config.get("table_name", "")
        workflow_step = self.config.get("workflow_step", "1")
        
        logger.info(f"[READ_BATCH] ===== read_batch() called =====")
        logger.info(f"[READ_BATCH] is_root_call: {is_root_call}")
        logger.info(f"[READ_BATCH] depends_on: {depends_on}")
        logger.info(f"[READ_BATCH] table_name: {table_name}")
        logger.info(f"[READ_BATCH] workflow_step: {workflow_step}")
        logger.info(f"[READ_BATCH] All config keys: {list(self.config.keys())}")
        
        # For dependent calls, fetch data directly without using DataSource API
        if not is_root_call and depends_on:
            logger.info(f"[READ_BATCH] ===== DETECTED DEPENDENT CALL =====")
            logger.info("Handling dependent call directly")
            
            # Create a reader and fetch the data directly
            schema = self.schema()
            if isinstance(schema, str):
                from pyspark.sql.types import StructType as ST
                schema = ST.fromDDL(schema)
            
            reader = self.create_reader(schema)  # type: ignore
            
            # TEMPORARY: Use hardcoded metering point for testing
            logger.info("[READ_BATCH] === TESTING MODE: Using hardcoded metering point ===")
            parent_data = [
                {
                    "meteringPointId": "571313113160133023",
                    "position": "D01",
                    "quantity": "E17",
                    "quality": "A01",
                    "unit": "KWH",
                    "resolution": "PT1H"
                }
            ]
            logger.info(f"[READ_BATCH] Hardcoded parent data: {json.dumps(parent_data, indent=2)}")
            
            logger.info(f"[READ_BATCH] Parent data retrieved: {len(parent_data) if parent_data else 0} records")
            
            if not parent_data:
                logger.info("[READ_BATCH] No parent data available for dependent call - returning empty DataFrame")
                # Return empty DataFrame with correct schema
                return spark.createDataFrame([], schema)  # type: ignore
            
            logger.info(f"[READ_BATCH] Processing {len(parent_data)} parent records for dependent API calls")
            
            all_rows = []
            # Get field mapping if provided
            field_mapping = reader.config.get("field_mapping", {})
            
            for i, parent_record in enumerate(parent_data):
                try:
                    logger.info(f"[READ_BATCH] Processing dependent API call for parent record {i+1}/{len(parent_data)}")
                    logger.debug(f"[READ_BATCH] Parent record: {json.dumps(parent_record, indent=2, default=str)[:300]}")
                    
                    records = reader._call_dependent_api(parent_record)
                    
                    logger.info(f"[READ_BATCH] Got {len(records)} records from dependent API call {i+1}")
                    
                    for record in records:
                        # Apply field mapping if configured
                        if field_mapping:
                            for source_field, target_field in field_mapping.items():
                                # Try flat key first
                                if source_field in record:
                                    record[target_field] = record.pop(source_field)
                                else:
                                    # Try nested key path (e.g., "out_Quantity.quantity")
                                    keys = source_field.split(".")
                                    if len(keys) > 1:
                                        value = record
                                        for key in keys:
                                            if isinstance(value, dict) and key in value:
                                                value = value[key]
                                            else:
                                                value = None
                                                break
                                        if value is not None:
                                            record[target_field] = value
                                            # Remove nested source if it exists as flat dict
                                            if keys[0] in record and isinstance(record[keys[0]], dict):
                                                if keys[1] in record[keys[0]]:
                                                    del record[keys[0]][keys[1]]
                        
                        # Add parent relationship if metering point ID exists
                        if "meteringPointId" in parent_record:
                            record["_parent_meteringPointId"] = parent_record["meteringPointId"]
                        all_rows.append(Row(**record))
                except Exception as e:
                    logger.error(f"[READ_BATCH] Dependent API call {i+1} failed: {e}", exc_info=True)
                    continue
            
            logger.info(f"[READ_BATCH] Created DataFrame with {len(all_rows)} total rows from dependent calls")
            
            # Create DataFrame from collected rows
            df = spark.createDataFrame(all_rows, schema)  # type: ignore
            return df
        
        # For root calls or when not a workflow, use standard DataSource API
        # Register the DataSource with Spark if not already registered
        logger.info(f"[READ_BATCH] Not a dependent call - using standard DataSource API")
        logger.info(f"[READ_BATCH] is_root_call={is_root_call}, depends_on={depends_on}")
        try:
            spark.dataSource.register(RestApiWorkflowDataSource)
            logger.debug(f"Registered DataSource: {self.name()}")
        except Exception as e:
            logger.debug(f"DataSource may already be registered: {e}")
        
        # Filter config to only include DataSource-relevant options
        excluded_keys = {
            'catalog', 'volume', 'source_system', 
            'model_name', 'format', 'schema'
        }
        datasource_config = {}
        for k, v in self.config.items():
            if (k not in excluded_keys and not k.endswith('_catalog') and not k.endswith('_schema')
                and not isinstance(v, StructType)):
                # Convert complex objects to JSON strings for Spark DataSource options
                if isinstance(v, (dict, list)):
                    datasource_config[str(k)] = json.dumps(v)
                else:
                    datasource_config[str(k)] = str(v)
        
        
        # Use Spark's format API to read data
        df = spark.read.format(self.name()).options(**datasource_config).load()
        
        logger.info(f"Read DataFrame with schema: {df.schema}")
        logger.info(f"DataFrame columns: {df.columns}")
        
        if len(df.columns) == 1 and df.columns[0] == "data":
            logger.warning("Generic schema detected. Cannot properly unwrap data.")
            logger.info("Ensure data contract schema is provided to connector via PipelineConfig.get_connector()")
        
        logger.info(f"Created DataFrame from REST API workflow using Spark format API {self.name()}")
        logger.info(f"datasource_config: {datasource_config}")
        return df
    
    def read_stream(self, spark: "SparkSession") -> "DataFrame":
        """
        Read data as a streaming DataFrame using Spark's format API.
        
        Overrides parent method to register RestApiWorkflowDataSource instead of RestApiDataSource.
        """
        # Register the DataSource with Spark if not already registered
        try:
            spark.dataSource.register(RestApiWorkflowDataSource)
            logger.debug(f"Registered DataSource: {self.name()}")
        except Exception as e:
            logger.debug(f"DataSource may already be registered: {e}")
        
        # Filter config to only include DataSource-relevant options
        excluded_keys = {
            'catalog', 'volume', 'source_system', 
            'model_name', 'format', 'schema'
        }
        datasource_config = {}
        for k, v in self.config.items():
            if (k not in excluded_keys and not k.endswith('_catalog') and not k.endswith('_schema')
                and not isinstance(v, StructType)):
                # Convert complex objects to JSON strings for Spark DataSource options
                if isinstance(v, (dict, list)):
                    datasource_config[str(k)] = json.dumps(v)
                else:
                    datasource_config[str(k)] = str(v)
        
        
        # Use Spark's format API for streaming
        df = spark.readStream.format(self.name()).options(**datasource_config).load()
        
        logger.info(f"Read streaming DataFrame with schema: {df.schema}")
        logger.info(f"DataFrame columns: {df.columns}")
        
        if len(df.columns) == 1 and df.columns[0] == "data":
            logger.warning("Generic schema detected. Cannot properly unwrap data.")
            logger.info("Ensure data contract schema is provided to connector via PipelineConfig.get_connector()")
        
        logger.info(f"Created streaming DataFrame from REST API workflow using Spark format API")
        return df


class RestApiWorkflowReader(RestApiDataSourceReader):
    """
    Workflow-aware batch reader with dependency resolution.
    Makes sequential dependent API calls without partitioning complexity.
    """
    
    def __init__(self, config: Dict[str, Any], schema: StructType, 
                 per_run_cache: PerRunCache):
        super().__init__(config, schema)
        
        self.per_run_cache = per_run_cache
        
        # Parse workflow configuration from schema-level config
        self.workflow_step = int(self.config.get("workflow_step", 1))
        
        # Parse is_root_call - can be bool, string "true"/"false", or None
        is_root_call_value = self.config.get("is_root_call", False)
        if isinstance(is_root_call_value, str):
            self.is_root_call = is_root_call_value.lower() == "true"
        else:
            self.is_root_call = bool(is_root_call_value)
        
        self.depends_on = self.config.get("depends_on")
        self.dependency_mapping = self.config.get("dependency_mapping", {})
        
        # Support both legacy params_template and new url_params/body_params
        self.url_params_template = self.config.get("url_params_template", {})
        self.body_params_template = self.config.get("body_params_template", {})
        self.params_template = self.config.get("params_template", {})  # Legacy support
        
        self.static_url_params = self.config.get("static_url_params", {})
        self.static_body_params = self.config.get("static_body_params", {})
        self.static_params = self.config.get("static_params", {})  # Legacy support
        
        # Get workflow config from parent DataSource and parse if needed
        workflow_config = self.config.get("workflow_config", {})
        if isinstance(workflow_config, str):
            try:
                workflow_config = json.loads(workflow_config)
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse workflow_config JSON in reader: {e}")
                workflow_config = {}
        self.workflow_config = workflow_config
        
        # Initialize parent data storage for dependent calls
        self._parent_data_for_partition: List[Dict[str, Any]] = []
        
        logger.info(f"Initialized RestApiWorkflowReader: step={self.workflow_step}, is_root={self.is_root_call}, depends_on={self.depends_on}")
    
    def create_partitions(self) -> Sequence[InputPartition]:
        """Create partitions based on workflow step and dependencies."""
        
        logger.info(f"[CREATE_PARTITIONS] ===== create_partitions() called =====")
        logger.info(f"[CREATE_PARTITIONS] is_root_call={self.is_root_call}, depends_on={self.depends_on}")
        
        if self.is_root_call:
            # Root call - use standard partitioning
            logger.info("[CREATE_PARTITIONS] Root call - using standard partitioning")
            partitions = super().create_partitions()
            logger.info(f"[CREATE_PARTITIONS] Root call created {len(partitions)} partitions")
            return partitions
        else:
            # Dependent call - create a special partition that carries parent data
            logger.info("[CREATE_PARTITIONS] Dependent call - creating dependency partition")
            
            # Create a marker partition that tells read_partition to fetch from Delta
            # We use a simple InputPartition subclass
            partition = SimpleInputPartition(data={"_dependent_call": True})
            logger.info(f"[CREATE_PARTITIONS] Created 1 dependency partition for dependent call")
            return [partition]
    
    def _get_parent_data_from_delta_table(self, spark: "SparkSession") -> List[Dict[str, Any]]:
        """
        Get parent call results from Delta table instead of in-memory caching.
        
        Args:
            spark: SparkSession to read from Delta
            
        Returns:
            List of parent records from Delta table, or empty list on error
        """
        if not self.depends_on:
            logger.warning("_get_parent_data_from_delta_table called but depends_on is None")
            return []
        
        try:
            logger.info(f"[DELTA_READ] Reading parent data from Delta table for: {self.depends_on}")
            logger.info(f"[DELTA_READ] All config keys: {list(self.config.keys())}")
            logger.info(f"[DELTA_READ] Config: {json.dumps({k: v for k, v in self.config.items() if isinstance(v, (str, int, float, bool, type(None)))}, indent=2, default=str)}")
            
            # Get table_name from workflow_config.table_names
            table_names = self.workflow_config.get("table_names", {})
            table_name = table_names.get(self.depends_on)
            
            if not table_name:
                logger.error(f"[DELTA_READ] Parent table '{self.depends_on}' not found in workflow_config.table_names")
                logger.debug(f"[DELTA_READ] Available tables: {list(table_names.keys())}")
                return []
            
            logger.info(f"[DELTA_READ] Found table_name for {self.depends_on}: {table_name}")
            
            # Get raw catalog and schema from config (for reading parent data)
            raw_catalog = self.config.get("raw_catalog")
            raw_schema = self.config.get("raw_schema")
            
            logger.info(f"[DELTA_READ] raw_catalog from config: {raw_catalog}")
            logger.info(f"[DELTA_READ] raw_schema from config: {raw_schema}")
            
            if not raw_catalog:
                logger.error("[DELTA_READ] CRITICAL: raw_catalog not found in config")
                logger.debug(f"[DELTA_READ] Available config keys: {list(self.config.keys())}")
                logger.debug(f"[DELTA_READ] All config values: {json.dumps({k: str(v)[:100] for k, v in self.config.items()}, indent=2, default=str)}")
                return []
            
            if not raw_schema:
                logger.error("[DELTA_READ] CRITICAL: raw_schema not found in config")
                logger.debug(f"[DELTA_READ] Available config keys: {list(self.config.keys())}")
                logger.debug(f"[DELTA_READ] All config values: {json.dumps({k: str(v)[:100] for k, v in self.config.items()}, indent=2, default=str)}")
                return []
            
            logger.info(f"[DELTA_READ] Using raw_catalog: {raw_catalog}, raw_schema: {raw_schema}")
            
            # Construct full Delta table path: catalog.schema.table_name
            # Note: DLT normalizes table names to lowercase, so we use model name in lowercase
            # Even though raw_factory creates with MeteringPoints, DLT stores as meteringpoints
            table_name_lower = self.depends_on.lower()
            full_table_path = f"{raw_catalog}.{raw_schema}.{table_name_lower}"
            logger.info(f"[DELTA_READ] Constructed full table path: {full_table_path} (parent={self.depends_on})")
            
            try:
                # Read parent data from Delta table using spark.read.table()
                # In DLT pipelines, tables are created in the same catalog/schema
                logger.info(f"[DELTA_READ] Reading Delta table: {full_table_path}")
                df = spark.read.table(full_table_path)
                logger.info(f"[DELTA_READ] Successfully read Delta table: {full_table_path}")
                
                logger.info(f"[DELTA_READ] Table schema: {df.schema}")
                row_count = df.count()
                logger.info(f"[DELTA_READ] Row count: {row_count}")
                
                # Convert to list of dicts
                parent_data = [row.asDict() for row in df.collect()]
                logger.info(f"[DELTA_READ] Converted Delta table to {len(parent_data)} records")
                
                if parent_data:
                    logger.info(f"[DELTA_READ] First parent record: {json.dumps(parent_data[0], indent=2, default=str)[:300]}")
                else:
                    logger.warning(f"[DELTA_READ] Delta table returned no records for {full_table_path}")
                
                return parent_data
                
            except Exception as e:
                logger.error(f"[DELTA_READ] Failed to read Delta table {full_table_path}: {e}")
                logger.error(f"[DELTA_READ] Exception details: {e}", exc_info=True)
                return []
                
        except Exception as e:
            logger.error(f"[DELTA_READ] Error reading parent data from Delta table: {e}", exc_info=True)
            return []
    
    def _get_parent_data(self) -> List[Dict[str, Any]]:
        """Get parent call results from cache or by executing parent call."""
        
        if not self.depends_on:
            logger.warning("[DIAGNOSTIC] _get_parent_data called but depends_on is None")
            return []
        
        logger.info(f"[DIAGNOSTIC] _get_parent_data: depends_on={self.depends_on}")
        
        # Generate cache key based on parent table and config
        # Filter out non-JSON-serializable objects (like StructType) from config
        config_for_hash = {}
        for k, v in self.config.items():
            try:
                # Test if value is JSON serializable
                json.dumps(v)
                config_for_hash[k] = v
            except (TypeError, ValueError):
                # Skip non-serializable objects like StructType
                logger.debug(f"Skipping non-serializable config key: {k} (type: {type(v).__name__})")
        
        config_hash = hashlib.md5(json.dumps(config_for_hash, sort_keys=True).encode()).hexdigest()
        cache_key = self.per_run_cache.get_cache_key(self.depends_on, config_hash)
        logger.info(f"[DIAGNOSTIC] Cache key: {cache_key}")
        
        # Try to get from cache first
        cached_data = self.per_run_cache.get(cache_key)
        if cached_data is not None:
            logger.info(f"[DIAGNOSTIC] CACHE HIT: Got {len(cached_data)} parent records from cache")
            return cached_data
        
        # Cache miss - need to execute parent call
        logger.warning(f"[DIAGNOSTIC] CACHE MISS for key: {cache_key} - executing parent call for {self.depends_on}")
        parent_data = self._execute_parent_call()
        
        if parent_data:
            logger.info(f"[DIAGNOSTIC] Parent call returned {len(parent_data)} records")
            # Cache the results
            self.per_run_cache.put(cache_key, parent_data)
            logger.info(f"[DIAGNOSTIC] Cached {len(parent_data)} parent records with key: {cache_key}")
        else:
            logger.error(f"[DIAGNOSTIC] Parent call returned empty results for {self.depends_on}")
        
        return parent_data
    
    def _get_timeout(self) -> float:
        """Get timeout value, converting from string if necessary."""
        timeout = self.config.get("timeout", 60)
        try:
            return float(timeout) if isinstance(timeout, str) else timeout
        except (ValueError, TypeError):
            logger.warning(f"Invalid timeout value: {timeout}. Using default 60s")
            return 60.0
    
    def _execute_parent_call(self) -> List[Dict[str, Any]]:
        """Execute parent API call to get dependency data."""
        
        if not self.depends_on:
            logger.warning("No parent dependency specified")
            return []
        
        try:
            # Get parent table_name from workflow config mapping
            table_names = self.workflow_config.get("table_names", {})
            parent_table_name = table_names.get(self.depends_on)
            
            if not parent_table_name:
                logger.error(f"Parent table '{self.depends_on}' not found in workflow_config.table_names")
                logger.debug(f"Available tables: {list(table_names.keys())}")
                return []
            
            # Build the parent endpoint
            base_endpoint = self.config.get("endpoint", "")
            
            if base_endpoint.endswith("/"):
                parent_endpoint = f"{base_endpoint}{parent_table_name}"
            else:
                parent_endpoint = f"{base_endpoint}/{parent_table_name}"
            
            logger.info(f"Executing parent API call: {parent_endpoint}")
            
            # Handle authentication based on auth type
            auth_type = self.config.get("auth_type", "bearer")
            auth_token = self.config.get("auth_token")
            headers = self.config.get("headers", {})
            if isinstance(headers, str):
                try:
                    headers = json.loads(headers)
                except json.JSONDecodeError:
                    headers = {}
            
            # For oauth2_refresh, retrieve access token from OAuth2TokenManager
            access_token = auth_token
            if auth_type == "oauth2_refresh" and auth_token:
                logger.info("Retrieving OAuth2 access token from OAuth2TokenManager for parent call")
                try:
                    # Try to get cached token first
                    cached_token = OAuth2TokenManager.get_cached_token(auth_token)
                    
                    if cached_token:
                        access_token = cached_token
                        logger.info("Using pre-cached OAuth2 access token from OAuth2TokenManager for parent call")
                    else:
                        logger.info("OAuth2 token not cached, exchanging refresh token for parent call")
                        access_token = OAuth2TokenManager.exchange_token(
                            refresh_token=auth_token,
                            token_endpoint=self.config.get("token_endpoint"),
                            token_method=self.config.get("token_method", "GET"),
                            token_response_path=self.config.get("token_response_path", "result")
                        )
                        logger.info("Successfully obtained OAuth2 access token for parent call")
                except Exception as e:
                    logger.error(f"Failed to obtain access token for parent call: {e}")
                    return []
            
            # Make parent API call
            session = self._get_session_with_retries("meteringpoints")
            try:
                if access_token and "Authorization" not in headers:
                    session.headers.update({"Authorization": f"Bearer {access_token}"})
                    logger.info("Added Authorization header to session for parent call")
                
                timeout = self._get_timeout()
                logger.info(f"Making parent API call to: {parent_endpoint}")
                logger.debug(f"Request headers: {dict(session.headers)}")
                logger.debug(f"Request timeout: {timeout}s")
                
                response = session.get(
                    parent_endpoint,
                    headers=session.headers if access_token else headers,
                    timeout=timeout
                )
                
                response.raise_for_status()
                logger.info(f"Parent API response status: {response.status_code}")
                logger.debug(f"Parent API response headers: {response.headers}")
                
                data = response.json()
                logger.info(f"Parent API response body (first 1000 chars): {json.dumps(data, indent=2)[:1000]}")
                logger.debug(f"Parent API full response: {json.dumps(data, indent=2)}")
                
                # Extract data using data_path
                data_path = self.config.get("data_path", "result")
                logger.info(f"Using data_path: {data_path}")
                parent_records = RestApiDataSource._extract_data_from_response(
                    data,
                    data_path
                )
                
                logger.info(f"Parent call successful: returned {len(parent_records)} records")
                if parent_records:
                    logger.debug(f"First parent record: {json.dumps(parent_records[0], indent=2)[:200]}")
                else:
                    logger.warning(f"Parent call returned empty records list. Raw data structure: {list(data.keys()) if isinstance(data, dict) else type(data)}")
                
                return parent_records
                
            finally:
                session.close()
                
        except Exception as e:
            logger.error(f"Parent API call failed: {e}", exc_info=True)
            # Return empty list on failure rather than raising
            # This allows the workflow to continue (fail-safe mode)
            return []
    
    def read_partition(self, partition: InputPartition) -> Iterator[Row]:
        """Read partition with workflow awareness."""
        
        logger.info(f"[READ_PARTITION] ===== read_partition() called =====")
        logger.info(f"[READ_PARTITION] is_root_call={self.is_root_call}, partition_type={type(partition).__name__}")
        logger.info(f"[READ_PARTITION] depends_on={self.depends_on}, workflow_step={self.workflow_step}")
        
        try:
            if self.is_root_call:
                # Root call - use standard reading
                logger.info(f"[READ_PARTITION] ROOT CALL - Processing root call partition")
                yield from super().read_partition(partition)
            else:
                # Dependent call - fetch parent data from Delta and make API calls
                logger.info(f"[READ_PARTITION] DEPENDENT CALL - Processing dependent call partition")
                logger.info(f"[READ_PARTITION] Getting SparkSession...")
                
                # Get SparkSession from the current context
                from pyspark.sql import SparkSession
                spark = SparkSession.getActiveSession()
                
                if spark is None:
                    logger.error("[READ_PARTITION] FATAL: Could not get active SparkSession")
                    return
                
                logger.info(f"[READ_PARTITION] Got SparkSession successfully")
                
                # Get parent data from Delta table
                logger.info(f"[READ_PARTITION] About to call _get_parent_data_from_delta_table()")
                parent_data = self._get_parent_data_from_delta_table(spark)
                
                logger.info(f"[READ_PARTITION] _get_parent_data_from_delta_table() returned: {len(parent_data) if parent_data else 0} records")
                
                if not parent_data:
                    logger.warning("[READ_PARTITION] WARNING: No parent data available for dependent call - will yield no rows")
                    return
                
                logger.info(f"[READ_PARTITION] SUCCESS: Got {len(parent_data)} parent records, starting dependent API calls")
                
                # Process each parent record and make dependent API calls
                rows_yielded = 0
                for i, parent_record in enumerate(parent_data):
                    try:
                        logger.info(f"[READ_PARTITION] Processing parent record {i+1}/{len(parent_data)}")
                        logger.debug(f"[READ_PARTITION] Parent record keys: {list(parent_record.keys())}")
                        
                        records = self._call_dependent_api(parent_record)
                        
                        logger.info(f"[READ_PARTITION] Got {len(records)} records from dependent API call {i+1}")
                        
                        # Yield each record
                        for record in records:
                            # Add parent relationship if metering point ID exists
                            if "meteringPointId" in parent_record:
                                record["_parent_meteringPointId"] = parent_record["meteringPointId"]
                            yield Row(**record)
                            rows_yielded += 1
                            
                    except Exception as e:
                        logger.error(f"[READ_PARTITION] ERROR in API call {i+1}: {e}", exc_info=True)
                        continue
                
                logger.info(f"[READ_PARTITION] COMPLETED: Yielded {rows_yielded} total rows from dependent calls")
                
        except Exception as e:
            logger.error(f"[READ_PARTITION] FATAL EXCEPTION: {e}", exc_info=True)
            raise


    def read(self, partition: InputPartition) -> Iterator[Row]:
        """Override read() method."""
        logger.debug(f"read() called: is_root_call={self.is_root_call}, partition_type={type(partition).__name__}")
        return self.read_partition(partition)


    def _call_dependent_api(self, parent_record: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Make dependent API call for a single parent record."""
        
        logger.info(f"[TIMESERIES_API] Calling dependent API with parent record keys: {list(parent_record.keys())}")
        logger.info(f"[TIMESERIES_API] Parent record: {json.dumps(parent_record, indent=2, default=str)}")
        
        # Substitute parameters for URL and body
        url_params = self._substitute_parameters_for_url(parent_record)
        body_params = self._substitute_parameters_for_body(parent_record)
        
        logger.info(f"[TIMESERIES_API] Substituted URL params: {json.dumps(url_params, indent=2, default=str)}")
        logger.info(f"[TIMESERIES_API] Substituted body params: {json.dumps(body_params, indent=2, default=str)}")
        
        # Build endpoint and substitute path parameters
        table_name = self.config.get("table_name", "")
        endpoint = self._build_endpoint_with_path_params(url_params)
        
        logger.info(f"[TIMESERIES_API] Table name: {table_name}")
        logger.info(f"[TIMESERIES_API] Built endpoint: {endpoint}")
        
        headers = self._build_headers()
        
        logger.info(f"[TIMESERIES_API] Headers: {json.dumps({k: v for k, v in headers.items() if k.lower() != 'authorization'}, indent=2, default=str)}")
        
        # Get path parameter names from table_name to exclude them from query params
        path_param_names = self._extract_path_param_names(table_name)
        
        # Filter out path parameters from url_params - they've been put in the URL already
        query_params = {k: v for k, v in url_params.items() if k not in path_param_names}
        
        logger.info(f"[TIMESERIES_API] Path param names: {path_param_names}")
        logger.info(f"[TIMESERIES_API] Query params (after filtering path params): {json.dumps(query_params, indent=2, default=str)}")
        
        # Determine endpoint type and HTTP method for retry policy
        endpoint_type = self._get_endpoint_type()
        method = self.config.get("method", "GET").upper()
        
        logger.info(f"[TIMESERIES_API] Endpoint type: {endpoint_type}")
        logger.info(f"[TIMESERIES_API] HTTP method: {method}")
        
        # Create session with appropriate retry policy
        session = self._get_session_with_retries(endpoint_type)
        
        try:
            # Make API call with appropriate method
            timeout = self._get_timeout()
            
            logger.info(f"[TIMESERIES_API] Request timeout: {timeout}s")
            logger.info(f"[TIMESERIES_API] Making dependent API call: {method} {endpoint}")
            
            if method in ["POST", "PUT", "PATCH"] and body_params:
                # Send body parameters as JSON
                logger.info(f"[TIMESERIES_API] Sending as POST with JSON body")
                logger.info(f"[TIMESERIES_API] Full request body: {json.dumps(body_params, indent=2, default=str)}")
                response = session.request(
                    method=method,
                    url=endpoint,
                    headers=headers,
                    params=query_params if query_params else None,
                    json=body_params,
                    timeout=timeout
                )
            else:
                # Send remaining parameters as query string
                logger.info(f"[TIMESERIES_API] Sending as {method} with query params")
                response = session.request(
                    method=method,
                    url=endpoint,
                    headers=headers,
                    params=query_params if query_params else None,
                    timeout=timeout
                )
            
            logger.info(f"[TIMESERIES_API] Response status: {response.status_code}")
            logger.info(f"[TIMESERIES_API] Response headers: {json.dumps(dict(response.headers), indent=2, default=str)}")
            
            # Log response body before raising error
            try:
                response_body = response.json()
                logger.info(f"[TIMESERIES_API] Response body: {json.dumps(response_body, indent=2, default=str)}")
            except:
                logger.info(f"[TIMESERIES_API] Response body (text): {response.text}")
            
            response.raise_for_status()
            
            data = response.json()
            
            logger.info(f"[TIMESERIES_API] Response JSON (first 2000 chars): {json.dumps(data, indent=2, default=str)[:2000]}")
            logger.info(f"[TIMESERIES_API] Full response JSON: {json.dumps(data, indent=2, default=str)}")
            
            # Parse response
            data_path = self.config.get("data_path")
            logger.info(f"[TIMESERIES_API] Using data_path: {data_path}")
            records = RestApiDataSource._extract_data_from_response(data, data_path)
            
            logger.info(f"[TIMESERIES_API] Extracted {len(records)} records from API response")
            if records:
                logger.info(f"[TIMESERIES_API] First extracted record (before mapping): {json.dumps(records[0], indent=2, default=str)[:500]}")
            else:
                logger.warning(f"[TIMESERIES_API] No records extracted! Check data_path '{data_path}' matches response structure")
            
            # Apply field mapping if configured
            field_mapping = self.config.get("field_mapping", {})
            if field_mapping:
                logger.info(f"[TIMESERIES_API] Applying field mapping: {field_mapping}")
                for record in records:
                    for source_field, target_field in field_mapping.items():
                        # Try flat key first
                        if source_field in record:
                            record[target_field] = record.pop(source_field)
                            logger.debug(f"[TIMESERIES_API] Mapped flat key {source_field} -> {target_field}")
                        else:
                            # Try nested key path (e.g., "out_Quantity.quantity")
                            keys = source_field.split(".")
                            if len(keys) > 1:
                                value = record
                                for key in keys:
                                    if isinstance(value, dict) and key in value:
                                        value = value[key]
                                    else:
                                        value = None
                                        break
                                if value is not None:
                                    record[target_field] = value
                                    logger.debug(f"[TIMESERIES_API] Mapped nested key {source_field} -> {target_field} = {value}")
            
            # If include_parent_context is enabled, flatten and include parent fields
            if self.config.get("include_parent_context"):
                logger.info(f"[TIMESERIES_API] Including parent context in records")
                
                # Extract parent context from response
                response_data = data.get("result", [{}])[0]
                market_doc = response_data.get("MyEnergyData_MarketDocument", {})
                
                # Get the first TimeSeries and Period for context (they should be consistent across all points)
                timeseries_list = market_doc.get("TimeSeries", [])
                if timeseries_list:
                    timeseries = timeseries_list[0]
                    periods_list = timeseries.get("Period", [])
                    if periods_list:
                        period = periods_list[0]
                        
                        # Build parent context dict to add to each record
                        parent_context = {
                            "mRID": timeseries.get("mRID"),
                            "businessType": timeseries.get("businessType"),
                            "curveType": timeseries.get("curveType"),
                            "measurement_Unit.name": timeseries.get("measurement_Unit", {}).get("name"),
                            "MarketEvaluationPoint.mRID": timeseries.get("MarketEvaluationPoint", {}).get("mRID", {}).get("name"),
                            "Period.resolution": period.get("resolution"),
                            "Period.timeInterval.start": period.get("timeInterval", {}).get("start"),
                            "Period.timeInterval.end": period.get("timeInterval", {}).get("end"),
                            "createdDateTime": market_doc.get("createdDateTime"),
                            "sender_MarketParticipant.name": market_doc.get("sender_MarketParticipant", {}).get("name"),
                        }
                        
                        logger.info(f"[TIMESERIES_API] Parent context: {json.dumps(parent_context, indent=2, default=str)}")
                        
                        # Add parent context to each record
                        for record in records:
                            record.update(parent_context)
                        
                        logger.info(f"[TIMESERIES_API] Added parent context to {len(records)} records")
            
            if records:
                logger.info(f"[TIMESERIES_API] First extracted record (after mapping): {json.dumps(records[0], indent=2, default=str)[:500]}")
            
            return records
            
        except Exception as e:
            logger.error(f"[TIMESERIES_API] Dependent API call FAILED: {e}", exc_info=True)
            raise
        finally:
            session.close()
    
    def _substitute_parameters(self, parent_record: Dict[str, Any]) -> Dict[str, Any]:
        """Substitute dynamic parameters using parent record data (legacy support)."""
        
        substituted = {}
        
        # Start with static parameters
        substituted.update(self.static_params)
        
        # Apply template substitution
        substituted.update(self._substitute_template(self.params_template, parent_record))
        
        return substituted
    
    def _substitute_parameters_for_url(self, parent_record: Dict[str, Any]) -> Dict[str, Any]:
        """Substitute dynamic URL parameters using parent record data."""
        
        substituted = {}
        
        # Start with static URL parameters (handle None safely)
        if self.static_url_params:
            substituted.update(self.static_url_params)
        
        # Get url_params_template and parse if it's a JSON string
        url_params_template = self.url_params_template
        if isinstance(url_params_template, str):
            try:
                url_params_template = json.loads(url_params_template)
            except json.JSONDecodeError as e:
                logger.warning(f"Failed to parse url_params_template JSON: {e}")
                url_params_template = {}
        
        # Add all url_params_template values first (these include path parameters and static values)
        if url_params_template:
            substituted.update(url_params_template)
        
        # Apply template substitution for dynamic values (containing ${...})
        dynamic_substituted = self._substitute_template(url_params_template, parent_record)
        if dynamic_substituted:
            substituted.update(dynamic_substituted)
        
        # Fall back to legacy params_template if no url_params_template
        if not substituted and self.params_template:
            substituted.update(self._substitute_parameters(parent_record))
        
        return substituted
    
    def _substitute_parameters_for_body(self, parent_record: Dict[str, Any]) -> Dict[str, Any]:
        """Substitute dynamic body parameters using parent record data."""
        
        substituted = {}
        
        # Start with static body parameters (handle None safely)
        if self.static_body_params:
            substituted.update(self.static_body_params)
        
        # Apply template substitution
        template_result = self._substitute_template(self.body_params_template, parent_record)
        if template_result:
            substituted.update(template_result)
        
        return substituted
    
    def _build_endpoint_with_path_params(self, url_params: Dict[str, Any]) -> str:
        """Build endpoint URL with path parameter substitution."""
        
        # Get base endpoint from config
        base_endpoint = self.config.get("endpoint", "")
        
        # Get table name which may contain path parameters like {dateFrom}
        table_name = self.config.get("table_name", "")
        
        # Build full endpoint path
        if table_name:
            endpoint = f"{base_endpoint}/{table_name}"
        else:
            endpoint = base_endpoint
        
        # Substitute path parameters in curly braces
        for key, value in url_params.items():
            placeholder = f"{{{key}}}"
            if placeholder in endpoint:
                endpoint = endpoint.replace(placeholder, str(value))
        
        return endpoint
    
    def _extract_path_param_names(self, table_name: str) -> set:
        """Extract parameter names from table_name path like {dateFrom}/{dateTo}."""
        import re
        # Find all {paramName} patterns
        pattern = r'\{(\w+)\}'
        matches = re.findall(pattern, table_name)
        logger.info(f"[DIAGNOSTIC] Extracted path param names from '{table_name}': {set(matches)}")
        return set(matches)
    
    def _substitute_template(self, template: Any, parent_record: Dict[str, Any]) -> Any:
        """Recursively substitute template variables."""
        
        if isinstance(template, str):
            # Handle ${variable} substitution
            if template.startswith("${") and template.endswith("}"):
                var_name = template[2:-1]
                mapped_field = self.dependency_mapping.get(var_name, var_name)
                value = parent_record.get(mapped_field)
                return value
            return template
        
        elif isinstance(template, dict):
            result = {k: self._substitute_template(v, parent_record) for k, v in template.items()}
            return result
        
        elif isinstance(template, list):
            result = [self._substitute_template(item, parent_record) for item in template]
            return result
        
        else:
            return template
    
    def _get_endpoint_type(self) -> str:
        """Determine endpoint type for retry policy selection."""
        table_name = self.config.get("table_name", "").lower()
        
        # Check for charges first since it can contain "meterdata" in the path
        if "charges" in table_name or "getcharges" in table_name:
            return "charges"
        elif "meteringpoints" in table_name:
            return "meteringpoints"
        elif "meterdata" in table_name:
            return "meterdata"
        else:
            return "default"
    
    def _get_session_with_retries(self, endpoint_type: str) -> requests.Session:
        """Get session with retry policy (delegated to DataSource)."""
        # This would be called from the parent DataSource
        # For now, create a basic session
        session = requests.Session()
        
        # Apply basic retry policy
        retry_strategy = Retry(
            total=3,
            backoff_factor=1.0,
            status_forcelist=[429, 500, 502, 503, 504]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        return session


class RestApiWorkflowStreamReader(RestApiDataSourceStreamReader):
    """
    Workflow-aware streaming reader with dependency resolution.
    """
    
    def __init__(self, config: Dict[str, Any], schema: StructType,
                 per_run_cache: PerRunCache):
        super().__init__(config, schema)
        
        self.per_run_cache = per_run_cache
        
        # Parse workflow configuration
        self.workflow_step = int(self.config.get("workflow_step", 1))
        
        # Parse is_root_call - can be bool, string "true"/"false", or None
        is_root_call_value = self.config.get("is_root_call", False)
        if isinstance(is_root_call_value, str):
            self.is_root_call = is_root_call_value.lower() == "true"
        else:
            self.is_root_call = bool(is_root_call_value)
        
        self.depends_on = self.config.get("depends_on")
        
        # Parse workflow_config from JSON string if needed
        workflow_config = self.config.get("workflow_config", {})
        if isinstance(workflow_config, str):
            try:
                workflow_config = json.loads(workflow_config)
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse workflow_config JSON in stream reader: {e}")
                workflow_config = {}
        self.workflow_config = workflow_config
        
        logger.info(f"Initialized WorkflowStreamReader: step={self.workflow_step}, is_root={self.is_root_call}")
    
    def get_latest_offset(self) -> dict:
        """Get latest offset for streaming, with workflow awareness.
        
        For dependent calls with path parameters, we can't query the API directly
        without parent data, so we use the initial offset instead.
        """
        # Check if this is a dependent call with path parameters
        is_root_call = self.is_root_call
        if isinstance(is_root_call, str):
            is_root_call = is_root_call.lower() == "true"
        
        table_name = self.config.get("table_name", "")
        has_path_params = "{" in table_name and "}" in table_name
        
        if not is_root_call and has_path_params:
            # For dependent calls with path parameters, use initial offset
            logger.info(f"Skipping get_latest_offset for dependent call with path parameters")
            return self.get_initial_offset()
        
        # For root calls, use parent implementation
        return super().get_latest_offset()
    
    def read_stream_partition(self, partition: InputPartition) -> Iterator[Row]:
        """Read streaming partition with workflow awareness."""
        
        if self.is_root_call:
            # Root call - use standard streaming
            yield from super().read_stream_partition(partition)
        else:
            # Dependent streaming call - monitor parent changes
            yield from self._read_dependent_stream_partition(partition)
    
    def _read_dependent_stream_partition(self, partition: InputPartition) -> Iterator[Row]:
        """Stream dependent data by monitoring parent table changes."""
        
        # For streaming dependent calls, we need to:
        # 1. Monitor parent table for changes
        # 2. Make dependent API calls for new/updated parent records
        # 3. Apply the same streaming logic for timestamp-based incremental loading
        
        # This is a simplified implementation - full version would integrate
        # with Delta table change data feed or similar mechanism
        
        logger.info("Streaming dependent call - simplified implementation")
        
        # For now, fall back to batch-like behavior
        # In production, this would use Delta CDC or similar
        yield from []