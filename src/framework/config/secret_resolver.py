"""Secret reference resolution - handles all secret format conversions."""
from src.framework.helper import databricks_helper, logging_helper

logger = logging_helper.get_logger(__name__)


class SecretResolver:
    """Resolves secret references to their actual values.
    
    Supports multiple secret reference formats:
    - {{spark.config-key}}: Spark configuration value (for DLT pipelines)
    - {{secrets/scope/key}}: Databricks secret reference
    - secret://scope/key: Databricks secret reference
    - Plain string: Returns as-is (not a secret reference)
    """
    
    def resolve(self, reference: str) -> str:
        """Resolve a single secret reference to its actual value.
        
        Args:
            reference: Secret reference or plain value
            
        Returns:
            Resolved secret value
            
        Raises:
            ValueError: If secret reference cannot be resolved
        """
        if not reference or not isinstance(reference, str):
            return reference
        
        # Handle "{{spark.config-key}}" format (Spark config for DLT pipelines)
        if reference.startswith("{{spark.") and reference.endswith("}}"):
            return self._resolve_spark_config(reference)
        
        # Handle "secret://scope/key" format
        if reference.startswith("secret://"):
            return self._resolve_databricks_secret(reference)
        
        # Handle "{{secrets/scope/key}}" format
        if reference.startswith("{{secrets/") and reference.endswith("}}"):
            return self._resolve_databricks_secret(reference)
        
        # Not a secret reference, return as-is
        return reference
    
    def _resolve_spark_config(self, reference: str) -> str:
        """Resolve {{spark.config-key}} format from Spark configuration.
        
        Args:
            reference: Secret reference in {{spark.config-key}} format
            
        Returns:
            Resolved value from Spark config
            
        Raises:
            ValueError: If Spark config key not found
        """
        config_key = reference[8:-2]  # Remove {{spark. and }}
        logger.info(f"Resolving {{{{spark.{config_key}}}}} from Spark config")
        
        try:
            spark = databricks_helper.get_spark()
            if spark:
                # Try with and without spark. prefix
                value = spark.conf.get(f"spark.{config_key}", None)
                if not value:
                    value = spark.conf.get(config_key, None)
                
                if value:
                    logger.info(f"Successfully resolved {{{{spark.{config_key}}}}} from Spark config (length: {len(str(value))})")
                    return value
                else:
                    logger.warning(f"Spark config key 'spark.{config_key}' or '{config_key}' not found")
            else:
                logger.warning(f"Could not get Spark session to resolve {{{{spark.{config_key}}}}}")
        except Exception as e:
            logger.warning(f"Exception resolving {{{{spark.{config_key}}}}}: {e}")
        
        raise ValueError(f"Could not resolve {{{{spark.{config_key}}}}} - key not found in Spark config")
    
    def _resolve_databricks_secret(self, reference: str) -> str:
        """Resolve Databricks secret reference.
        
        Handles both formats:
        - secret://scope/key
        - {{secrets/scope/key}}
        
        Args:
            reference: Secret reference
            
        Returns:
            Secret value from Databricks
            
        Raises:
            ValueError: If secret cannot be accessed
        """
        scope, key = self._extract_scope_and_key(reference)
        logger.debug(f"Resolving Databricks secret: scope='{scope}', key='{key}'")
        return self._get_dbutils_secret(scope, key)
    
    def _extract_scope_and_key(self, reference: str) -> tuple:
        """Extract scope and key from secret reference.
        
        Args:
            reference: Secret reference in either format
            
        Returns:
            Tuple of (scope, key)
        """
        if reference.startswith("secret://"):
            path = reference.replace("secret://", "")
        elif reference.startswith("{{secrets/") and reference.endswith("}}"):
            path = reference[10:-2]  # Remove {{secrets/ and }}
        else:
            raise ValueError(f"Invalid secret reference format: {reference}")
        
        if "/" not in path:
            raise ValueError(f"Invalid secret path format: {path}. Expected 'scope/key'")
        
        scope, key = path.split("/", 1)
        return scope, key
    
    def _get_dbutils_secret(self, scope: str, key: str) -> str:
        """Get a secret from Databricks secrets using dbutils.
        
        Args:
            scope: Secret scope name
            key: Secret key name
            
        Returns:
            Secret value
            
        Raises:
            ValueError: If secret cannot be accessed
        """
        try:
            # Try direct dbutils access (works in Databricks notebooks)
            try:
                dbutils = globals().get('dbutils')
                if dbutils:
                    return dbutils.secrets.get(scope, key)
            except:
                pass
            
            # Try via PySpark context
            from pyspark.sql import SparkSession
            spark = SparkSession.getActiveSession()
            if not spark:
                spark = SparkSession.builder.getOrCreate()
            
            if spark:
                try:
                    # Access via Spark context
                    sc = spark.sparkContext
                    return sc.parallelize([1]).map(lambda x: __import__('dbutils').secrets.get(scope, key)).collect()[0]
                except:
                    pass
            
            raise ValueError(f"Could not access dbutils for secret resolution")
        except Exception as e:
            logger.error(f"Failed to resolve secret {scope}/{key}: {e}")
            raise ValueError(f"Failed to resolve secret {scope}/{key}: {e}")
