"""
JDBC connector for reading data from relational databases.

This connector implements the BaseConnector interface for database
ingestion with support for multiple database types, partitioning,
and incremental loading.
"""

from typing import Dict, Any, Optional
from pyspark.sql import SparkSession, DataFrame
from src.framework.connectors.base_connector import BaseConnector
from src.framework.helper import logging_helper, add_audit_columns

logger = logging_helper.get_logger(__name__)


class JdbcConnector(BaseConnector):
    """
    Connector for reading data from relational databases via JDBC.
    
    Supports:
    - Multiple database types (PostgreSQL, MySQL, SQL Server, Oracle, etc.)
    - Partitioned reading for large tables
    - Incremental loading with watermark columns
    - Custom SQL queries
    - Connection pooling options
    """
    
    # Common JDBC drivers
    DRIVERS = {
        "postgresql": "org.postgresql.Driver",
        "mysql": "com.mysql.cj.jdbc.Driver",
        "sqlserver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        "oracle": "oracle.jdbc.driver.OracleDriver",
        "db2": "com.ibm.db2.jcc.DB2Driver",
    }
    
    def validate_config(self, config: Dict[str, Any]) -> None:
        """
        Validate JDBC connector configuration.
        
        Required fields:
            - url: JDBC connection URL
            - table: Table name or SQL query (wrapped in parentheses)
            
        Optional fields:
            - driver: JDBC driver class (auto-detected from URL if not provided)
            - user: Database username
            - password: Database password
            - properties: Additional JDBC connection properties
            - partition_column: Column for partitioned reading
            - lower_bound: Lower bound for partition column
            - upper_bound: Upper bound for partition column
            - num_partitions: Number of partitions for parallel reading
            - fetch_size: JDBC fetch size
            - query_timeout: Query timeout in seconds
            - incremental_column: Column for incremental loading (e.g., updated_at)
            - incremental_value: Last processed value for incremental column
            - add_audit_columns: Whether to add audit columns (default: False)
        
        Args:
            config: Configuration dictionary
            
        Raises:
            ValueError: If required fields are missing
        """
        required_fields = ["url", "table"]
        missing = [f for f in required_fields if f not in config]
        
        if missing:
            raise ValueError(
                f"JdbcConnector missing required config fields: {', '.join(missing)}. "
                f"Required: {', '.join(required_fields)}"
            )
        
        # Validate partitioning configuration
        partition_fields = ["partition_column", "lower_bound", "upper_bound", "num_partitions"]
        partition_provided = [f for f in partition_fields if f in config]
        
        if partition_provided and len(partition_provided) != len(partition_fields):
            raise ValueError(
                f"Partitioned reading requires all of: {', '.join(partition_fields)}. "
                f"Provided: {', '.join(partition_provided)}"
            )
        
        logger.info(f"JdbcConnector config validated: {config['url']}")
    
    def read_stream(self, spark: SparkSession) -> DataFrame:
        """
        Read data as a streaming DataFrame.
        
        Note: JDBC streaming is not directly supported by Spark.
        For incremental updates, use read_batch() with incremental_column.
        
        Args:
            spark: Active SparkSession
            
        Returns:
            Streaming DataFrame from the database
            
        Raises:
            NotImplementedError: JDBC streaming is not supported
        """
        raise NotImplementedError(
            "JDBC streaming is not supported by Spark. "
            "Use read_batch() with incremental_column for incremental loading."
        )
    
    def read_batch(self, spark: SparkSession) -> DataFrame:
        """
        Read data as a batch DataFrame from database.
        
        Supports partitioned reading for large tables and incremental loading.
        
        Args:
            spark: Active SparkSession
            
        Returns:
            Batch DataFrame from the database
        """
        url = self.config["url"]
        table = self.config["table"]
        
        logger.info(f"Reading batch data from database: {url}")
        
        # Build JDBC options
        jdbc_options = self._build_jdbc_options()
        
        # Read data
        try:
            # Check if using partitioned reading
            if "partition_column" in self.config:
                df = self._read_partitioned(spark, jdbc_options)
            else:
                df = spark.read.format("jdbc").options(**jdbc_options).load()
            
            # Apply incremental filter if configured
            if "incremental_column" in self.config and "incremental_value" in self.config:
                incremental_col = self.config["incremental_column"]
                incremental_val = self.config["incremental_value"]
                df = df.filter(f"{incremental_col} > '{incremental_val}'")
                logger.info(f"Applied incremental filter: {incremental_col} > {incremental_val}")
            
            # Add audit columns if requested
            if self.config.get("add_audit_columns", False):
                df = add_audit_columns(df=df)
                logger.info("Added audit columns to batch DataFrame")
            
            logger.info(f"Successfully read data from database table: {table}")
            return df
            
        except Exception as e:
            logger.error(f"Failed to read from database {url}: {str(e)}")
            raise
    
    def _build_jdbc_options(self) -> Dict[str, str]:
        """Build JDBC connection options dictionary."""
        url = self.config["url"]
        table = self.config["table"]
        
        options = {
            "url": url,
            "dbtable": table,
        }
        
        # Add driver (auto-detect if not provided)
        driver = self.config.get("driver")
        if not driver:
            driver = self._detect_driver(url)
        if driver:
            options["driver"] = driver
        
        # Add credentials
        if "user" in self.config:
            options["user"] = self.config["user"]
        if "password" in self.config:
            options["password"] = self.config["password"]
        
        # Add performance options
        if "fetch_size" in self.config:
            options["fetchsize"] = str(self.config["fetch_size"])
        if "query_timeout" in self.config:
            options["queryTimeout"] = str(self.config["query_timeout"])
        
        # Add custom properties
        if "properties" in self.config:
            options.update(self.config["properties"])
        
        return options
    
    def _read_partitioned(self, spark: SparkSession, base_options: Dict[str, str]) -> DataFrame:
        """Read data using partitioned reading for parallelism."""
        partition_column = self.config["partition_column"]
        lower_bound = self.config["lower_bound"]
        upper_bound = self.config["upper_bound"]
        num_partitions = self.config["num_partitions"]
        
        logger.info(
            f"Reading with partitioning: column={partition_column}, "
            f"bounds=[{lower_bound}, {upper_bound}], partitions={num_partitions}"
        )
        
        df = (
            spark.read
            .format("jdbc")
            .options(**base_options)
            .option("partitionColumn", partition_column)
            .option("lowerBound", str(lower_bound))
            .option("upperBound", str(upper_bound))
            .option("numPartitions", str(num_partitions))
            .load()
        )
        
        return df
    
    def _detect_driver(self, url: str) -> Optional[str]:
        """Auto-detect JDBC driver from connection URL."""
        url_lower = url.lower()
        
        for db_type, driver in self.DRIVERS.items():
            if db_type in url_lower:
                logger.info(f"Auto-detected JDBC driver: {driver}")
                return driver
        
        logger.warning(f"Could not auto-detect JDBC driver from URL: {url}")
        return None
