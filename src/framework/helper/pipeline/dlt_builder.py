"""Delta Live Tables (DLT) builder utilities."""
from typing import Any, Callable, Dict, List, Optional
from pyspark.sql import DataFrame

from src.framework.helper.core import get_logger, get_spark
from src.framework.helper.config import DefaultTblProperties

logger = get_logger(__name__)


def ldp_table(
    name: str,
    connector: Any,  # BaseConnector instance
    comment: Optional[str] = None,
    spark_conf: Optional[Dict[str, Any]] = None,
    table_properties: Optional[Dict[str, Any]] = None,
    path: Optional[str] = None,
    partition_cols: Optional[List[str]] = None,
    cluster_by_auto: bool = True,
    cluster_by: Optional[List[str]] = None,
    schema: Optional[str] = None,
    row_filter: Optional[str] = None,
    private: bool = False,
) -> None:
    """
    Creates a Delta Live Table (DLT) using a connector for data ingestion.
    
    This is the new, preferred API for creating DLT tables with extensible
    data source support via the connector framework.
    
    Args:
        name (str): Fully qualified table name (catalog.schema.table)
        connector (BaseConnector): Connector instance that handles data reading
        comment (str, optional): Table description
        spark_conf (dict, optional): Spark configuration overrides
        table_properties (dict, optional): Delta table properties
        path (str, optional): Storage location
        partition_cols (list, optional): Partitioning columns
        cluster_by_auto (bool, optional): Enable liquid clustering (default: True)
        cluster_by (list, optional): Manual clustering columns
        schema (str, optional): Explicit schema definition
        row_filter (str, optional): Row-level filter
        private (bool, optional): Whether table is private to pipeline (default: False)
        
    Returns:
        None: Registers DLT table in pipeline
    """
    # Add connector metadata to table properties
    connector_metadata: Dict[str, str] = {
        "metadata.connector-type": type(connector).__name__,
        "metadata.connector-version": "2.0",
    }
    
    merged_properties: Dict[str, Any] = {
        **(table_properties or {}),
        **(DefaultTblProperties().as_dict() or {}),
        **connector_metadata,
    }
    
    logger.info(f"Creating DLT table {name} using {type(connector).__name__}")
    
    # Use factory function pattern to properly capture variables in closures
    # This avoids Python's late-binding closure issue when called in loops
    def create_table_with_params(
        table_name: str,
        table_connector: Any,
        table_comment: Optional[str],
        table_spark_conf: Optional[Dict[str, Any]],
        table_properties: Optional[Dict[str, Any]],
        table_path: Optional[str],
        table_partition_cols: Optional[List[str]],
        table_cluster_by_auto: bool,
        table_cluster_by: Optional[List[str]],
        table_schema: Optional[str],
        table_row_filter: Optional[str],
        table_private: bool,
    ) -> None:
        # Import dlt inside function - only when actually creating tables in DLT context
        import dlt  # type: ignore
        
        @dlt.table(
            name=table_name,
            comment=table_comment,
            spark_conf=table_spark_conf,
            table_properties=table_properties,
            path=table_path,
            partition_cols=table_partition_cols,
            cluster_by_auto=table_cluster_by_auto,
            cluster_by=table_cluster_by,
            schema=table_schema,
            row_filter=table_row_filter,
            private=table_private,
        )
        def table_creation(
            table_connector: Any = table_connector,
            table_name: str = table_name,
        ) -> DataFrame:
            """Inner function that reads data via connector."""
            logger.info(f"Reading data using {type(table_connector).__name__}")
            spark = get_spark()
            
            # Check for explicit mode configuration in connector config
            connector_mode = getattr(table_connector, 'config', {}).get('mode', 'batch')
            
            # Check if connector has a preference for batch vs streaming
            # Explicit mode config overrides connector preference
            if connector_mode == 'streaming':
                logger.info(f"Using streaming read for {table_name} (mode=streaming)")
                df = table_connector.read_stream(spark)
            elif connector_mode == 'batch':
                logger.info(f"Using batch read for {table_name} (mode=batch)")
                df = table_connector.read_batch(spark)
            else:
                # Fallback to connector preference
                use_batch = getattr(table_connector, 'prefer_batch', False)
                
                if use_batch:
                    logger.info(f"Using batch read for {type(table_connector).__name__} (prefer_batch=True)")
                    df = table_connector.read_batch(spark)
                else:
                    logger.info(f"Using streaming read for {type(table_connector).__name__} (prefer_batch=False)")
                    df = table_connector.read_stream(spark)
            return df
    
    # Invoke the factory function to register the table with properly captured variables
    create_table_with_params(
        table_name=name,
        table_connector=connector,
        table_comment=comment,
        table_spark_conf=spark_conf,
        table_properties=merged_properties,
        table_path=path,
        table_partition_cols=partition_cols,
        table_cluster_by_auto=cluster_by_auto,
        table_cluster_by=cluster_by,
        table_schema=schema,
        table_row_filter=row_filter,
        table_private=private,
    )


def ldp_view(
    source_catalog: str,
    source_schema: str,
    source_object: str,
    source_dataframe: DataFrame,
    comment: Optional[str] = None,
) -> None:
    """
    Creates a view in the Lakeflow declarative pipeline.

    Args:
        source_catalog (str): The name of the source catalog.
        source_schema (str): The name of the source schema.
        source_object (str): The name of the source object.
        source_dataframe (DataFrame): The DataFrame to be used for the view.
        comment (str, optional): An optional comment for the view.

    Returns:
        None: This function does not return a value. It registers a view in the pipeline.
    """
    # Import dlt inside function - only when actually creating views in DLT context
    import dlt  # type: ignore

    @dlt.view(
        name=f"{source_catalog}_{source_schema}_{source_object}_view",
        comment=comment,
    )
    def view_creation() -> DataFrame:
        return source_dataframe


def ldp_create_streaming_table(
    name: str,
    comment: Optional[str] = None,
    spark_conf: Optional[Dict[str, Any]] = None,
    table_properties: Optional[Dict[str, Any]] = None,
    path: Optional[str] = None,
    partition_cols: Optional[List[str]] = None,
    cluster_by_auto: bool = True,
    cluster_by: Optional[List[str]] = None,
    schema: Optional[str] = None,
    expect_all: Optional[Dict[str, Any]] = None,
    expect_all_or_drop: Optional[Dict[str, Any]] = None,
    expect_all_or_fail: Optional[Dict[str, Any]] = None,
    row_filter: Optional[str] = None,
) -> None:
    """
    Creates a streaming table in Delta Live Tables (DLT) using the specified parameters.

    Args:
        name (str): The name of the streaming table.
        comment (str, optional): A comment for the table.
        spark_conf (dict, optional): Spark configuration settings as key-value pairs.
        table_properties (dict, optional): Table properties as key-value pairs.
        path (str, optional): The storage location path for the table.
        partition_cols (list, optional): List of columns to partition the table by.
        cluster_by_auto (bool, optional): Whether to automatically cluster the table. Defaults to True.
        cluster_by (list, optional): List of columns to cluster the table by.
        schema (str, optional): The schema definition for the table.
        expect_all (dict, optional): Expectations for the table as key-value pairs.
        expect_all_or_drop (dict, optional): Expectations for dropping rows as key-value pairs.
        expect_all_or_fail (dict, optional): Expectations for failing on rows as key-value pairs.
        row_filter (str, optional): A SQL-like filter clause for the rows.

    Returns:
        None: This function does not return a value. It registers a streaming table in DLT.
    """
    # Import dlt inside function - only when actually creating tables in DLT context
    import dlt  # type: ignore
    
    dlt.create_streaming_table(
        name=name,
        comment=comment,
        spark_conf=spark_conf,
        table_properties=table_properties,
        path=path,
        partition_cols=partition_cols,
        cluster_by_auto=cluster_by_auto,
        cluster_by=cluster_by,
        schema=schema,
        expect_all=expect_all,
        expect_all_or_drop=expect_all_or_drop,
        expect_all_or_fail=expect_all_or_fail,
        row_filter=row_filter,
    )


def ldp_change_data_capture(
    source: str,
    target_catalog: str,
    target_schema: str,
    target_object: str,
    keys: List[str],
    sequence_column: str,
    stored_as_scd_type: int,
    ignore_null_updates: bool = False,
    apply_as_deletes: Optional[Dict[str, Any]] = None,
    apply_as_truncates: Optional[Dict[str, Any]] = None,
    column_list: Optional[List[str]] = None,
    except_column_list: Optional[List[str]] = None,
    track_history_column_list: Optional[List[str]] = None,
    track_history_except_column_list: Optional[List[str]] = None,
    table_properties: Optional[Dict[str, Any]] = None,
    name: Optional[str] = None,
    once: bool = False,
) -> None:
    """
    Creates a change data capture (CDC) flow for a specified source and target object.
    
    This function leverages Delta Live Tables' built-in CDC capabilities with automatic
    Slowly Changing Dimension (SCD) Type 1 or 2 handling. For SCD Type 2, the function
    automatically manages effective dating columns (__START_AT and __END_AT) to track
    the validity period of each record version.
    
    SCD Type 1: Updates existing records in place (overwrites history)
    SCD Type 2: Maintains full history with effective date ranges, automatically adding:
        - __START_AT: Timestamp when the record version became effective
        - __END_AT: Timestamp when the record version expired (NULL for current records)

    Parameters:
        source (str): The source table (fully qualified or alias) for CDC reading.
        target_catalog (str): The target catalog for the CDC table.
        target_schema (str): The target schema for the CDC table.
        target_object (str): The target table name for CDC records.
        keys (list): List of column names that form the primary key (uniquely identify records).
        sequence_column (str): Column used to order changes chronologically (typically a timestamp or version).
        stored_as_scd_type (int): SCD type (1 or 2). Type 1 overwrites, Type 2 maintains history.
        ignore_null_updates (bool, optional): If True, skips updates where all non-key columns are NULL. Defaults to False.
        apply_as_deletes (dict, optional): Condition specifying when to treat changes as deletes. Defaults to None.
        apply_as_truncates (dict, optional): Condition specifying when to treat changes as truncates. Defaults to None.
        column_list (list, optional): Explicit list of columns to include in CDC. Defaults to None (all columns).
        except_column_list (list, optional): List of columns to exclude from CDC. Defaults to None.
        track_history_column_list (list, optional): Columns to explicitly track in history (for SCD Type 2). Defaults to None.
        track_history_except_column_list (list, optional): Columns to exclude from history tracking. Defaults to None.
        table_properties (dict, optional): Additional Delta table properties. Defaults to None.
        name (str, optional): Name for the CDC flow (if None, auto-generated). Defaults to None.
        once (bool, optional): If True, the flow executes once and stops. Defaults to False.

    Raises:
        ValueError: If stored_as_scd_type is not 1 or 2.
        
    Example:
        ldp_change_data_capture(
            source="catalog.source_schema.orders_changes",
            target_catalog="catalog",
            target_schema="curated",
            target_object="orders_scd2",
            keys=["order_id"],
            sequence_column="change_timestamp",
            stored_as_scd_type=2,
            ignore_null_updates=True,
        )
        # Creates a SCD Type 2 table with automatic __START_AT and __END_AT columns
    """
    import dlt  # type: ignore
    
    # Validate required parameters
    if not source or not isinstance(source, str):
        raise ValueError("Parameter 'source' must be a non-empty string")
    if not target_catalog or not isinstance(target_catalog, str):
        raise ValueError("Parameter 'target_catalog' must be a non-empty string")
    if not target_schema or not isinstance(target_schema, str):
        raise ValueError("Parameter 'target_schema' must be a non-empty string")
    if not target_object or not isinstance(target_object, str):
        raise ValueError("Parameter 'target_object' must be a non-empty string")
    if not keys or not isinstance(keys, list) or len(keys) == 0:
        raise ValueError("Parameter 'keys' must be a non-empty list of column names")
    if not sequence_column or not isinstance(sequence_column, str):
        raise ValueError("Parameter 'sequence_column' must be a non-empty string")
    if stored_as_scd_type not in (1, 2):
        raise ValueError("stored_as_scd_type must be either 1 or 2, got: {}".format(stored_as_scd_type))

    # Merge table_properties with additional metadata
    merged_table_properties: Dict[str, Any] = {
        **(table_properties or {}),
        **(DefaultTblProperties().as_dict() or {}),
        "metadata.scd-type": f"{stored_as_scd_type}",
    }

    ldp_create_streaming_table(
        name=f"{target_catalog}.{target_schema}.{target_object}",
        table_properties=merged_table_properties,
    )

    dlt.create_auto_cdc_flow(
        target=f"{target_catalog}.{target_schema}.{target_object}",
        source=source,
        keys=keys,
        sequence_by=sequence_column,
        ignore_null_updates=ignore_null_updates,
        apply_as_deletes=apply_as_deletes,
        apply_as_truncates=apply_as_truncates,
        column_list=column_list,
        except_column_list=except_column_list,
        stored_as_scd_type=stored_as_scd_type,
        track_history_column_list=track_history_column_list,
        track_history_except_column_list=track_history_except_column_list,
        name=name,
        once=once,
    )
    
    logger.info(f"Created CDC flow: {target_catalog}.{target_schema}.{target_object} with SCD type {stored_as_scd_type}")

