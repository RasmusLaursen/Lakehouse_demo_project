import warnings
from src.framework.helper import databricks_helper
from src.framework.helper import read
from pyspark.sql import DataFrame
from src.framework.helper.config import DefaultTblProperties
from typing import Optional

from src.framework.helper import logging_helper

# Initialize logger
logger = logging_helper.get_logger(__name__)


def ldp_table(
    name: str,
    connector,  # BaseConnector instance
    comment: Optional[str] = None,
    spark_conf: Optional[dict] = None,
    table_properties: Optional[dict] = None,
    path: Optional[str] = None,
    partition_cols: Optional[list] = None,
    cluster_by_auto: bool = True,
    cluster_by: Optional[list] = None,
    schema: Optional[str] = None,
    row_filter: Optional[str] = None,
    exceptions: Optional[list[dict]] = None,
    private: bool = False,
):
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
        exceptions (list[dict], optional): Data quality expectations
        private (bool, optional): Whether table is private to pipeline (default: False)
        
    Returns:
        None: Registers DLT table in pipeline
        
    Example:
        >>> from src.framework.connectors import AutoLoaderConnector
        >>> connector = AutoLoaderConnector({
        ...     "source_type": "volume",
        ...     "catalog": "landing",
        ...     "schema": "lakehouse",
        ...     "volume": "customer_contract",
        ...     "format": "parquet"
        ... })
        >>> ldp_table(
        ...     name="raw.lakehouse.customer",
        ...     connector=connector,
        ...     comment="Raw customer data"
        ... )
    """
    # Add connector metadata to table properties
    connector_metadata = {
        "metadata.connector-type": type(connector).__name__,
        "metadata.connector-version": "2.0",
    }
    
    merged_properties = {
        **(table_properties or {}),
        **(DefaultTblProperties().as_dict() or {}),
        **connector_metadata,
    }
    
    logger.info(f"Creating DLT table {name} using {type(connector).__name__}")
    
    # Use factory function pattern to properly capture variables in closures
    # This avoids Python's late-binding closure issue when called in loops
    def create_table_with_params(
        table_name: str,
        table_connector,
        table_comment: Optional[str],
        table_spark_conf: Optional[dict],
        table_properties: Optional[dict],
        table_path: Optional[str],
        table_partition_cols: Optional[list],
        table_cluster_by_auto: bool,
        table_cluster_by: Optional[list],
        table_schema: Optional[str],
        table_row_filter: Optional[str],
        table_private: bool,
    ):
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
            table_connector = table_connector,
            table_name = table_name,

        ) -> DataFrame:
            """Inner function that reads data via connector."""
            logger.info(f"Reading data using {type(table_connector).__name__}")
            spark = databricks_helper.get_spark()
            
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
):
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
    def view_creation():
        return source_dataframe


def ldp_change_data_capture(
    source: str,
    target_catalog: str,
    target_schema: str,
    target_object: str,
    keys: list,
    sequence_column: str,
    stored_as_scd_type: int,
    ignore_null_updates=False,
    apply_as_deletes=None,
    apply_as_truncates=None,
    column_list=None,
    except_column_list=None,
    track_history_column_list=None,
    track_history_except_column_list=None,
    table_properties=None,
    name=None,
    once=False,
):
    """
    Creates a change data capture (CDC) flow for a specified source and target object.

    Parameters:
    - source_catalog (str): The catalog of the source object.
    - source_schema (str): The schema of the source object.
    - source_object (str): The name of the source object.
    - target_catalog (str): The catalog of the target object.
    - target_schema (str): The schema of the target object.
    - target_object (str): The name of the target object.
    - keys (list): A list of keys to identify records.
    - sequence_column (str): The column used for sequencing changes.
    - stored_as_scd_type (int): The type of slowly changing dimension (1 or 2).
    - ignore_null_updates (bool, optional): Whether to ignore updates with null values. Defaults to False.
    - apply_as_deletes (optional): Specifies how to apply deletes.
    - apply_as_truncates (optional): Specifies how to apply truncates.
    - column_list (optional): A list of columns to include in the CDC.
    - except_column_list (optional): A list of columns to exclude from the CDC.
    - track_history_column_list (optional): A list of columns to track history.
    - track_history_except_column_list (optional): A list of columns to exclude from history tracking.
    - name (str, optional): The name of the CDC flow. Defaults to None.
    - once (bool, optional): If True, the flow will run only once. Defaults to False.

    Raises:
    - ValueError: If stored_as_scd_type is not 1 or 2.
    """
    if stored_as_scd_type not in (1, 2):
        raise ValueError("stored_as_scd_type must be either 1 or 2.")

    # Merge table_properties with additional metadata
    table_properties = {
        **(table_properties or {}),
        **(DefaultTblProperties().as_dict() or {}),
        "metadata.scd-type": f"{stored_as_scd_type}",
    }

    ldp_create_streaming_table(
        name=f"{target_catalog}.{target_schema}.{target_object}",
        table_properties=table_properties,
    )
    
    # Import dlt inside function - only when actually creating CDC in DLT context
    import dlt  # type: ignore

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


def ldp_create_streaming_table(
    name: str,
    comment: Optional[str] = None,
    spark_conf: Optional[dict] = None,
    table_properties: Optional[dict] = None,
    path: Optional[str] = None,
    partition_cols: Optional[list] = None,
    cluster_by_auto: bool = True,
    cluster_by: Optional[list] = None,
    schema: Optional[str] = None,
    expect_all: Optional[dict] = None,
    expect_all_or_drop: Optional[dict] = None,
    expect_all_or_fail: Optional[dict] = None,
    row_filter: Optional[str] = None,
):
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
    # Import dlt inside function - only when actually creating streaming tables in DLT context
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
