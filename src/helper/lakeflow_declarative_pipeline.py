import dlt
from src.helper import commen
from src.helper import databricks_helper
from src.helper import read
from pyspark.sql import DataFrame

spark = databricks_helper.get_spark()


def ldp_table(
    name: str,
    source_catalog: str = None,
    source_schema: str = None,
    objectname: str = None,
    source_dataframe: DataFrame = None,
    loadtype: str = "dataframe",
    filetype: str = None,
    commet: str = None,
    spark_conf: dict = None,
    table_properties: dict = None,
    path: str = None,
    partition_cols: list = None,
    cluster_by_auto: bool = True,
    cluster_by: list = None,
    schema: str = None,
    row_filter: str = None,
    exeptions: list[dict] = None,
    private=False,
):
    @dlt.table(
        name=name,
        comment=commet,
        spark_conf=spark_conf,
        table_properties=table_properties,
        path=path,
        partition_cols=partition_cols,
        cluster_by_auto=cluster_by_auto,
        cluster_by=cluster_by,
        schema=schema,
        row_filter=row_filter,
        private=private,
    )
    # @ldp_exeption(rules=exeptions)
    # @handle_exceptions(exeptions)
    def table_creation(
        loadtype=loadtype,
        source_catalog=source_catalog,
        source_schema=source_schema,
        objectname=objectname,
        filetype=filetype,
        source_dataframe=source_dataframe,
    ) -> DataFrame:
        if loadtype == "table":
            df = read.read_dataframe(
                source_catalog=source_catalog,
                source_schema=source_schema,
                objectname=objectname,
            )
            return df
        elif loadtype == "table_stream":
            df = spark.readStream.table(
                f"{source_catalog}.{source_schema}.{objectname}"
            )
            df = commen.add_audit_columns(df=df)
            return df
        elif loadtype == "volume":
            return read.read_cloudfiles_autoloader(
                source_catalog=source_catalog,
                source_schema=source_schema,
                objectname=objectname,
                filetype=filetype,
                add_audit_column=True,
            )
        elif loadtype == "dataframe":
            return source_dataframe


def handle_exceptions(exeptions):
    for expection in exeptions:
        description, constraint, ldp_exeption_type = expection.values()
        dlt.expect(description, constraint)


def ldp_exeption(exeptions):
    for expection in exeptions:
        description, constraint, ldp_exeption_type = expection.values()
        return dlt.expect(description, constraint)
        # list_of_exeptions = []
        # for expection in exeptions:
        #     description, constraint, ldp_exeption_type = expection.values()
        #     if ldp_exeption_type == "expect":
        #         list_of_exeptions.append(dlt.expect(description, constraint))
        #     elif ldp_exeption_type == "expect_or_drop":
        #         list_of_exeptions.append(dlt.expect_or_drop(description, constraint))
        #     elif ldp_exeption_type == "expect_or_fail":
        #         list_of_exeptions.append(dlt.expect_or_fail(description, constraint))
        #     elif ldp_exeption_type == "expect_all":
        #         list_of_exeptions.append(dlt.expect_all({description: constraint}))
        #     elif ldp_exeption_type == "expect_all_or_drop":
        #         list_of_exeptions.append(dlt.expect_all_or_drop({description: constraint}))
        #     elif ldp_exeption_type == "expect_all_or_fail":
        #         list_of_exeptions.append(dlt.expect_all_or_fail({description: constraint}))
        #     else:
        #         raise ValueError("ldp_exeption_type must be either 'expect', 'expect_or_drop', 'expect_or_fail', 'expect_all', 'expect_all_or_drop', or 'expect_all_or_fail'.")
        # return list_of_exeptions


def ldp_apply_changes():
    pass


def ldp_create_sink():
    pass


def ldp_view(
    source_catalog: str,
    source_schema: str,
    source_object: str,
    source_dataframe: DataFrame,
    comment: str = None,
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

    @dlt.view(
        name=f"{source_catalog}_{source_schema}_{source_object}_view",
        comment=comment,
    )
    def view_creation():
        return source_dataframe


def ldp_change_data_capture(
    source_catalog: str,
    source_schema: str,
    source_object: str,
    target_catalog: str,
    target_schema: str,
    target_object: str,
    private_name: str,
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
    name=None,
    once=False,
):
    if stored_as_scd_type not in [1, 2]:
        raise ValueError("stored_as_scd_type must be either 1 or 2.")

    ldp_create_streaming_table(name=f"{target_catalog}.{target_schema}.{target_object}")

    dlt.create_auto_cdc_flow(
        target=f"{target_catalog}.{target_schema}.{target_object}",
        source=f"{source_catalog}.{source_schema}.{source_object}",
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
    comment: str = None,
    spark_conf: dict = None,
    table_properties: dict = None,
    path: str = None,
    partition_cols: list = None,
    cluster_by_auto: bool = True,
    cluster_by: list = None,
    schema: str = None,
    expect_all: dict = None,
    expect_all_or_drop: dict = None,
    expect_all_or_fail: dict = None,
    row_filter: str = None,
):
    """
    Creates a streaming table in Delta Live Tables (DLT) using the specified parameters.

    Args:
        source_catalog (str): The source catalog for the streaming table.
        source_schema (str): The source schema for the streaming table.
        source_object (str): The name of the object/table to be created.
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
