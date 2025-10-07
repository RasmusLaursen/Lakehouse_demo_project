from pyspark.sql.functions import col

from src.helper import databricks_helper

spark = databricks_helper.get_spark()


def dimension_keys_lookup(curated_catalog, curated_dimension_schema, fact_df):
    dimenension_keys = [column for column in fact_df.columns if column.endswith("key")]

    for key in dimenension_keys:
        if key in fact_df.columns:
            dimension_name = key.replace("_key", "")

            if key.count("_") == 1:
                dimension_df = spark.read.table(
                    f"{curated_catalog}.{curated_dimension_schema}.dim_{dimension_name}"
                )
                dimension_key = key
                dimension_role_name = dimension_name
            elif key.count("_") == 2:
                split = key.split("_")
                dimension_key = f"{split[0]}_key"
                dimension_name = f"{split[0]}"
                dimension_role = f"{split[1]}"
                dimension_df = spark.read.table(
                    f"{curated_catalog}.{curated_dimension_schema}.dim_{dimension_name}"
                )
                dimension_role_name = dimension_name + "_" + dimension_role

            fact_df = fact_df.alias("lhr").join(
                dimension_df.select(
                    col(dimension_key).alias(key),
                    col(f"{dimension_name}_id").alias(f"{dimension_role_name}_id"),
                ).alias("dim"),
                on=col(f"lhr.{key}") == col(f"dim.{key}"),
                how="left",
            )

    cols = [c for c in fact_df.columns if not c.endswith("_key")]
    fact_df = fact_df.select(*cols)

    # Reorder columns so columns ending with _id come first
    cols = [c for c in fact_df.columns if c.endswith("_id")] + [
        c for c in fact_df.columns if not c.endswith("_id")
    ]
    fact_df = fact_df.select(*cols)
    return fact_df

def get_table_properties(catalog: str, schema: str, table: str) -> dict:
    """
    Returns all table properties for a given table in Databricks using PySpark only.

    Args:
        catalog (str): The catalog name.
        schema (str): The schema/database name.
        table (str): The table name.

    Returns:
        dict: Dictionary of table properties.
    """
    table_identifier = f"{catalog}.{schema}.{table}"
    desc_df = spark.sql(f"DESCRIBE TABLE EXTENDED {table_identifier}")
    properties_row = desc_df.filter(col("col_name") == "Table Properties").collect()
    if properties_row:
        properties_str = properties_row[0]["data_type"]
        properties_str = properties_str.strip("[]")
        properties = {}
        if properties_str:
            for item in properties_str.split(","):
                if "=" in item:
                    k, v = item.split("=", 1)
                    properties[k.strip()] = v.strip()
        return properties
    return {}