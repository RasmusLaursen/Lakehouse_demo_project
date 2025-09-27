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
