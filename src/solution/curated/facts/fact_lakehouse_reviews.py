"""Curated fact table for lakehouse reviews.

This module uses the factory pattern to create DLT fact tables.
All logic has been consolidated into the CuratedFactFactory.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from src.framework.factory.fact_factory import FactFactory
from src.framework.helper import get_spark

# Create the fact table for lakehouse reviews
spark = get_spark()
factory = FactFactory(spark, source_system="review")


def _transform_fact_lakehouse_reviews() -> DataFrame:
    df = spark.readStream.table(factory.catalog_manager.get_base_table_path("reviews"))
    return df.withColumn("calendar_review_key", col("review_date").cast("date"))


factory.create_fact(
    fact_name="fact_lakehouse_reviews",
    transform_fn=_transform_fact_lakehouse_reviews,
)
