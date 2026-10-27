"""Curated fact table for lakehouse rentals.

This module uses the factory pattern to create DLT fact tables.
All logic has been consolidated into the CuratedFactFactory.
"""

from pyspark.sql import DataFrame
from src.framework.factory.fact_factory import FactFactory
from src.framework.helper import get_spark, get_pipeline_configurations

# Create the fact table for lakehouse rentals
spark = get_spark()
factory = FactFactory(spark)


def _tranform_fact_lakehouse_rentals() -> DataFrame:
    """Transformation function to create the source DataFrame for the fact table."""
    df = spark.readStream.table(
        factory.catalog_manager.get_base_table_path("lakehouse_rentals")
    )

    # Apply necessary transformations, joins, and column renames
    df = df.withColumnRenamed("seller_id", "seller_key")
    return df


factory.create_fact(
    fact_name="fact_lakehouse_rentals",
    transform_fn=_tranform_fact_lakehouse_rentals,
    # dimension_mappings={
    #     'seller_id': 'seller_key',
    #     'customer_id': 'customer_key',
    #     'lakehouse_id': 'lakehouse_key',
    #     'order_date': 'calendar_order_key',
    #     'check_in_date': 'calendar_checkin_key',
    #     'check_out_date': 'calendar_checkout_key',
    # }
)
