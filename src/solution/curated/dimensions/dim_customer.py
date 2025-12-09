"""Curated dimension table for customer.

This module uses the factory pattern to create DLT dimension tables.
All logic has been consolidated into the CuratedDimensionFactory.
"""
from src.framework.pipelines.dimension_factory import CuratedDimensionFactory
from src.framework.helper import databricks_helper

# Create the dimension table for customer
spark = databricks_helper.get_spark()
factory = CuratedDimensionFactory(spark)

factory.create_dimension(
    dimension_name='dim_customer',
    source_table='customer',
    business_key_column='customer_id',
    filter_active=True
)
