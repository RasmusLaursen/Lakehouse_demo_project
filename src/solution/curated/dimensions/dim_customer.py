"""Curated dimension table for customer.

This module uses the factory pattern to create DLT dimension tables.
Customer dimension is SCD Type 2 with automatic start/end time tracking.
"""
from src.framework.factory.dimension_factory import CuratedDimensionFactory
from src.framework.helper import get_spark, get_pipeline_configurations

# Create the dimension table for customer with SCD Type 2
spark = get_spark()
factory = CuratedDimensionFactory(spark)

factory.create_dimension(
    dimension_name='dim_customer',
    source_table='customer',
    business_key_column='customer_id',
    filter_active=True,
    scd_type=2  # Use SCD Type 2 with automatic start/end time columns
)

