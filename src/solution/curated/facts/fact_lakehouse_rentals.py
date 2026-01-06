"""Curated fact table for lakehouse rentals.

This module uses the factory pattern to create DLT fact tables.
All logic has been consolidated into the CuratedFactFactory.
"""
from src.framework.factory.fact_factory import CuratedFactFactory
from src.framework.helper import get_spark, get_pipeline_configurations

# Create the fact table for lakehouse rentals
spark = get_spark()
factory = CuratedFactFactory(spark)

factory.create_fact(
    fact_name='fact_lakehouse_rentals',
    source_table='lakehouse_rentals',
    dimension_mappings={
        'seller_id': 'seller_key',
        'customer_id': 'customer_key',
        'lakehouse_id': 'lakehouse_key',
        'order_date': 'calendar_order_key',
        'check_in_date': 'calendar_checkin_key',
        'check_out_date': 'calendar_checkout_key',
    }
)
