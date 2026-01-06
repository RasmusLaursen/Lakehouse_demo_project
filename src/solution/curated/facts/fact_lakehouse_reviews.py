"""Curated fact table for lakehouse reviews.

This module uses the factory pattern to create DLT fact tables.
All logic has been consolidated into the CuratedFactFactory.
"""
from src.framework.factory.fact_factory import CuratedFactFactory
from src.framework.helper import get_spark, get_pipeline_configurations

# Create the fact table for lakehouse reviews
spark = get_spark()
factory = CuratedFactFactory(spark, source_system='review')

factory.create_fact(
    fact_name='fact_lakehouse_reviews',
    source_table='reviews',
    source_schema='review_base_schema',  # Different base schema
    dimension_mappings={
        'review_date': 'calendar_review_key'
    }
)
