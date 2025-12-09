"""Raw layer ingestion pipeline for bookings source system.

This module uses the factory pattern to create DLT tables dynamically.
All logic has been consolidated into the RawPipelineFactory.
"""
from src.framework.pipelines.raw_factory import create_raw_pipeline

# Create the raw pipeline for bookings source system
create_raw_pipeline("bookings")
