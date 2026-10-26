"""Raw layer ingestion pipeline for lakehouse source system.

This module uses the factory pattern to create DLT tables dynamically.
All logic has been consolidated into the RawPipelineFactory.
"""
from src.framework.factory.raw_factory import create_raw_pipeline

# Create the raw pipeline for lakehouse source system
create_raw_pipeline("lakehouse")
