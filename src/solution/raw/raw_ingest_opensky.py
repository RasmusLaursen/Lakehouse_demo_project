"""Raw layer ingestion pipeline for opensky source system.

This module uses the factory pattern to create DLT tables dynamically.
All logic has been consolidated into the RawPipelineFactory.

OpenSky Network provides real-time aircraft state vectors for tracking
flights over Danish airspace using their free REST API.
"""
from src.framework.factory.raw_factory import create_raw_pipeline

# Create the raw pipeline for opensky source system
create_raw_pipeline("opensky")
