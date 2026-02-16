"""Base layer CDC pipeline for opensky source system.

This module uses the factory pattern to create DLT tables dynamically.
All logic has been consolidated into the BasePipelineFactory.

The factory properly handles:
- Data contract loading
- Configuration management
- Optional data quality validation
- Change Data Capture (CDC) processing
- Proper closure variable capture for DLT decorators

OpenSky Network tracking data from Danish airspace is processed here
with CDC to track changes in aircraft states over time.
"""
from src.framework.factory.base_factory import create_base_pipeline

# Create the base pipeline for opensky source system
create_base_pipeline("opensky")
