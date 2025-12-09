"""Base layer CDC pipeline for bookings source system.

This module uses the factory pattern to create DLT tables dynamically.
All logic has been consolidated into the BasePipelineFactory.

The factory properly handles:
- Data contract loading
- Configuration management
- Optional data quality validation
- Change Data Capture (CDC) processing
- Proper closure variable capture for DLT decorators
"""
from src.framework.pipelines.base_factory import create_base_pipeline

# Create the base pipeline for bookings source system
create_base_pipeline("bookings")
