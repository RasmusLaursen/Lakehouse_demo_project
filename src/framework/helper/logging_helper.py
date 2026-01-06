"""Backward compatibility module for logging utilities.

This module re-exports functions from src.framework.helper.core for backward compatibility
with code that imports logging_helper directly.
"""

from src.framework.helper.core.logging import get_logger

__all__ = [
    "get_logger",
]
