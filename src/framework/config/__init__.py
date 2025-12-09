"""Configuration package for lakehouse pipelines."""

# Import unified configuration
from src.framework.factory.config import PipelineConfig

# Backward compatibility alias
CuratedConfig = PipelineConfig

__all__ = ["PipelineConfig", "CuratedConfig"]
