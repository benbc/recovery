"""Configuration for pipeline2."""

from pathlib import Path

# Reuse pipeline1's database and paths
from pipeline.config import DB_PATH, FILES_DIR, EXPORT_DIR

# Pipeline2 stages
STAGE_ORDER = ["1", "1b", "2", "3"]

__all__ = ["DB_PATH", "FILES_DIR", "EXPORT_DIR", "STAGE_ORDER"]
