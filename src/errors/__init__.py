# ---------------------------------------------------------------
# IMPORTS
# ---------------------------------------------------------------

from src.errors.exceptions import (ReaderError, ReaderConfigError, ReaderBuildError,
    ReaderExecutionError, ReaderSchemaError, WaypointError, WaypointBuildError
)

# ---------------------------------------------------------------
# PACKAGE MANAGEMENT
# ---------------------------------------------------------------

__all__ = [
    "ReaderError",
    "ReaderConfigError",
    "ReaderBuildError",
    "ReaderExecutionError",
    "ReaderSchemaError",
    "WaypointError",
    "WaypointBuildError"
]
__version__ = "0.0.1"
__author__ = "HysingerDev"