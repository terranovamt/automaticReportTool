"""
Core business logic and models for ART.stdf

This package contains the foundational data structures and business logic.
"""

from .models import (
    ProcessType,
    FileStatus,
    STDFFile,
    ProcessingResult,
    ReportType,
)
from .exceptions import (
    ARTError,
    ConfigurationError,
    ProcessingError,
    ValidationError,
    FileNotFoundError,
    ParsingError,
)
from .constants import (
    STDF_EXTENSIONS,
    PARQUET_EXTENSIONS,
    HTML_EXTENSIONS,
    DEFAULT_COMPRESSION,
    RECORD_TYPES,
)

__all__ = [
    # Models
    "ProcessType",
    "FileStatus",
    "STDFFile",
    "ProcessingResult",
    "ReportType",
    # Exceptions
    "ARTError",
    "ConfigurationError",
    "ProcessingError",
    "ValidationError",
    "FileNotFoundError",
    "ParsingError",
    # Constants
    "STDF_EXTENSIONS",
    "PARQUET_EXTENSIONS",
    "HTML_EXTENSIONS",
    "DEFAULT_COMPRESSION",
    "RECORD_TYPES",
]
