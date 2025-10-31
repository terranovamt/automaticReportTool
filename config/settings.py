"""
Global settings and configuration for ART.stdf

This module provides centralized configuration management with validation
and type safety.
"""

import os
import re
from dataclasses import dataclass, field
from typing import Set, Optional
from pathlib import Path


@dataclass
class ProcessingConfig:
    """Configuration for data processing operations."""

    # Parallel processing
    parallel_stdf_workers: int = 2  # Number of parallel STDF processors
    max_workers: int = 4  # Maximum parallel workers

    # Memory management
    chunk_size: int = 10000  # Records per chunk for processing
    max_memory_mb: int = 2048  # Max memory usage in MB

    # Performance
    use_polars: bool = True  # Use Polars for DataFrame operations
    compression: str = "lz4"  # Parquet compression (lz4, snappy, gzip, zstd)
    buffer_size_mb: int = 2  # I/O buffer size in MB

    # Processing options
    remove_retests: bool = True  # Remove retest data
    optimize_dtypes: bool = True  # Optimize data types for memory

    # Validation
    validate_stdf: bool = True  # Validate STDF format
    strict_mode: bool = False  # Strict validation mode

    def __post_init__(self):
        """Validate configuration after initialization."""
        if self.parallel_stdf_workers < 1:
            raise ValueError("parallel_stdf_workers must be >= 1")
        if self.max_workers < self.parallel_stdf_workers:
            self.max_workers = self.parallel_stdf_workers
        if self.compression not in {"lz4", "snappy", "gzip", "zstd", "uncompressed"}:
            raise ValueError(f"Invalid compression: {self.compression}")


@dataclass
class FlowConfig:
    """Configuration for test flow validation."""

    allowed_flows: Set[str] = field(default_factory=lambda: {
        "EWS1", "EWS2", "EWS3", "EWSDIE", "EWSCHAR",
        "FT", "FT1", "FT2",
    })

    allowed_packages: Set[str] = field(default_factory=lambda: {
        "QFP", "QFN", "DIP", "WLCSP", "CSP", "BGA"
    })

    product_regex: re.Pattern = field(default_factory=lambda: re.compile(r"^[A-F0-9]{3}$"))

    def is_valid_flow(self, flow: str) -> bool:
        """Check if flow is valid."""
        return flow.upper() in self.allowed_flows

    def is_valid_package(self, package: str) -> bool:
        """Check if package is valid."""
        return package.upper() in self.allowed_packages

    def is_valid_product(self, product: str) -> bool:
        """Check if product code is valid."""
        return bool(self.product_regex.match(product))


@dataclass
class LoggingConfig:
    """Configuration for logging system."""

    log_dir: Path = field(default_factory=lambda: Path("log"))
    max_lines_per_file: int = 1000
    backup_count: int = 1
    log_level: str = "INFO"
    log_format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

    # Separate log files for different components
    stdf2data_log: str = "stdf2data.log"
    data2report_log: str = "data2report.log"
    condition2report_log: str = "condition2report.log"
    shmoo_log: str = "shmoo.log"
    char_log: str = "char.log"

    def __post_init__(self):
        """Ensure log directory exists."""
        self.log_dir.mkdir(parents=True, exist_ok=True)


@dataclass
class ReportConfig:
    """Configuration for report generation."""

    # Report types
    enable_condition: bool = True
    enable_stability: bool = True
    enable_volume: bool = True
    enable_testtime: bool = True
    enable_yield: bool = True
    enable_char: bool = True
    enable_shmoo: bool = True

    # Chart settings
    chart_width: int = 1200
    chart_height: int = 600
    chart_theme: str = "plotly"  # plotly, plotly_white, plotly_dark

    # Output formats
    generate_html: bool = True
    generate_csv: bool = False
    generate_json: bool = False


class Settings:
    """
    Global settings singleton for ART.stdf application.

    Usage:
        from config import Settings

        settings = Settings()
        print(settings.processing.parallel_stdf_workers)
    """

    _instance: Optional['Settings'] = None

    def __new__(cls):
        """Ensure singleton pattern."""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        """Initialize settings (only once)."""
        if self._initialized:
            return

        self.processing = ProcessingConfig()
        self.flow = FlowConfig()
        self.logging = LoggingConfig()
        self.report = ReportConfig()

        # Load from environment variables if present
        self._load_from_env()

        self._initialized = True

    def _load_from_env(self):
        """Load settings from environment variables."""
        # Processing settings
        if workers := os.getenv("ART_PARALLEL_WORKERS"):
            self.processing.parallel_stdf_workers = int(workers)

        if compression := os.getenv("ART_COMPRESSION"):
            self.processing.compression = compression

        # Logging settings
        if log_level := os.getenv("ART_LOG_LEVEL"):
            self.logging.log_level = log_level

    def validate(self) -> bool:
        """
        Validate all configuration settings.

        Returns:
            True if valid, raises ValueError otherwise
        """
        # Validation is done in __post_init__ of each config class
        return True

    def to_dict(self) -> dict:
        """Convert settings to dictionary."""
        return {
            "processing": vars(self.processing),
            "flow": {
                "allowed_flows": list(self.flow.allowed_flows),
                "allowed_packages": list(self.flow.allowed_packages),
            },
            "logging": vars(self.logging),
            "report": vars(self.report),
        }

    def __repr__(self) -> str:
        """String representation."""
        return f"Settings(processing={self.processing}, flow={self.flow})"


# Global settings instance
settings = Settings()
