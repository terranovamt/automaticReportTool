"""
ART.stdf - Centralized Configuration Settings

This module contains all global configuration settings for the ART.stdf system.
Settings can be overridden via environment variables.

Author: Matteo Terranova (matteo.terranova@st.com)
Organization: STMicroelectronics - MDRF GPAM
"""

import os
from pathlib import Path
from typing import Set


class Settings:
    """Global application settings."""

    # Project Information
    PROJECT_NAME = "ART.stdf"
    VERSION = "2.0.0"
    AUTHOR = "Matteo Terranova"
    EMAIL = "matteo.terranova@st.com"
    ORGANIZATION = "STMicroelectronics - MDRF GPAM"
    SITE = "Catania"

    # Base Paths
    BASE_DIR = Path(__file__).resolve().parent.parent
    SRC_DIR = BASE_DIR / "src"
    CONFIG_DIR = BASE_DIR / "config"
    DOCS_DIR = BASE_DIR / "docs"
    TESTS_DIR = BASE_DIR / "tests"
    LOG_DIR = BASE_DIR / "log"

    # Default Watch Path (can be overridden via environment variable)
    DEFAULT_WATCH_PATH = os.getenv(
        "ART_WATCH_PATH",
        r"\\gpm-pe-data.gnb.st.com\ENGI_MCD_STDF"
    )

    # Alternative paths for different environments
    LOCAL_WATCH_PATH = BASE_DIR / "STDF"
    UNIX_WATCH_PATH = Path("/prj/ENGI_MCD_STDF")

    # Polling Configuration
    POLLING_INTERVAL_SECONDS = int(os.getenv("ART_POLLING_INTERVAL", "10"))

    # Logging Configuration
    MAX_LINES_PER_LOG = int(os.getenv("ART_MAX_LOG_LINES", "1000"))
    LOG_BACKUP_COUNT = int(os.getenv("ART_LOG_BACKUP_COUNT", "1"))

    # File Processing Configuration
    ALLOWED_FLOWS: Set[str] = {
        "EWS1", "EWS2", "EWS3", "EWSDIE", "FT", "FT1", "FT2", "EWSCHAR"
    }

    ALLOWED_PACKAGES: Set[str] = {
        "QFP", "QFN", "DIP", "WLCSP", "CSP", "BGA"
    }

    # STDF File Extensions
    STDF_EXTENSIONS = {".std", ".stdf", ".STDF"}
    COMPRESSION_EXTENSIONS = {".gz", ".7z", ".zip", ".bz2", ".xz", ".tar", ".rar"}

    # Parquet Configuration
    PARQUET_COMPRESSION = os.getenv("ART_PARQUET_COMPRESSION", "lz4")
    PARQUET_FOLDER_NAME = "parquet"

    # Report Configuration
    REPORT_FOLDER_NAME = "Report"
    REPORT_COMPLETION_MARKER = "REPORT DONE.txt"
    COMPLETION_MARKER_CONTENT = (
        "IF YOU READ THIS ALL REPORT HAVE BEEN GENERATED\n"
        "THIS FOLDER WILL BE SKIPPED\n"
        "IN CASE DELETE THIS FILE END REPORT YOU WANT TO REGENERATE AND WAIT"
    )

    # SVN Configuration
    SVN_BASE_URL = "svn://mcd-pe-svn.gnb.st.com/prj/ENGI_MCD_SVN/TPI_REPO/trunk"
    SVN_COMPOSITES_PATH = "{productcut}/{flow}/cnf/composites.cnf"

    # Jupyter Configuration
    JUPYTER_NOTEBOOKS_DIR = SRC_DIR / "presentation" / "notebooks" / "templates"
    JUPYTER_TIMEOUT_SECONDS = int(os.getenv("ART_JUPYTER_TIMEOUT", "600"))

    # History Configuration
    HISTORY_FILE = "history.parquet"

    # Performance Configuration
    ENABLE_PERFORMANCE_MONITORING = os.getenv("ART_PERFORMANCE_MONITORING", "false").lower() == "true"

    # Debug Mode
    DEBUG = os.getenv("ART_DEBUG", "false").lower() == "true"

    @classmethod
    def get_watch_path(cls, custom_path: str = None) -> Path:
        """
        Get the watch path for STDF file monitoring.

        Args:
            custom_path: Optional custom path to use

        Returns:
            Path object for the watch directory
        """
        if custom_path:
            return Path(custom_path)

        # Check if default network path is accessible
        try:
            default_path = Path(cls.DEFAULT_WATCH_PATH)
            if default_path.exists():
                return default_path
        except:
            pass

        # Fall back to local path
        return cls.LOCAL_WATCH_PATH

    @classmethod
    def ensure_directories(cls):
        """Create necessary directories if they don't exist."""
        cls.LOG_DIR.mkdir(parents=True, exist_ok=True)
        cls.LOCAL_WATCH_PATH.mkdir(parents=True, exist_ok=True)


# Create global settings instance
settings = Settings()
