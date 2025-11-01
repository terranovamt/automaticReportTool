"""
ART.stdf - Logging Configuration

Centralized logging configuration for the ART.stdf system.

Author: Matteo Terranova (matteo.terranova@st.com)
Organization: STMicroelectronics - MDRF GPAM
"""

import logging
from pathlib import Path
from typing import Dict

from config.settings import settings


class LoggerConfig:
    """Logger configuration manager."""

    # Standard log format
    LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

    # Log file names
    LOG_FILES: Dict[str, str] = {
        "polling": "polling.log",
        "stdf2data": "stdf2data.log",
        "data2report": "data2report.log",
        "condition2report": "condition2report.log",
        "shmoo": "shmoo.log",
        "char": "char.log",
        "system": "system.log",
    }

    @classmethod
    def get_log_path(cls, logger_name: str) -> Path:
        """
        Get the full path for a logger's log file.

        Args:
            logger_name: Name of the logger

        Returns:
            Path to the log file
        """
        log_file = cls.LOG_FILES.get(logger_name, f"{logger_name}.log")
        return settings.LOG_DIR / log_file

    @classmethod
    def get_logger_level(cls) -> int:
        """
        Get logging level based on debug mode.

        Returns:
            Logging level constant
        """
        return logging.DEBUG if settings.DEBUG else logging.INFO

    @classmethod
    def configure_root_logger(cls):
        """Configure the root logger with basic settings."""
        logging.basicConfig(
            level=cls.get_logger_level(),
            format=cls.LOG_FORMAT,
            datefmt=cls.DATE_FORMAT,
        )


# Initialize logging configuration
LoggerConfig.configure_root_logger()
