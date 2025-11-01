"""
ART.stdf - Logger Factory

Factory for creating configured loggers with custom rotating handlers.
Extracted and refactored from original polling.py module.

Author: Matteo Terranova (matteo.terranova@st.com)
Organization: STMicroelectronics - MDRF GPAM
"""

import logging
from pathlib import Path
from typing import Optional

from config.settings import settings
from config.logging_config import LoggerConfig
from src.infrastructure.logging.rotating_handler import LineCountRotatingFileHandler


class LoggerFactory:
    """
    Factory class for creating and managing loggers.

    This class provides methods to create loggers with custom rotating
    file handlers based on line count.
    """

    @staticmethod
    def create_logger(
        name: str,
        log_file: Optional[str] = None,
        level: Optional[int] = None,
        max_lines: Optional[int] = None,
        backup_count: Optional[int] = None,
    ) -> logging.Logger:
        """
        Create a logger with custom rotating file handler.

        Args:
            name: Logger name (also used as log file name if log_file not provided)
            log_file: Optional custom log file path
            level: Logging level (default: from config)
            max_lines: Maximum lines before rotation (default: from settings)
            backup_count: Number of backup files (default: from settings)

        Returns:
            Configured logger instance

        Example:
            >>> logger = LoggerFactory.create_logger("polling")
            >>> logger.info("Processing started")
        """
        # Determine log file path
        if log_file is None:
            log_path = LoggerConfig.get_log_path(name)
        else:
            log_path = Path(log_file)

        # Ensure log directory exists
        log_path.parent.mkdir(parents=True, exist_ok=True)

        # Set defaults from configuration
        if level is None:
            level = LoggerConfig.get_logger_level()

        if max_lines is None:
            max_lines = settings.MAX_LINES_PER_LOG

        if backup_count is None:
            backup_count = settings.LOG_BACKUP_COUNT

        # Create formatter
        formatter = logging.Formatter(
            LoggerConfig.LOG_FORMAT,
            datefmt=LoggerConfig.DATE_FORMAT
        )

        # Create rotating handler
        handler = LineCountRotatingFileHandler(
            filename=str(log_path),
            max_lines=max_lines,
            backup_count=backup_count
        )
        handler.setFormatter(formatter)

        # Create and configure logger
        logger = logging.getLogger(name)
        logger.setLevel(level)

        # Remove existing handlers to avoid duplicates
        logger.handlers.clear()

        # Add the new handler
        logger.addHandler(handler)

        return logger

    @staticmethod
    def get_logger(name: str) -> logging.Logger:
        """
        Get an existing logger or create a new one with default configuration.

        Args:
            name: Logger name

        Returns:
            Logger instance

        Example:
            >>> logger = LoggerFactory.get_logger("stdf2data")
            >>> logger.debug("Processing STDF file")
        """
        logger = logging.getLogger(name)

        # If logger doesn't have handlers, create it with default config
        if not logger.handlers:
            return LoggerFactory.create_logger(name)

        return logger


# Convenience function for backward compatibility
def setup_logger(name: str, log_file: str, level: int = logging.INFO) -> logging.Logger:
    """
    Set up a logger with custom rotating file handler.

    This function maintains backward compatibility with the original polling.py
    setup_logger function.

    Args:
        name: Logger name
        log_file: Path to log file
        level: Logging level

    Returns:
        Configured logger instance

    Example:
        >>> logger = setup_logger("polling", "log/polling.log")
        >>> logger.info("System started")
    """
    return LoggerFactory.create_logger(name, log_file, level)
