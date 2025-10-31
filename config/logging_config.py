"""
Logging configuration for ART.stdf

This module provides centralized logging setup with rotation and formatting.
"""

import logging
import sys
from pathlib import Path
from typing import Optional
from logging.handlers import BaseRotatingHandler


class LineCountRotatingFileHandler(BaseRotatingHandler):
    """
    Custom rotating file handler that rotates based on line count.

    This is more predictable than size-based rotation for log analysis.
    """

    def __init__(
        self,
        filename: str,
        max_lines: int = 1000,
        backup_count: int = 1,
        encoding: Optional[str] = None,
    ):
        """
        Initialize the handler.

        Args:
            filename: Log file path
            max_lines: Maximum lines before rotation
            backup_count: Number of backup files to keep
            encoding: File encoding
        """
        self.max_lines = max_lines
        self.backup_count = backup_count
        self.current_line_count = 0

        super().__init__(filename, "a", encoding=encoding, delay=False)

        # Count existing lines in file
        if Path(self.baseFilename).exists():
            with open(self.baseFilename, "r", encoding=encoding) as f:
                self.current_line_count = sum(1 for _ in f)

    def shouldRollover(self, record) -> bool:
        """
        Determine if rollover should occur.

        Args:
            record: Log record

        Returns:
            True if should rollover
        """
        return self.current_line_count >= self.max_lines

    def doRollover(self):
        """Perform log file rollover."""
        if self.stream:
            self.stream.close()
            self.stream = None

        # Rotate existing backup files
        for i in range(self.backup_count - 1, 0, -1):
            sfn = self.rotation_filename(f"{self.baseFilename}.{i}")
            dfn = self.rotation_filename(f"{self.baseFilename}.{i + 1}")
            if Path(sfn).exists():
                if Path(dfn).exists():
                    Path(dfn).unlink()
                Path(sfn).rename(dfn)

        # Rename current file to .1
        dfn = self.rotation_filename(f"{self.baseFilename}.1")
        if Path(dfn).exists():
            Path(dfn).unlink()
        if Path(self.baseFilename).exists():
            Path(self.baseFilename).rename(dfn)

        # Reset line count
        self.current_line_count = 0

        # Reopen stream
        if not self.delay:
            self.stream = self._open()

    def emit(self, record):
        """
        Emit a log record.

        Args:
            record: Log record to emit
        """
        try:
            super().emit(record)
            self.current_line_count += 1
        except Exception:
            self.handleError(record)


def setup_logging(
    log_dir: Path,
    log_level: str = "INFO",
    max_lines: int = 1000,
    backup_count: int = 1,
    log_to_console: bool = True,
) -> None:
    """
    Setup global logging configuration.

    Args:
        log_dir: Directory for log files
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        max_lines: Maximum lines per log file before rotation
        backup_count: Number of backup files to keep
        log_to_console: Whether to log to console as well
    """
    # Create log directory
    log_dir = Path(log_dir)
    log_dir.mkdir(parents=True, exist_ok=True)

    # Get root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, log_level.upper()))

    # Clear existing handlers
    root_logger.handlers.clear()

    # Log format
    log_format = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # Console handler
    if log_to_console:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(log_format)
        root_logger.addHandler(console_handler)

    # File handler for main log
    main_log_file = log_dir / "art.log"
    file_handler = LineCountRotatingFileHandler(
        filename=str(main_log_file),
        max_lines=max_lines,
        backup_count=backup_count,
    )
    file_handler.setLevel(getattr(logging, log_level.upper()))
    file_handler.setFormatter(log_format)
    root_logger.addHandler(file_handler)


def get_logger(name: str, log_file: Optional[str] = None, log_dir: Optional[Path] = None) -> logging.Logger:
    """
    Get a logger with optional dedicated log file.

    Args:
        name: Logger name
        log_file: Optional separate log file name
        log_dir: Log directory (uses default if None)

    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(name)

    # If specific log file requested, add dedicated handler
    if log_file and log_dir:
        log_dir = Path(log_dir)
        log_dir.mkdir(parents=True, exist_ok=True)

        log_path = log_dir / log_file
        handler = LineCountRotatingFileHandler(
            filename=str(log_path),
            max_lines=1000,
            backup_count=1,
        )
        handler.setFormatter(logging.Formatter(
            "%(asctime)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        ))
        logger.addHandler(handler)

    return logger


def configure_component_loggers(log_dir: Path) -> dict[str, logging.Logger]:
    """
    Configure loggers for different system components.

    Args:
        log_dir: Directory for log files

    Returns:
        Dictionary mapping component names to loggers
    """
    components = {
        "stdf2data": "stdf2data.log",
        "data2report": "data2report.log",
        "condition2report": "condition2report.log",
        "char": "char.log",
        "shmoo": "shmoo.log",
    }

    loggers = {}
    for component, log_file in components.items():
        loggers[component] = get_logger(component, log_file, log_dir)

    return loggers
