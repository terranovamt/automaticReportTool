"""
Custom exceptions for ART.stdf

This module defines all custom exceptions used throughout the application.
"""


class ARTError(Exception):
    """Base exception for all ART.stdf errors."""

    def __init__(self, message: str, details: dict = None):
        """
        Initialize ARTError.

        Args:
            message: Error message
            details: Optional additional error details
        """
        super().__init__(message)
        self.message = message
        self.details = details or {}

    def __str__(self) -> str:
        """String representation."""
        if self.details:
            details_str = ", ".join(f"{k}={v}" for k, v in self.details.items())
            return f"{self.message} ({details_str})"
        return self.message


class ConfigurationError(ARTError):
    """Raised when configuration is invalid or missing."""
    pass


class ValidationError(ARTError):
    """Raised when data validation fails."""
    pass


class ProcessingError(ARTError):
    """Raised when data processing fails."""
    pass


class ParsingError(ARTError):
    """Raised when file parsing fails."""
    pass


class FileNotFoundError(ARTError):
    """Raised when a required file is not found."""
    pass


class ConversionError(ProcessingError):
    """Raised when data format conversion fails."""
    pass


class ReportGenerationError(ProcessingError):
    """Raised when report generation fails."""
    pass


class STDFFormatError(ParsingError):
    """Raised when STDF file format is invalid."""
    pass


class ParquetError(ProcessingError):
    """Raised when Parquet operations fail."""
    pass


class ParameterExtractionError(ParsingError):
    """Raised when parameter extraction from path/file fails."""
    pass


class MonitoringError(ARTError):
    """Raised when directory monitoring fails."""
    pass
