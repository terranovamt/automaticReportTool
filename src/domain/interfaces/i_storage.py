"""
ART.stdf - Storage Interface

Port interface for storage operations (Clean Architecture dependency inversion).

Author: Matteo Terranova (matteo.terranova@st.com)
Organization: STMicroelectronics - MDRF GPAM
"""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Dict, Optional

import polars as pl


class IFileRepository(ABC):
    """
    Abstract interface for file repository operations.

    This interface abstracts file system operations, allowing
    the domain layer to be independent of infrastructure details.
    """

    @abstractmethod
    def check_completion_marker(self, path: str, marker_name: str = None) -> bool:
        """
        Check if completion marker exists.

        Args:
            path: Directory path
            marker_name: Marker file name

        Returns:
            True if marker exists
        """
        pass

    @abstractmethod
    def create_completion_marker(
        self,
        path: str,
        marker_name: str = None,
        content: str = None
    ) -> None:
        """
        Create completion marker.

        Args:
            path: Directory path
            marker_name: Marker file name
            content: Marker content
        """
        pass

    @abstractmethod
    def get_report_path(
        self,
        base_path: str,
        parameter: Dict,
        report_type: str
    ) -> Path:
        """
        Get report file path.

        Args:
            base_path: Base directory
            parameter: Parameter dictionary
            report_type: Type of report

        Returns:
            Path to report file
        """
        pass


class IParquetRepository(ABC):
    """
    Abstract interface for Parquet data operations.

    Handles reading and writing Parquet columnar data.
    """

    @abstractmethod
    def read(self, path: str, columns: Optional[list] = None) -> pl.DataFrame:
        """
        Read Parquet file.

        Args:
            path: Path to Parquet file
            columns: Optional columns to read

        Returns:
            Polars DataFrame
        """
        pass

    @abstractmethod
    def write(
        self,
        df: pl.DataFrame,
        path: str,
        compression: str = "lz4"
    ) -> None:
        """
        Write DataFrame to Parquet.

        Args:
            df: DataFrame to write
            path: Output path
            compression: Compression format
        """
        pass

    @abstractmethod
    def exists(self, path: str) -> bool:
        """
        Check if Parquet file exists.

        Args:
            path: Path to check

        Returns:
            True if file exists
        """
        pass
