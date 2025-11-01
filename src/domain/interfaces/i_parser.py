"""
ART.stdf - Parser Interface

Port interface for file parsers (Clean Architecture dependency inversion).

Author: Matteo Terranova (matteo.terranova@st.com)
Organization: STMicroelectronics - MDRF GPAM
"""

from abc import ABC, abstractmethod
from typing import Dict
from pathlib import Path

import polars as pl


class IParser(ABC):
    """
    Abstract interface for file parsers.

    This interface allows the domain layer to depend on abstractions
    rather than concrete implementations, following the Dependency
    Inversion Principle.
    """

    @abstractmethod
    def parse(self, file_path: str) -> Dict:
        """
        Parse a file and return structured data.

        Args:
            file_path: Path to file to parse

        Returns:
            Dictionary with parsed data

        Raises:
            FileNotFoundError: If file doesn't exist
            ValueError: If file format is invalid
        """
        pass


class ISTDFParser(IParser):
    """
    Interface for STDF file parsers.

    Extends IParser with STDF-specific methods.
    """

    @abstractmethod
    def parse_to_parquet(
        self,
        stdf_path: str,
        output_dir: str,
        compression: str = "lz4"
    ) -> Dict[str, Path]:
        """
        Parse STDF file and convert to Parquet format.

        Args:
            stdf_path: Path to STDF file
            output_dir: Output directory for Parquet files
            compression: Compression format

        Returns:
            Dictionary mapping record types to parquet paths
        """
        pass

    @abstractmethod
    def read_parquet_record(
        self,
        parquet_path: str,
        columns: list = None
    ) -> pl.DataFrame:
        """
        Read a parquet record into a DataFrame.

        Args:
            parquet_path: Path to parquet file
            columns: Optional list of columns to read

        Returns:
            Polars DataFrame with data
        """
        pass


class IConditionParser(IParser):
    """
    Interface for condition file parsers.

    Parses HTML anaflow files to extract test conditions.
    """

    @abstractmethod
    def parse_anaflow(self, html_path: str) -> Dict:
        """
        Parse anaflow HTML file.

        Args:
            html_path: Path to anaflow HTML file

        Returns:
            Dictionary with extracted conditions
        """
        pass


class IShmooParser(IParser):
    """
    Interface for shmoo file parsers.

    Parses .shm files for shmoo plot generation.
    """

    @abstractmethod
    def parse_shmoo_file(self, shm_path: str) -> Dict:
        """
        Parse shmoo file.

        Args:
            shm_path: Path to .shm file

        Returns:
            Dictionary with shmoo data
        """
        pass
