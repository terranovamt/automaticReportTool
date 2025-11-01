"""
ART.stdf - STDF Parser

Parser adapter for STDF (Standard Test Data Format) files.
Uses the pystdf library for actual parsing operations.

Author: Matteo Terranova (matteo.terranova@st.com)
Organization: STMicroelectronics - MDRF GPAM
"""

import os
from pathlib import Path
from typing import Dict, Optional

import polars as pl

from src.infrastructure.pystdf.Importer import STDF2ParquetFiles
from src.infrastructure.storage.compression_handler import CompressionHandler
from src.infrastructure.storage.file_repository import FileRepository


class STDFParser:
    """
    Parser for STDF files with support for compressed formats.

    This class provides a high-level interface for converting STDF files
    to Parquet format, handling decompression automatically.
    """

    def __init__(self):
        """Initialize STDF parser."""
        self.compression_handler = CompressionHandler()
        self.file_repository = FileRepository()

    def parse_to_parquet(
        self,
        stdf_path: str,
        output_dir: str,
        compression: str = "lz4"
    ) -> Dict[str, Path]:
        """
        Parse STDF file and convert to Parquet format.

        This method:
        1. Decompresses the file if necessary
        2. Parses the STDF binary format
        3. Converts to Parquet columnar format
        4. Saves multiple parquet files (one per record type)

        Args:
            stdf_path: Path to STDF file (compressed or uncompressed)
            output_dir: Directory where Parquet files will be saved
            compression: Parquet compression format (lz4, zstd, snappy, gzip)

        Returns:
            Dictionary mapping record types to their parquet file paths
            Example: {"ptr": "/path/to/file.ptr.parquet", "ftr": ...}

        Example:
            >>> parser = STDFParser()
            >>> files = parser.parse_to_parquet(
            ...     "test.std.gz",
            ...     "/output/parquet"
            ... )
            >>> print(files["ptr"])  # Access parametric test records
        """
        stdf_path = Path(stdf_path)
        output_dir = Path(output_dir)

        # Ensure output directory exists
        output_dir.mkdir(parents=True, exist_ok=True)

        # Handle compressed files
        temp_dir = None
        working_path = stdf_path

        if self.compression_handler.is_compressed(str(stdf_path)):
            # Create temp directory for decompression
            temp_dir = output_dir / "tmp"
            temp_dir.mkdir(exist_ok=True)

            # Decompress file
            decompressed = self.compression_handler.decompress_file(
                str(stdf_path),
                str(temp_dir),
                remove_compressed=False
            )

            if decompressed:
                working_path = decompressed
            else:
                raise ValueError(f"Failed to decompress {stdf_path}")

        try:
            # Convert STDF to Parquet using pystdf library
            created_files = STDF2ParquetFiles(
                str(working_path),
                str(output_dir),
                use_polars=True,
                compression=compression
            )

            # Build result dictionary
            result = {}
            for file_path in created_files:
                file_path = Path(file_path)
                # Extract record type from filename (e.g., "file.ptr.parquet" -> "ptr")
                parts = file_path.stem.split(".")
                if len(parts) >= 2:
                    record_type = parts[-1]
                    result[record_type] = file_path

            return result

        finally:
            # Clean up temp directory
            if temp_dir and temp_dir.exists():
                import shutil
                shutil.rmtree(temp_dir, ignore_errors=True)

    def read_parquet_record(
        self,
        parquet_path: str,
        columns: Optional[list] = None
    ) -> pl.DataFrame:
        """
        Read a parquet file into a Polars DataFrame.

        Args:
            parquet_path: Path to parquet file
            columns: Optional list of columns to read (default: all)

        Returns:
            Polars DataFrame with the data

        Example:
            >>> parser = STDFParser()
            >>> df = parser.read_parquet_record("file.ptr.parquet")
            >>> print(df.shape)
        """
        if not os.path.exists(parquet_path):
            raise FileNotFoundError(f"Parquet file not found: {parquet_path}")

        return pl.read_parquet(parquet_path, columns=columns)

    def get_parquet_path_for_stdf(
        self,
        stdf_path: str,
        record_type: str = "ptr"
    ) -> Path:
        """
        Get the expected parquet file path for a given STDF file and record type.

        Args:
            stdf_path: Path to original STDF file
            record_type: Type of record (ptr, ftr, mir, prr, etc.)

        Returns:
            Expected path to parquet file

        Example:
            >>> parser = STDFParser()
            >>> path = parser.get_parquet_path_for_stdf(
            ...     "/data/test.std.gz",
            ...     "ptr"
            ... )
            >>> print(path)
            /data/parquet/test.std.ptr.parquet
        """
        stdf_path = Path(stdf_path)
        base_dir = stdf_path.parent

        # Get parquet directory
        parquet_dir = self.file_repository.get_parquet_directory(str(base_dir))

        # Build filename: original_name.record_type.parquet
        filename = f"{stdf_path.name}.{record_type}.parquet"

        return parquet_dir / filename
