"""
ART.stdf - Convert STDF Use Case

Use case for converting STDF files to Parquet format.
Extracted and refactored from original stdf2data.py and polling.py.

Author: Matteo Terranova (matteo.terranova@st.com)
Organization: STMicroelectronics - MDRF GPAM
"""

import logging
from pathlib import Path
from typing import Dict, Optional

from src.domain.models.parameter import Parameter
from src.infrastructure.parsers.stdf_parser import STDFParser
from src.infrastructure.storage.file_repository import FileRepository


class ConvertSTDFUseCase:
    """
    Use case for converting STDF files to Parquet format.

    This use case orchestrates the conversion of binary STDF files
    to efficient Parquet columnar format for further processing.

    Responsibilities:
    - Validate input STDF file exists
    - Create output directory structure
    - Parse STDF to Parquet
    - Verify conversion success
    - Log conversion progress
    """

    def __init__(
        self,
        stdf_parser: Optional[STDFParser] = None,
        file_repository: Optional[FileRepository] = None,
        logger: Optional[logging.Logger] = None
    ):
        """
        Initialize the use case.

        Args:
            stdf_parser: STDF parser instance (injected)
            file_repository: File repository instance (injected)
            logger: Logger instance (injected)
        """
        self.stdf_parser = stdf_parser or STDFParser()
        self.file_repository = file_repository or FileRepository()
        self.logger = logger or logging.getLogger(__name__)

    def execute(
        self,
        stdf_path: str,
        parameter: Optional[Parameter] = None,
        compression: str = "lz4"
    ) -> Dict[str, Path]:
        """
        Execute STDF to Parquet conversion.

        Args:
            stdf_path: Path to STDF file (compressed or uncompressed)
            parameter: Optional parameter object with metadata
            compression: Parquet compression format (default: lz4)

        Returns:
            Dictionary mapping record types to parquet file paths

        Raises:
            FileNotFoundError: If STDF file doesn't exist
            ValueError: If conversion fails

        Example:
            >>> use_case = ConvertSTDFUseCase()
            >>> result = use_case.execute("test.std.gz")
            >>> print(result["ptr"])  # Path to parametric test records
        """
        stdf_path = Path(stdf_path)

        # Validate input
        if not stdf_path.exists():
            raise FileNotFoundError(f"STDF file not found: {stdf_path}")

        # Log start
        if parameter:
            self._log_start(parameter)
        else:
            self.logger.info(f"[STDF2DATA] Converting {stdf_path.name}")

        # Determine output directory
        output_dir = self._get_output_directory(stdf_path)

        try:
            # Perform conversion
            parquet_files = self.stdf_parser.parse_to_parquet(
                str(stdf_path),
                str(output_dir),
                compression=compression
            )

            # Verify conversion
            if not parquet_files:
                raise ValueError("No parquet files were created")

            # Log completion
            if parameter:
                self._log_completion(parameter)
            else:
                self.logger.info(
                    f"[STDF2DATA] Converted {stdf_path.name} -> "
                    f"{len(parquet_files)} parquet files"
                )

            return parquet_files

        except Exception as e:
            self.logger.error(f"[STDF2DATA] Conversion failed for {stdf_path}: {e}")
            raise

    def _get_output_directory(self, stdf_path: Path) -> Path:
        """
        Determine output directory for parquet files.

        Args:
            stdf_path: Path to STDF file

        Returns:
            Path to parquet output directory
        """
        base_dir = stdf_path.parent
        return self.file_repository.ensure_parquet_directory(str(base_dir))

    def _log_start(self, parameter: Parameter):
        """Log conversion start with parameter details."""
        self.logger.info(
            f"[STDF2DATA] Start conversion {parameter.cut} {parameter.flow} "
            f"{parameter.lot} {parameter.wafer} {parameter.type}"
        )

    def _log_completion(self, parameter: Parameter):
        """Log conversion completion with parameter details."""
        self.logger.info(
            f"[STDF2DATA] Completed conversion {parameter.cut} {parameter.flow} "
            f"{parameter.lot} {parameter.wafer} {parameter.type}"
        )

    def is_conversion_needed(self, stdf_path: str) -> bool:
        """
        Check if STDF file needs conversion.

        Args:
            stdf_path: Path to STDF file

        Returns:
            True if conversion is needed, False if parquet files exist

        Example:
            >>> use_case = ConvertSTDFUseCase()
            >>> if use_case.is_conversion_needed("test.std"):
            ...     use_case.execute("test.std")
        """
        stdf_path = Path(stdf_path)
        parquet_dir = self.file_repository.get_parquet_directory(str(stdf_path.parent))

        if not parquet_dir.exists():
            return True

        # Check for essential parquet files
        essential_types = ["ptr", "ftr", "mir", "prr"]
        for record_type in essential_types:
            expected_file = parquet_dir / f"{stdf_path.name}.{record_type}.parquet"
            if not expected_file.exists():
                return True

        # Count total parquet files
        parquet_files = list(parquet_dir.glob(f"{stdf_path.name}.*.parquet"))
        return len(parquet_files) < 8  # Expect at least 8 different record types
