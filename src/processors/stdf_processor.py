"""STDF to Parquet processor - main conversion pipeline"""

import time
from pathlib import Path
from typing import Optional
from src.processors.base import BaseProcessor
from src.core.models import ProcessingResult
from src.core.exceptions import ProcessingError
from src.utils.validation import validate_stdf_file
from conversion import stdf2data  # STDF to Parquet converter


class STDFProcessor(BaseProcessor):
    """
    Processor for converting STDF files to Parquet format.

    This is the main processor that handles STDF → Parquet conversion
    using the optimized pystdf library.
    """

    def __init__(self):
        super().__init__("STDFProcessor")

    def validate_input(self, input_path: Path) -> bool:
        """Validate STDF file."""
        return validate_stdf_file(input_path)

    def process(self, input_path: Path, output_path: Path) -> ProcessingResult:
        """
        Convert STDF file to Parquet files.

        Args:
            input_path: Path to STDF file
            output_path: Output directory for Parquet files

        Returns:
            ProcessingResult with conversion status
        """
        start_time = time.time()

        try:
            # Validate input
            self.validate_input(input_path)

            # Create output directory
            output_path = Path(output_path)
            output_path.mkdir(parents=True, exist_ok=True)

            # Convert using existing stdf2data module
            stdf2data.stdf2data_converter(str(input_path), str(output_path))

            # Collect output files
            output_files = list(output_path.glob("*.parquet"))

            processing_time = time.time() - start_time

            return ProcessingResult(
                success=True,
                file_path=input_path,
                output_files=output_files,
                processing_time=processing_time,
                metadata={"output_dir": str(output_path)}
            )

        except Exception as e:
            processing_time = time.time() - start_time
            return ProcessingResult(
                success=False,
                file_path=input_path,
                error_message=str(e),
                processing_time=processing_time
            )
