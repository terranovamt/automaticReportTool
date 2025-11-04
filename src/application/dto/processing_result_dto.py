"""
ART.stdf - Processing Result DTO

Data Transfer Objects for processing results.

Author: Matteo Terranova (matteo.terranova@st.com)
Organization: STMicroelectronics - MDRF GPAM
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Optional


@dataclass
class ProcessingResultDTO:
    """
    Data Transfer Object for processing results.

    Attributes:
        success: Whether processing succeeded
        file_path: Path to processed file
        output_path: Path to output (parquet/report)
        error: Error message if failed
        duration_seconds: Processing duration
    """

    success: bool
    file_path: Path
    output_path: Optional[Path] = None
    error: Optional[str] = None
    duration_seconds: Optional[float] = None

    def __str__(self) -> str:
        """String representation."""
        if self.success:
            return f"✓ {self.file_path.name} -> {self.output_path}"
        else:
            return f"✗ {self.file_path.name}: {self.error}"


@dataclass
class CycleResultDTO:
    """
    Data Transfer Object for polling cycle results.

    Attributes:
        stdf_count: Number of STDF files processed
        report_count: Number of reports generated
        condition_count: Number of condition files processed
        shmoo_count: Number of shmoo directories processed
        char_count: Number of char datasets processed
        total_count: Total items processed
    """

    stdf_count: int = 0
    report_count: int = 0
    condition_count: int = 0
    shmoo_count: int = 0
    char_count: int = 0

    @property
    def total_count(self) -> int:
        """Get total number of items processed."""
        return (
            self.stdf_count +
            self.report_count +
            self.condition_count +
            self.shmoo_count +
            self.char_count
        )

    def __str__(self) -> str:
        """String representation."""
        return (
            f"STDF={self.stdf_count}, Report={self.report_count}, "
            f"Condition={self.condition_count}, SHMOO={self.shmoo_count}, "
            f"CHAR={self.char_count}"
        )
