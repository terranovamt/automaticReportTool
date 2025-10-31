"""
Data models for ART.stdf

This module defines all data structures used throughout the application.
"""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Optional, Dict, List, Any
import polars as pl


class ProcessType(Enum):
    """Enumeration for different processing types."""
    STDF2DATA = "stdf2data"
    DATA2REPORT = "data2report"
    CONDITION2REPORT = "condition2report"
    CHAR = "char"
    SHMOO = "shmoo"


class FileStatus(Enum):
    """Status of file processing."""
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


class ReportType(Enum):
    """Types of reports that can be generated."""
    CONDITION = "condition"
    STABILITY = "stability"
    VOLUME = "volume"
    TESTTIME = "testtime"
    YIELD = "yield"
    CHAR = "char"
    SHMOO = "shmoo"


@dataclass
class STDFFile:
    """
    Represents an STDF file to be processed.

    Attributes:
        path: Full path to STDF file
        filename: File name only
        status: Current processing status
        created_at: File creation timestamp
        size_bytes: File size in bytes
        checksum: Optional file checksum for validation
    """
    path: Path
    filename: str
    status: FileStatus = FileStatus.PENDING
    created_at: Optional[datetime] = None
    size_bytes: Optional[int] = None
    checksum: Optional[str] = None

    def __post_init__(self):
        """Initialize derived attributes."""
        self.path = Path(self.path)
        if not self.filename:
            self.filename = self.path.name
        if not self.created_at and self.path.exists():
            self.created_at = datetime.fromtimestamp(self.path.stat().st_mtime)
        if not self.size_bytes and self.path.exists():
            self.size_bytes = self.path.stat().st_size

    @property
    def is_compressed(self) -> bool:
        """Check if file is gzip compressed."""
        return self.path.suffix.lower() in {".gz"}

    @property
    def is_valid_stdf(self) -> bool:
        """Check if file has valid STDF extension."""
        name_lower = self.filename.lower()
        return name_lower.endswith((".std", ".stdf", ".std.gz", ".stdf.gz"))

    def __repr__(self) -> str:
        """String representation."""
        return f"STDFFile(path={self.path}, status={self.status.value})"


@dataclass
class ProcessingResult:
    """
    Result of a file processing operation.

    Attributes:
        success: Whether processing succeeded
        file_path: Path to processed file
        output_files: List of output files created
        error_message: Error message if failed
        processing_time: Time taken in seconds
        records_processed: Number of records processed
        metadata: Additional metadata
    """
    success: bool
    file_path: Path
    output_files: List[Path] = field(default_factory=list)
    error_message: Optional[str] = None
    processing_time: Optional[float] = None
    records_processed: Optional[int] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __repr__(self) -> str:
        """String representation."""
        status = "SUCCESS" if self.success else "FAILED"
        return f"ProcessingResult({status}, {self.file_path})"


@dataclass
class Parameter:
    """
    Extracted parameters from file path/content.

    This represents the test parameters extracted from STDF file paths
    and content, used for report generation.

    Attributes:
        CUT: Product code (3 characters, e.g., '44E')
        FLOW: Test flow (e.g., 'EWS1', 'FT')
        LOT: Lot ID
        WAFER: Wafer ID
        TYPE: Report type (e.g., 'CONDITION', 'VOLUME')
        PACKAGE: Package type (e.g., 'QFP', 'BGA')
        CODE: Full product code
        FILE: File information dictionary
        COM: Composite name
        MAIN: Main test category
        TEST_NUM: Test numbers (single or list)
        EWSLOT: EWS lot ID
        temperature: Test temperature
        xwafer: Wafer X coordinate range
        ywafer: Wafer Y coordinate range
    """
    CUT: str
    FLOW: str
    LOT: str
    WAFER: str
    TYPE: str
    PACKAGE: str = ""
    CODE: str = ""
    FILE: Dict = field(default_factory=dict)
    COM: str = ""
    MAIN: str = ""
    TEST_NUM: Any = None  # Can be int or List[int]
    EWSLOT: str = ""
    temperature: int = 30
    xwafer: tuple = (0, 30)
    ywafer: tuple = (0, 30)
    additional: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        """Validate parameters after initialization."""
        if not self.CUT:
            raise ValueError("CUT parameter is required")
        if not self.FLOW:
            raise ValueError("FLOW parameter is required")
        if not self.LOT:
            raise ValueError("LOT parameter is required")

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        return {
            "CUT": self.CUT,
            "FLOW": self.FLOW,
            "LOT": self.LOT,
            "WAFER": self.WAFER,
            "TYPE": self.TYPE,
            "PACKAGE": self.PACKAGE,
            "CODE": self.CODE,
            "FILE": self.FILE,
            "COM": self.COM,
            "MAIN": self.MAIN,
            "TEST_NUM": self.TEST_NUM,
            "EWSLOT": self.EWSLOT,
            "temperature": self.temperature,
            "xwafer": self.xwafer,
            "ywafer": self.ywafer,
            **self.additional,
        }


@dataclass
class STDFData:
    """
    Container for parsed STDF data as DataFrames.

    Attributes:
        mir: Master Information Record
        mrr: Master Results Record
        prr: Part Results Record
        ptr: Parametric Test Record
        ftr: Functional Test Record
        pcr: Part Count Record
        hbr: Hardware Bin Record
        sbr: Software Bin Record
        additional: Additional record types
    """
    mir: Optional[pl.DataFrame] = None
    mrr: Optional[pl.DataFrame] = None
    prr: Optional[pl.DataFrame] = None
    ptr: Optional[pl.DataFrame] = None
    ftr: Optional[pl.DataFrame] = None
    pcr: Optional[pl.DataFrame] = None
    hbr: Optional[pl.DataFrame] = None
    sbr: Optional[pl.DataFrame] = None
    additional: Dict[str, pl.DataFrame] = field(default_factory=dict)

    def get(self, record_type: str) -> Optional[pl.DataFrame]:
        """
        Get DataFrame by record type.

        Args:
            record_type: Record type (e.g., 'ptr', 'ftr')

        Returns:
            DataFrame if exists, None otherwise
        """
        record_type = record_type.lower()
        if hasattr(self, record_type):
            return getattr(self, record_type)
        return self.additional.get(record_type)

    def set(self, record_type: str, df: pl.DataFrame):
        """
        Set DataFrame for record type.

        Args:
            record_type: Record type
            df: DataFrame to set
        """
        record_type = record_type.lower()
        if hasattr(self, record_type):
            setattr(self, record_type, df)
        else:
            self.additional[record_type] = df

    @property
    def record_types(self) -> List[str]:
        """Get list of available record types."""
        types = []
        for attr in ['mir', 'mrr', 'prr', 'ptr', 'ftr', 'pcr', 'hbr', 'sbr']:
            if getattr(self, attr) is not None:
                types.append(attr)
        types.extend(self.additional.keys())
        return types


@dataclass
class Report:
    """
    Generated report metadata.

    Attributes:
        report_type: Type of report
        output_path: Path to generated report
        created_at: Creation timestamp
        parameter: Parameters used for generation
        charts: List of chart file paths
        metadata: Additional metadata
    """
    report_type: ReportType
    output_path: Path
    created_at: datetime = field(default_factory=datetime.now)
    parameter: Optional[Parameter] = None
    charts: List[Path] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __repr__(self) -> str:
        """String representation."""
        return f"Report({self.report_type.value}, {self.output_path})"
