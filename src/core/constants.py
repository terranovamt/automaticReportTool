"""
Constants for ART.stdf

This module defines all constants used throughout the application.
"""

from typing import Final

# File extensions
STDF_EXTENSIONS: Final[tuple] = (".std", ".stdf", ".std.gz", ".stdf.gz")
PARQUET_EXTENSIONS: Final[tuple] = (".parquet",)
HTML_EXTENSIONS: Final[tuple] = (".html", ".htm")
CSV_EXTENSIONS: Final[tuple] = (".csv",)
JSON_EXTENSIONS: Final[tuple] = (".json",)

# Compression types
DEFAULT_COMPRESSION: Final[str] = "lz4"
COMPRESSION_TYPES: Final[tuple] = ("lz4", "snappy", "gzip", "zstd", "uncompressed")

# STDF Record types (for reference)
RECORD_TYPES: Final[dict] = {
    "FAR": "File Attributes Record",
    "ATR": "Audit Trail Record",
    "MIR": "Master Information Record",
    "MRR": "Master Results Record",
    "PCR": "Part Count Record",
    "HBR": "Hardware Bin Record",
    "SBR": "Software Bin Record",
    "PMR": "Pin Map Record",
    "PGR": "Pin Group Record",
    "PLR": "Pin List Record",
    "RDR": "Retest Data Record",
    "SDR": "Site Description Record",
    "WIR": "Wafer Information Record",
    "WRR": "Wafer Results Record",
    "WCR": "Wafer Configuration Record",
    "PIR": "Part Information Record",
    "PRR": "Part Results Record",
    "TSR": "Test Synopsis Record",
    "PTR": "Parametric Test Record",
    "MPR": "Multiple-Result Parametric Record",
    "FTR": "Functional Test Record",
    "BPS": "Begin Program Section Record",
    "EPS": "End Program Section Record",
    "GDR": "Generic Data Record",
    "DTR": "Datalog Text Record",
}

# Test record types that contain test results
TEST_RECORD_TYPES: Final[tuple] = ("PTR", "MPR", "FTR")

# Record types that need PART_ID
PART_ID_RECORDS: Final[tuple] = ("PIR", "PRR", "PTR", "MPR", "FTR")

# Report types
REPORT_TYPES: Final[dict] = {
    "CONDITION": "Condition Analysis Report",
    "STABILITY": "Stability/Loop Report",
    "VOLUME": "Volume IP Report",
    "TESTTIME": "Test Time Analysis Report",
    "YIELD": "Yield Analysis Report",
    "CHAR": "Characterization Report",
    "SHMOO": "Shmoo Plot Report",
}

# Flow types
FLOW_TYPES: Final[tuple] = (
    "EWS1", "EWS2", "EWS3", "EWSDIE", "EWSCHAR",
    "FT", "FT1", "FT2",
)

# Package types
PACKAGE_TYPES: Final[tuple] = ("QFP", "QFN", "DIP", "WLCSP", "CSP", "BGA")

# Composite patterns for test name parsing
COMPOSITE_PATTERN: Final[str] = r"(?P<TestName>.*)_(?P<COM>{composite})_(?P<TARGET>.*)"
TTIME_PATTERN: Final[str] = r"(?P<COM>log_ttime)__(?P<TestName>.*)::(?P<TARGET>.*)"
VDD_PATTERN: Final[str] = r"(?P<TestName>.+)_(?P<SplitName>{vdd_types})_(?P<Split>[^_]+)_(?P<COM>{composite})_(?P<tmpfunc>[^:]+)(?::(?P<TARGET>.+))?"

# VDD split types
VDD_SPLIT_TYPES: Final[str] = "vio|vbt|v11|v12|v33|FRC|frc"

# Wafer coordinate ranges (default)
DEFAULT_WAFER_X_RANGE: Final[tuple] = (0, 30)
DEFAULT_WAFER_Y_RANGE: Final[tuple] = (0, 30)

# Test limits
MIN_TEST_COUNT: Final[int] = 1
MAX_TEST_COUNT: Final[int] = 100000

# Temperature defaults
DEFAULT_TEMPERATURE: Final[int] = 30  # Celsius
TEMPERATURE_ROUNDING: Final[int] = 5  # Round to nearest 5°C

# Yield calculations
MIN_YIELD_PERCENTAGE: Final[float] = 0.0
MAX_YIELD_PERCENTAGE: Final[float] = 100.0

# Process capability indices
MIN_CP: Final[float] = 10.0  # Minimum Cp for parametric tests (STABILITY)
MIN_CPK: Final[float] = 1.6  # Minimum CpK for volume tests (VOLUME IP)

# Parquet file naming
PARQUET_SUFFIX: Final[str] = ".parquet"
PARQUET_RECORD_SUFFIX: Final[dict] = {
    "mir": ".mir.parquet",
    "prr": ".prr.parquet",
    "ptr": ".ptr.parquet",
    "ftr": ".ftr.parquet",
    "pcr": ".pcr.parquet",
    "hbr": ".hbr.parquet",
    "sbr": ".sbr.parquet",
}

# Buffer sizes
DEFAULT_BUFFER_SIZE: Final[int] = 2 * 1024 * 1024  # 2MB
LARGE_BUFFER_SIZE: Final[int] = 8 * 1024 * 1024  # 8MB

# Chunk sizes for processing
DEFAULT_CHUNK_SIZE: Final[int] = 10000  # records per chunk
LARGE_CHUNK_SIZE: Final[int] = 50000

# Timeouts
FILE_LOCK_TIMEOUT: Final[int] = 30  # seconds
PROCESS_TIMEOUT: Final[int] = 3600  # 1 hour

# Chart configuration
CHART_COLORS: Final[dict] = {
    "pass": "#49B170",
    "fail": "#F3693F",
    "bin1": "#03234B",
    "bin2": "#3CB4E6",
    "warning": "#FFD200",
}

# Temperature color palette for CHAR (default)
DEFAULT_TEMP_PALETTE: Final[dict] = {
    "-40": "#03234B",
    "-10": "#3CB4E6",
    "30": "#49B170",
    "60": "#A4C238",
    "90": "#FFD200",
    "130": "#F3693F",
}

# Regex patterns
PRODUCT_CODE_PATTERN: Final[str] = r"^[A-F0-9]{3}$"
LOT_ID_PATTERN: Final[str] = r"^[A-Z0-9]{6,10}$"
WAFER_ID_PATTERN: Final[str] = r"^[A-Z0-9]{1,6}$"

# Data types for Polars optimization
OPTIMIZED_DTYPES: Final[dict] = {
    "ptr": {
        "PART_ID": "u32",
        "TEST_NUM": "u32",
        "HEAD_NUM": "u8",
        "SITE_NUM": "u8",
        "TEST_FLG": "u8",
        "PARM_FLG": "u16",
        "RESULT": "f32",
        "RES_SCAL": "i8",
        "LO_LIMIT": "f32",
        "HI_LIMIT": "f32",
    },
    "ftr": {
        "PART_ID": "u32",
        "TEST_NUM": "u32",
        "HEAD_NUM": "u8",
        "SITE_NUM": "u8",
        "TEST_FLG": "u8",
    },
    "prr": {
        "PartID": "u32",
        "HEAD_NUM": "u8",
        "SITE_NUM": "u8",
        "PART_FLG": "u8",
        "NUM_TEST": "u16",
        "HARD_BIN": "u16",
        "SOFT_BIN": "u16",
        "X_COORD": "i16",
        "Y_COORD": "i16",
        "TEST_T": "u32",
    },
}

# SVN configuration
SVN_BASE_URL: Final[str] = "svn://mcd-pe-svn.gnb.st.com/prj/ENGI_MCD_SVN/TPI_REPO/trunk"

# Network paths
NETWORK_SHARE: Final[str] = r"\\gpm-pe-data.gnb.st.com\ENGI_MCD_STDF"

# Status messages
STATUS_PROCESSING: Final[str] = "Processing"
STATUS_COMPLETED: Final[str] = "Completed"
STATUS_FAILED: Final[str] = "Failed"
STATUS_PENDING: Final[str] = "Pending"
STATUS_SKIPPED: Final[str] = "Skipped"
