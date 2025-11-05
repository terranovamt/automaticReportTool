"""
ART.stdf - Pytest Configuration and Fixtures

Global test configuration and shared fixtures for pytest.

Author: Matteo Terranova (matteo.terranova@st.com)
Organization: STMicroelectronics - MDRF GPAM
"""

import pytest
import tempfile
import shutil
import logging
from pathlib import Path
from typing import Generator, Dict
import polars as pl

from src.domain.models.parameter import Parameter, FileCorner


# =============================================================================
# File System Fixtures
# =============================================================================

@pytest.fixture
def temp_dir() -> Generator[Path, None, None]:
    """
    Create a temporary directory for test files.

    Yields:
        Path to temporary directory (automatically cleaned up)
    """
    temp_path = Path(tempfile.mkdtemp())
    try:
        yield temp_path
    finally:
        shutil.rmtree(temp_path, ignore_errors=True)


@pytest.fixture
def mock_stdf_file(temp_dir: Path) -> Path:
    """
    Create a mock STDF file for testing.

    Args:
        temp_dir: Temporary directory fixture

    Returns:
        Path to mock STDF file
    """
    stdf_file = temp_dir / "test.std"
    stdf_file.write_bytes(b"MOCK_STDF_DATA")
    return stdf_file


@pytest.fixture
def mock_parquet_dir(temp_dir: Path) -> Path:
    """
    Create a mock parquet directory structure.

    Args:
        temp_dir: Temporary directory fixture

    Returns:
        Path to parquet directory
    """
    parquet_dir = temp_dir / "parquet"
    parquet_dir.mkdir()
    return parquet_dir


# =============================================================================
# Domain Model Fixtures
# =============================================================================

@pytest.fixture
def sample_parameter():
    """
    Sample parameter dictionary for testing (legacy format).

    Returns:
        Dictionary with sample parameters
    """
    return {
        "TITLE": "Test Report",
        "COM": "TEST_COMPOSITE",
        "FLOW": "EWS1",
        "TYPE": "VOLUME",
        "PRODUCT": "Test Product",
        "CODE": "44E",
        "LOT": "Q123456",
        "WAFER": "01",
        "CUT": "44EA",
        "REVISION": "0.1",
        "FILE": {
            "01": {
                "corner": "TTTT",
                "path": "/path/to/test.std",
            }
        },
        "AUTHOR": "Matteo Terranova",
        "MAIL": "matteo.terranova@st.com",
        "SITE": "Catania",
        "GROUP": "MDRF - EP - GPAM",
        "TEST_NUM": [],
        "MAIN": "/path/to/main",
    }


@pytest.fixture
def sample_parameter_object() -> Parameter:
    """
    Sample Parameter object for testing (new format).

    Returns:
        Parameter dataclass instance
    """
    return Parameter(
        code="44E",
        cut="44EZ",
        flow="EWSCHAR",
        type="CHAR",
        lot="Q445172",
        wafer="05",
        file={
            "05": FileCorner(
                corner="TTTT",
                path="./STDF/44E/44EZ/EWSCHAR/Q445172_05_TTTT"
            )
        }
    )


# =============================================================================
# DataFrame Fixtures
# =============================================================================

@pytest.fixture
def sample_ptr_dataframe() -> pl.DataFrame:
    """Create a sample PTR (Parametric Test Record) DataFrame."""
    return pl.DataFrame({
        "TestName": ["TEST1:param", "TEST1:param", "TEST2:value"],
        "TestNumber": [1, 1, 2],
        "Value": [1.0, 1.1, 2.0],
        "Corner": ["TTTT", "SSTT", "TTTT"],
        "°C": ["30", "30", "30"],
        "Low Limit": [0.8, 0.8, 1.8],
        "High Limit": [1.2, 1.2, 2.2],
        "Unit": ["V", "V", "A"],
        "Split": ["Standard", "Standard", "Standard"],
        "X_COORD": [10, 11, 10],
        "Y_COORD": [20, 20, 21],
        "pltype": ["STD", "STD", "STD"],
    })


@pytest.fixture
def sample_ftr_dataframe() -> pl.DataFrame:
    """Create a sample FTR (Functional Test Record) DataFrame."""
    return pl.DataFrame({
        "TestName": ["FUNC1", "FUNC1", "FUNC2"],
        "TestNumber": [100, 100, 101],
        "RESULT": [1, 1, 0],
        "Corner": ["TTTT", "SSTT", "TTTT"],
        "°C": ["30", "30", "30"],
        "Split": ["Standard", "Standard", "Standard"],
        "pltype": ["STD", "STD", "STD"],
    })


@pytest.fixture
def sample_mir_dataframe() -> pl.DataFrame:
    """Create a sample MIR (Master Information Record) DataFrame."""
    return pl.DataFrame({
        "FAMLY_ID": ["STM32F4"],
        "SETUP_T": [1234567890],
        "START_T": [1234567900],
        "NODE_NAM": ["TESTER1"],
    })


@pytest.fixture
def sample_df_stdf() -> Dict[str, pl.DataFrame]:
    """
    Create a complete sample df_stdf dictionary.

    Returns:
        Dictionary with all STDF record types as DataFrames
    """
    return {
        "ptr": pl.DataFrame({
            "TestName": ["TEST1", "TEST2"],
            "Value": [1.0, 2.0],
            "Corner": ["TTTT", "TTTT"],
            "°C": ["30", "30"],
        }),
        "ftr": pl.DataFrame({
            "TestName": ["FUNC1"],
            "RESULT": [1],
        }),
        "mir": pl.DataFrame({
            "FAMLY_ID": ["STM32"],
        }),
        "prr": pl.DataFrame(),
        "pcr": pl.DataFrame(),
        "hbr": pl.DataFrame(),
        "sbr": pl.DataFrame(),
    }


# =============================================================================
# Logging Fixtures
# =============================================================================

@pytest.fixture
def test_logger() -> logging.Logger:
    """
    Create a test logger that doesn't produce output.

    Returns:
        Configured logger for testing
    """
    logger = logging.getLogger("test")
    logger.setLevel(logging.DEBUG)
    logger.addHandler(logging.NullHandler())
    return logger


# =============================================================================
# Mock Fixtures
# =============================================================================

@pytest.fixture
def mock_file_repository(mocker):
    """Create a mock FileRepository for testing."""
    from src.infrastructure.storage.file_repository import FileRepository

    mock = mocker.Mock(spec=FileRepository)
    mock.check_completion_marker.return_value = False
    mock.create_completion_marker.return_value = None
    mock.find_files.return_value = []

    return mock


@pytest.fixture
def mock_stdf_parser(mocker):
    """Create a mock STDFParser for testing."""
    from src.infrastructure.parsers.stdf_parser import STDFParser

    mock = mocker.Mock(spec=STDFParser)
    mock.parse_to_parquet.return_value = {
        "ptr": Path("test.ptr.parquet"),
        "ftr": Path("test.ftr.parquet"),
        "mir": Path("test.mir.parquet"),
    }

    return mock


# =============================================================================
# Pytest Configuration
# =============================================================================

def pytest_configure(config):
    """Configure custom markers."""
    config.addinivalue_line("markers", "unit: Unit tests")
    config.addinivalue_line("markers", "integration: Integration tests")
    config.addinivalue_line("markers", "slow: Slow running tests")
    config.addinivalue_line("markers", "network: Tests requiring network access")
    config.addinivalue_line("markers", "stdf: Tests involving STDF parsing")
    config.addinivalue_line("markers", "requires_data: Tests requiring external data files")
