"""
ART.stdf - Pytest Configuration and Fixtures

Global test configuration and shared fixtures for pytest.

Author: Matteo Terranova (matteo.terranova@st.com)
Organization: STMicroelectronics - MDRF GPAM
"""

import pytest
import tempfile
import shutil
from pathlib import Path
from typing import Generator


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
def sample_parameter():
    """
    Sample parameter dictionary for testing.

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


# Add custom markers
def pytest_configure(config):
    """Configure custom markers."""
    config.addinivalue_line("markers", "unit: Unit tests")
    config.addinivalue_line("markers", "integration: Integration tests")
    config.addinivalue_line("markers", "slow: Slow running tests")
    config.addinivalue_line("markers", "network: Tests requiring network access")
    config.addinivalue_line("markers", "stdf: Tests involving STDF parsing")
