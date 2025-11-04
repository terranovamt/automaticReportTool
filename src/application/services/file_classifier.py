"""
ART.stdf - File Classifier Service

Service for classifying and validating different file types.
Extracted from original polling.py logic.

Author: Matteo Terranova (matteo.terranova@st.com)
Organization: STMicroelectronics - MDRF GPAM
"""

import os
import re
from pathlib import Path
from typing import Optional, List, Tuple
from dataclasses import dataclass

from config.settings import settings


@dataclass
class FileClassification:
    """
    Result of file classification.

    Attributes:
        file_type: Type of file (STDF, CONDITION, SHMOO, etc.)
        is_valid: Whether file passes validation
        path: Full path to file
        reason: Reason for validation result
    """

    file_type: str
    is_valid: bool
    path: Path
    reason: str = ""


class FileClassifier:
    """
    Service for classifying and validating files in the STDF processing system.

    This service handles:
    - File type detection (STDF, condition, shmoo, etc.)
    - Path validation
    - Product/flow/package validation
    - File naming pattern matching
    """

    # Regex patterns
    PRODUCT_REGEX = re.compile(r"^[A-F0-9]{3}$")
    PRODUCTCUT_REGEX_TEMPLATE = r"^{product}[A-Z]$"
    LOT_WAFER_REGEX_TEMPLATE = r"^{lot}_([0][1-9]|1[0-9]|2[0-5])$"
    STDF_PATTERN = re.compile(
        r".*\.(std|stdf|STDF)(\.(gz|7z|zip|bz2|xz|tar|rar))?$"
    )

    def __init__(self):
        """Initialize file classifier."""
        self.allowed_flows = settings.ALLOWED_FLOWS
        self.allowed_packages = settings.ALLOWED_PACKAGES

    def classify_file(self, file_path: str) -> FileClassification:
        """
        Classify a file and determine its type.

        Args:
            file_path: Path to file to classify

        Returns:
            FileClassification result

        Example:
            >>> classifier = FileClassifier()
            >>> result = classifier.classify_file("/path/to/file.std.gz")
            >>> print(result.file_type)  # "STDF"
        """
        path = Path(file_path)

        if not path.exists():
            return FileClassification(
                file_type="UNKNOWN",
                is_valid=False,
                path=path,
                reason="File does not exist"
            )

        # Check if it's a directory
        if path.is_dir():
            return self._classify_directory(path)

        # Check file extensions
        filename = path.name.lower()

        if self._is_stdf_file(filename):
            return FileClassification(
                file_type="STDF",
                is_valid=True,
                path=path,
                reason="Valid STDF file"
            )

        if filename.startswith("anaflow") and filename.endswith(".html"):
            return FileClassification(
                file_type="CONDITION",
                is_valid=True,
                path=path,
                reason="Valid condition file"
            )

        if filename.endswith(".shm"):
            return FileClassification(
                file_type="SHMOO",
                is_valid=True,
                path=path,
                reason="Valid shmoo file"
            )

        return FileClassification(
            file_type="UNKNOWN",
            is_valid=False,
            path=path,
            reason="Unknown file type"
        )

    def _classify_directory(self, dir_path: Path) -> FileClassification:
        """Classify a directory (SHMOO, CONDITION, etc.)."""
        dir_name = dir_path.name

        if dir_name == "CONDITION":
            return FileClassification(
                file_type="CONDITION_DIR",
                is_valid=True,
                path=dir_path,
                reason="Condition directory"
            )

        if dir_name == "SHMOO":
            return FileClassification(
                file_type="SHMOO_DIR",
                is_valid=True,
                path=dir_path,
                reason="Shmoo directory"
            )

        if dir_name in ["LOOP", "VOLUME"]:
            return FileClassification(
                file_type="DATA_DIR",
                is_valid=True,
                path=dir_path,
                reason=f"{dir_name} data directory"
            )

        return FileClassification(
            file_type="DIRECTORY",
            is_valid=False,
            path=dir_path,
            reason="Unknown directory type"
        )

    def _is_stdf_file(self, filename: str) -> bool:
        """Check if filename matches STDF pattern."""
        return bool(self.STDF_PATTERN.match(filename))

    def is_valid_product(self, product: str) -> bool:
        """
        Validate product code format.

        Args:
            product: Product code (e.g., "44E")

        Returns:
            True if valid 3-character hex code

        Example:
            >>> classifier = FileClassifier()
            >>> classifier.is_valid_product("44E")
            True
            >>> classifier.is_valid_product("XYZ")
            False
        """
        return bool(self.PRODUCT_REGEX.match(product))

    def is_valid_productcut(self, product: str, productcut: str) -> bool:
        """
        Validate product cut format.

        Args:
            product: Product code (e.g., "44E")
            productcut: Product cut (e.g., "44EA")

        Returns:
            True if valid product + single letter

        Example:
            >>> classifier = FileClassifier()
            >>> classifier.is_valid_productcut("44E", "44EA")
            True
        """
        pattern = re.compile(self.PRODUCTCUT_REGEX_TEMPLATE.format(product=product))
        return bool(pattern.match(productcut))

    def is_valid_flow(self, flow: str) -> bool:
        """
        Validate flow type.

        Args:
            flow: Flow name (e.g., "EWS1", "FT")

        Returns:
            True if flow is in allowed list

        Example:
            >>> classifier = FileClassifier()
            >>> classifier.is_valid_flow("EWS1")
            True
            >>> classifier.is_valid_flow("INVALID")
            False
        """
        return flow.upper() in self.allowed_flows

    def is_valid_package(self, package: str) -> bool:
        """
        Validate package type.

        Args:
            package: Package name (e.g., "QFP", "BGA")

        Returns:
            True if package contains valid type

        Example:
            >>> classifier = FileClassifier()
            >>> classifier.is_valid_package("QFP48")
            True
        """
        package_upper = package.upper()
        return any(pkg in package_upper for pkg in self.allowed_packages)

    def is_valid_lot_wafer(self, lot: str, wafer: str) -> bool:
        """
        Validate lot-wafer combination.

        Args:
            lot: Lot identifier
            wafer: Wafer identifier (must be 01-25)

        Returns:
            True if valid format

        Example:
            >>> classifier = FileClassifier()
            >>> classifier.is_valid_lot_wafer("Q123456", "Q123456_05")
            True
        """
        pattern = re.compile(self.LOT_WAFER_REGEX_TEMPLATE.format(lot=lot))
        return bool(pattern.match(wafer))

    def find_stdf_files(self, directory: str) -> List[Path]:
        """
        Find all STDF files in a directory.

        Args:
            directory: Directory path to search

        Returns:
            List of STDF file paths

        Example:
            >>> classifier = FileClassifier()
            >>> files = classifier.find_stdf_files("/path/to/data")
            >>> print(len(files))
        """
        dir_path = Path(directory)

        if not dir_path.is_dir():
            return []

        stdf_files = []
        for file in dir_path.iterdir():
            if file.is_file() and self._is_stdf_file(file.name.lower()):
                stdf_files.append(file)

        return stdf_files

    def find_condition_files(self, directory: str) -> List[Path]:
        """
        Find condition files (anaflow*.html) in CONDITION subdirectory.

        Args:
            directory: Flow directory path

        Returns:
            List of condition file paths

        Example:
            >>> classifier = FileClassifier()
            >>> files = classifier.find_condition_files("/path/to/EWS1")
        """
        condition_dir = Path(directory) / "CONDITION"

        if not condition_dir.is_dir():
            return []

        condition_files = []
        for file in condition_dir.iterdir():
            filename = file.name.lower()
            if (
                file.is_file()
                and filename.startswith("anaflow")
                and filename.endswith(".html")
            ):
                condition_files.append(file)

        return condition_files

    def find_shmoo_files(self, directory: str) -> List[Path]:
        """
        Find .shm files in SHMOO subdirectory.

        Args:
            directory: Flow directory path

        Returns:
            List of shmoo file paths

        Example:
            >>> classifier = FileClassifier()
            >>> files = classifier.find_shmoo_files("/path/to/EWS1")
        """
        shmoo_dir = Path(directory) / "SHMOO"

        if not shmoo_dir.is_dir():
            return []

        return list(shmoo_dir.glob("*.shm"))

    def get_parquet_files(self, stdf_path: str) -> List[Path]:
        """
        Get parquet files for an STDF file.

        Args:
            stdf_path: Path to STDF file

        Returns:
            List of parquet file paths

        Example:
            >>> classifier = FileClassifier()
            >>> parquets = classifier.get_parquet_files("/path/file.std.gz")
        """
        stdf_path = Path(stdf_path)
        parquet_dir = stdf_path.parent / "parquet"

        if not parquet_dir.is_dir():
            return []

        # Look for parquet files matching the STDF filename
        pattern = f"{stdf_path.name}.*.parquet"
        return list(parquet_dir.glob(pattern))

    def count_parquet_files(self, stdf_path: str) -> int:
        """
        Count parquet files for an STDF file.

        Args:
            stdf_path: Path to STDF file

        Returns:
            Number of parquet files

        Example:
            >>> classifier = FileClassifier()
            >>> count = classifier.count_parquet_files("/path/file.std")
            >>> if count >= 8:
            ...     print("Conversion complete")
        """
        return len(self.get_parquet_files(stdf_path))
