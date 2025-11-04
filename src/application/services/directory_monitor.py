"""
ART.stdf - Directory Monitor Service

Service for monitoring and scanning directories for STDF files and related content.
Extracted and heavily refactored from original polling.py DirectoryPoller class.

Author: Matteo Terranova (matteo.terranova@st.com)
Organization: STMicroelectronics - MDRF GPAM
"""

import os
import logging
from pathlib import Path
from typing import List, Set, Tuple, Optional, Callable
from dataclasses import dataclass

from src.application.services.file_classifier import FileClassifier
from src.application.services.completion_tracker import CompletionTracker
from config.settings import settings


@dataclass
class ScanResult:
    """
    Result of directory scan.

    Attributes:
        stdf_files: List of STDF files found
        data_directories: List of directories ready for report generation
        condition_files: List of condition files found
        shmoo_directories: List of shmoo directories found
        char_directories: List of characterization directories found
    """

    stdf_files: List[str]
    data_directories: List[str]
    condition_files: List[str]
    shmoo_directories: List[str]
    char_directories: List[str]

    @property
    def total_items(self) -> int:
        """Get total number of items found."""
        return (
            len(self.stdf_files) +
            len(self.data_directories) +
            len(self.condition_files) +
            len(self.shmoo_directories) +
            len(self.char_directories)
        )


class DirectoryMonitor:
    """
    Service for monitoring directories and detecting files for processing.

    This service scans directory structures looking for:
    - STDF files that need conversion
    - Data directories ready for report generation
    - Condition files (anaflow HTML)
    - Shmoo directories with .shm files
    - Characterization datasets

    It respects the directory structure conventions and validates
    product/flow/package hierarchies.
    """

    def __init__(
        self,
        file_classifier: Optional[FileClassifier] = None,
        completion_tracker: Optional[CompletionTracker] = None,
        logger: Optional[logging.Logger] = None,
        progress_callback: Optional[Callable[[str, int, int], None]] = None
    ):
        """
        Initialize directory monitor.

        Args:
            file_classifier: File classification service
            completion_tracker: Completion tracking service
            logger: Logger instance
            progress_callback: Optional callback for progress updates
                               Signature: callback(product_name, current, total)
        """
        self.file_classifier = file_classifier or FileClassifier()
        self.completion_tracker = completion_tracker or CompletionTracker()
        self.logger = logger or logging.getLogger(__name__)
        self.progress_callback = progress_callback

        # Track seen files to avoid duplicates
        self._seen_files: Set[str] = set()

    def scan_directory(
        self,
        root_directory: str,
        product_filter: Optional[List[str]] = None
    ) -> ScanResult:
        """
        Scan directory for files and datasets to process.

        Args:
            root_directory: Root directory to scan
            product_filter: Optional list of product codes to include

        Returns:
            ScanResult with all found items

        Example:
            >>> monitor = DirectoryMonitor()
            >>> result = monitor.scan_directory("/data/STDF")
            >>> print(f"Found {result.total_items} items")
            >>> print(f"STDF files: {len(result.stdf_files)}")
        """
        stdf_files = set()
        data_dirs = set()
        condition_files = set()
        shmoo_dirs = set()
        char_dirs = set()

        root_path = Path(root_directory)

        if not root_path.exists():
            self.logger.error(f"Directory not found: {root_directory}")
            return ScanResult([], [], [], [], [])

        # Walk directory structure
        for root, dirs, files in os.walk(root_path):
            # Find matching product directories
            matching_products = self._find_matching_products(dirs, product_filter)

            if not matching_products:
                continue

            # Process each product
            total_products = len(matching_products)
            for index, product in enumerate(matching_products, start=1):
                # Progress callback
                if self.progress_callback:
                    self.progress_callback(product, index, total_products)

                product_path = Path(root) / product

                # Process this product directory
                self._process_product_directory(
                    product_path,
                    product,
                    stdf_files,
                    data_dirs,
                    condition_files,
                    shmoo_dirs,
                    char_dirs
                )

            # Don't recurse deeper (we handle structure ourselves)
            break

        return ScanResult(
            stdf_files=list(stdf_files),
            data_directories=list(data_dirs),
            condition_files=list(condition_files),
            shmoo_directories=list(shmoo_dirs),
            char_directories=list(char_dirs)
        )

    def _find_matching_products(
        self,
        dirs: List[str],
        product_filter: Optional[List[str]]
    ) -> List[str]:
        """Find product directories matching filter."""
        matching = [
            d for d in dirs
            if self.file_classifier.is_valid_product(d)
        ]

        if product_filter:
            matching = [d for d in matching if d in product_filter]

        return matching

    def _process_product_directory(
        self,
        product_path: Path,
        product: str,
        stdf_files: Set[str],
        data_dirs: Set[str],
        condition_files: Set[str],
        shmoo_dirs: Set[str],
        char_dirs: Set[str]
    ):
        """Process a single product directory."""
        if not product_path.is_dir():
            return

        # Find product cut directories
        for item in product_path.iterdir():
            if not item.is_dir():
                continue

            if self.file_classifier.is_valid_productcut(product, item.name):
                self._process_productcut_directory(
                    item,
                    stdf_files,
                    data_dirs,
                    condition_files,
                    shmoo_dirs,
                    char_dirs
                )

    def _process_productcut_directory(
        self,
        productcut_path: Path,
        stdf_files: Set[str],
        data_dirs: Set[str],
        condition_files: Set[str],
        shmoo_dirs: Set[str],
        char_dirs: Set[str]
    ):
        """Process a product cut directory."""
        # Look for flow directories
        for flow_item in productcut_path.iterdir():
            if not flow_item.is_dir():
                continue

            flow = flow_item.name

            if not self.file_classifier.is_valid_flow(flow):
                continue

            # Handle different flow types
            if flow == "EWSCHAR":
                self._process_ews_char_flow(
                    flow_item,
                    stdf_files,
                    data_dirs,
                    char_dirs
                )
            elif flow.startswith("EWS"):
                self._process_ews_flow(
                    flow_item,
                    stdf_files,
                    data_dirs,
                    condition_files,
                    shmoo_dirs
                )
            else:
                self._process_standard_flow(
                    flow_item,
                    stdf_files,
                    data_dirs,
                    condition_files,
                    shmoo_dirs
                )

    def _process_ews_char_flow(
        self,
        flow_path: Path,
        stdf_files: Set[str],
        data_dirs: Set[str],
        char_dirs: Set[str]
    ):
        """Process EWSCHAR flow directory."""
        # EWSCHAR has direct wafer subdirectories
        for wafer_dir in flow_path.iterdir():
            if not wafer_dir.is_dir():
                continue

            self._check_data_directory(
                wafer_dir,
                stdf_files,
                data_dirs,
                char_dirs
            )

    def _process_ews_flow(
        self,
        flow_path: Path,
        stdf_files: Set[str],
        data_dirs: Set[str],
        condition_files: Set[str],
        shmoo_dirs: Set[str]
    ):
        """Process EWS flow directory."""
        # Check for CONDITION subdirectory
        self._check_condition_directory(flow_path, condition_files)

        # Check for SHMOO subdirectory
        self._check_shmoo_directory(flow_path, shmoo_dirs)

        # Look for lot directories
        for lot_dir in flow_path.iterdir():
            if not lot_dir.is_dir():
                continue

            # Skip CONDITION and SHMOO directories
            if lot_dir.name in ["CONDITION", "SHMOO"]:
                continue

            # Look for wafer directories
            self._process_wafer_directories(lot_dir, stdf_files, data_dirs)

    def _process_standard_flow(
        self,
        flow_path: Path,
        stdf_files: Set[str],
        data_dirs: Set[str],
        condition_files: Set[str],
        shmoo_dirs: Set[str]
    ):
        """Process standard (non-EWS) flow directory."""
        # Check for CONDITION subdirectory
        self._check_condition_directory(flow_path, condition_files)

        # Check for SHMOO subdirectory
        self._check_shmoo_directory(flow_path, shmoo_dirs)

        # Look for package directories
        for package_dir in flow_path.iterdir():
            if not package_dir.is_dir():
                continue

            # Skip CONDITION and SHMOO directories
            if package_dir.name in ["CONDITION", "SHMOO"]:
                continue

            if self.file_classifier.is_valid_package(package_dir.name):
                # Look for badge directories
                for badge_dir in package_dir.iterdir():
                    if badge_dir.is_dir():
                        self._process_wafer_directories(badge_dir, stdf_files, data_dirs)

    def _process_wafer_directories(
        self,
        parent_path: Path,
        stdf_files: Set[str],
        data_dirs: Set[str]
    ):
        """Process LOOP/VOLUME wafer subdirectories."""
        for subfolder_name in ["LOOP", "VOLUME"]:
            subfolder = parent_path / subfolder_name

            if subfolder.is_dir():
                self._check_data_directory(
                    subfolder,
                    stdf_files,
                    data_dirs,
                    None
                )

    def _check_condition_directory(
        self,
        flow_path: Path,
        condition_files: Set[str]
    ):
        """Check for condition files in CONDITION subdirectory."""
        condition_dir = flow_path / "CONDITION"

        if not condition_dir.is_dir():
            return

        # Check if already processed
        if self.completion_tracker.is_complete(str(condition_dir)):
            return

        # Find condition files
        found_files = self.file_classifier.find_condition_files(str(flow_path))

        for file in found_files:
            file_str = str(file)
            if file_str not in self._seen_files:
                self.logger.info(f"[Monitor] New CONDITION: {file.name}")
                condition_files.add(file_str)
                self._seen_files.add(file_str)

    def _check_shmoo_directory(
        self,
        flow_path: Path,
        shmoo_dirs: Set[str]
    ):
        """Check for .shm files in SHMOO subdirectory."""
        shmoo_dir = flow_path / "SHMOO"

        if not shmoo_dir.is_dir():
            return

        # Find .shm files
        shm_files = self.file_classifier.find_shmoo_files(str(flow_path))

        if shm_files:
            shmoo_dir_str = str(shmoo_dir)
            if shmoo_dir_str not in self._seen_files:
                self.logger.info(f"[Monitor] New SHMOO: {len(shm_files)} files")
                shmoo_dirs.add(shmoo_dir_str)
                self._seen_files.add(shmoo_dir_str)

    def _check_data_directory(
        self,
        data_path: Path,
        stdf_files: Set[str],
        data_dirs: Set[str],
        char_dirs: Optional[Set[str]]
    ):
        """Check a data directory for STDF files and readiness."""
        if not data_path.is_dir():
            return

        # Check if already processed
        if self.completion_tracker.is_complete(str(data_path)):
            return

        # Find STDF files
        found_stdf = self.file_classifier.find_stdf_files(str(data_path))

        if not found_stdf:
            return

        # Single STDF file (normal case)
        if len(found_stdf) == 1 and "CHAR" not in str(data_path):
            stdf_file = found_stdf[0]
            stdf_str = str(stdf_file)

            # Check if needs conversion
            if self.completion_tracker.needs_conversion(stdf_str):
                if stdf_str not in self._seen_files:
                    self.logger.info(f"[Monitor] New STDF: {stdf_file.name}")
                    stdf_files.add(stdf_str)
                    self._seen_files.add(stdf_str)

            # Check if ready for report generation
            parquet_count = self.file_classifier.count_parquet_files(stdf_str)
            if parquet_count >= 8:
                data_str = str(data_path)
                if self.completion_tracker.needs_report_generation(data_str):
                    data_dirs.add(data_str)

        # Multiple STDF files (CHAR case)
        else:
            # Check each STDF file
            all_ready = True
            for stdf_file in found_stdf:
                stdf_str = str(stdf_file)

                if self.completion_tracker.needs_conversion(stdf_str):
                    all_ready = False
                    if stdf_str not in self._seen_files:
                        self.logger.info(f"[Monitor] New STDF: {stdf_file.name}")
                        stdf_files.add(stdf_str)
                        self._seen_files.add(stdf_str)

            # If all ready and is CHAR, add to char dirs
            if all_ready and char_dirs is not None:
                parent_str = str(data_path.parent)
                if self.completion_tracker.needs_report_generation(parent_str):
                    char_dirs.add(parent_str)

    def clear_seen_files(self):
        """
        Clear the seen files cache.

        Use this to force re-detection of files.

        Example:
            >>> monitor = DirectoryMonitor()
            >>> monitor.clear_seen_files()  # Force re-scan
        """
        self._seen_files.clear()

    def get_seen_count(self) -> int:
        """
        Get count of files that have been seen.

        Returns:
            Number of unique files seen

        Example:
            >>> monitor = DirectoryMonitor()
            >>> monitor.scan_directory("/data")
            >>> print(f"Seen {monitor.get_seen_count()} files")
        """
        return len(self._seen_files)
