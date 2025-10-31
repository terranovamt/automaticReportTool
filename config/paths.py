"""
Path configuration and management for ART.stdf

This module provides centralized path management with validation.
"""

import os
from pathlib import Path
from typing import Optional
from dataclasses import dataclass


@dataclass
class PathConfig:
    """
    Centralized path configuration for ART.stdf.

    All paths are resolved to absolute paths for consistency.
    """

    # Root paths
    project_root: Path
    src_root: Path
    data_root: Optional[Path] = None

    # Directory paths
    log_dir: Path = None
    temp_dir: Path = None
    output_dir: Path = None

    # STDF watch directory (network share)
    watch_path: Optional[Path] = None

    def __post_init__(self):
        """Initialize and validate paths."""
        # Convert to Path objects and resolve
        self.project_root = Path(self.project_root).resolve()
        self.src_root = Path(self.src_root).resolve()

        # Set default paths if not provided
        if self.log_dir is None:
            self.log_dir = self.project_root / "log"

        if self.temp_dir is None:
            self.temp_dir = self.src_root / "jupiter" / "tmp"

        if self.output_dir is None:
            self.output_dir = self.project_root / "output"

        # Resolve all paths
        self.log_dir = Path(self.log_dir).resolve()
        self.temp_dir = Path(self.temp_dir).resolve()
        self.output_dir = Path(self.output_dir).resolve()

        if self.data_root:
            self.data_root = Path(self.data_root).resolve()

        if self.watch_path:
            self.watch_path = Path(self.watch_path).resolve()

        # Create necessary directories
        self._create_directories()

    def _create_directories(self):
        """Create necessary directories if they don't exist."""
        for directory in [self.log_dir, self.temp_dir, self.output_dir]:
            directory.mkdir(parents=True, exist_ok=True)

    # Convenience methods for common path operations

    def get_parquet_dir(self, stdf_path: Path) -> Path:
        """
        Get Parquet output directory for a given STDF file.

        Args:
            stdf_path: Path to STDF file

        Returns:
            Path to parquet directory
        """
        parent = stdf_path.parent
        return parent / "parquet"

    def get_report_dir(self, stdf_path: Path) -> Path:
        """
        Get report output directory for a given STDF file.

        Args:
            stdf_path: Path to STDF file

        Returns:
            Path to report directory
        """
        parent = stdf_path.parent
        return parent / "REPORT"

    def get_condition_dir(self, stdf_path: Path) -> Path:
        """
        Get condition output directory for a given STDF file.

        Args:
            stdf_path: Path to STDF file

        Returns:
            Path to condition directory
        """
        parent = stdf_path.parent
        return parent / "CONDITION"

    def get_char_dir(self, base_path: Path) -> Path:
        """
        Get characterization output directory.

        Args:
            base_path: Base path for CHAR data

        Returns:
            Path to CHAR directory
        """
        return base_path / "CHAR"

    def validate(self) -> bool:
        """
        Validate path configuration.

        Returns:
            True if valid

        Raises:
            ValueError: If paths are invalid
        """
        # Check project root exists
        if not self.project_root.exists():
            raise ValueError(f"Project root does not exist: {self.project_root}")

        # Check src root exists
        if not self.src_root.exists():
            raise ValueError(f"Source root does not exist: {self.src_root}")

        # Check watch path if specified
        if self.watch_path and not self.watch_path.exists():
            raise ValueError(f"Watch path does not exist: {self.watch_path}")

        return True

    def __repr__(self) -> str:
        """String representation."""
        return (
            f"PathConfig(\n"
            f"  project_root={self.project_root},\n"
            f"  src_root={self.src_root},\n"
            f"  log_dir={self.log_dir},\n"
            f"  watch_path={self.watch_path}\n"
            f")"
        )


def get_default_paths() -> PathConfig:
    """
    Get default path configuration for ART.stdf.

    Returns:
        PathConfig with default paths
    """
    # Determine project root (2 levels up from this file)
    current_file = Path(__file__).resolve()
    project_root = current_file.parent.parent

    return PathConfig(
        project_root=project_root,
        src_root=project_root / "src",
        watch_path=Path(os.getenv(
            "ART_WATCH_PATH",
            "//gpm-pe-data.gnb.st.com/ENGI_MCD_STDF"
        )) if os.getenv("ART_WATCH_PATH") else None
    )
