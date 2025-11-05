"""
ART.stdf - File Repository

File operations repository for managing STDF files, reports, and completion markers.
Extracted and refactored from original polling.py module.

Author: Matteo Terranova (matteo.terranova@st.com)
Organization: STMicroelectronics - MDRF GPAM
"""

import os
from pathlib import Path
from typing import Dict, List

from config.settings import settings


class FileRepository:
    """
    Repository for file operations.

    This class handles file system operations including:
    - Completion marker management
    - Report path generation
    - Directory management
    """

    @staticmethod
    def check_completion_marker(path: str, marker_name: str = None) -> bool:
        """
        Check if completion marker file exists in a directory.

        Args:
            path: Directory path to check
            marker_name: Name of marker file (default: from settings)

        Returns:
            True if marker exists, False otherwise

        Example:
            >>> FileRepository.check_completion_marker("/path/to/dir")
            False
        """
        if marker_name is None:
            marker_name = settings.REPORT_COMPLETION_MARKER

        marker_path = Path(path) / marker_name
        return marker_path.is_file()

    @staticmethod
    def create_completion_marker(
        path: str,
        marker_name: str = None,
        content: str = None,
        file_count: int = None
    ) -> None:
        """
        Create completion marker file in a directory.

        Args:
            path: Directory path where marker will be created
            marker_name: Name of marker file (default: from settings)
            content: Content to write to marker (default: from settings)
            file_count: Optional file count to include in marker content

        Example:
            >>> FileRepository.create_completion_marker("/path/to/dir")
            >>> FileRepository.create_completion_marker("/path/to/dir", file_count=5)
        """
        if marker_name is None:
            marker_name = settings.REPORT_COMPLETION_MARKER

        if content is None:
            if file_count is not None:
                content = f"Completed processing {file_count} files"
            else:
                content = settings.COMPLETION_MARKER_CONTENT

        # Ensure directory exists
        directory = Path(path)
        directory.mkdir(parents=True, exist_ok=True)

        # Write marker file
        marker_path = directory / marker_name
        marker_path.write_text(content, encoding='utf-8')

    @staticmethod
    def remove_completion_marker(
        path: str,
        marker_name: str = None
    ) -> None:
        """
        Remove completion marker file from a directory.

        Args:
            path: Directory path where marker exists
            marker_name: Name of marker file (default: from settings)

        Example:
            >>> FileRepository.remove_completion_marker("/path/to/dir")
        """
        if marker_name is None:
            marker_name = settings.REPORT_COMPLETION_MARKER

        marker_path = Path(path) / marker_name
        if marker_path.exists():
            marker_path.unlink()

    @staticmethod
    def get_report_directory(
        base_path: str,
        parameter: Dict,
        report_type: str
    ) -> Path:
        """
        Generate report directory path based on parameters and report type.

        Args:
            base_path: Base directory path
            parameter: Parameter dictionary with metadata
            report_type: Type of report (DATA2REPORT, CONDITION2REPORT, CHAR)

        Returns:
            Path object for report directory

        Example:
            >>> path = FileRepository.get_report_directory(
            ...     "/base/path",
            ...     {"TYPE": "VOLUME", "TITLE": "Test Report"},
            ...     "DATA2REPORT"
            ... )
        """
        base_path = Path(base_path)

        if report_type == "DATA2REPORT":
            report_base = base_path.parent / "Report"

            # TTIME and YIELD go directly in Report folder
            if "TTIME" in parameter.get("COM", "") or "YIELD" in parameter.get("COM", ""):
                return report_base

            # Other reports go in TYPE subdirectory
            return report_base / parameter.get("TYPE", "").upper()

        elif report_type == "CHAR":
            # CHAR reports use special path structure
            report_base = base_path / "Report"
            flow = parameter.get("FLOW", "")

            if flow in str(report_base):
                parts = str(report_base).split(flow)
                return Path(parts[0]) / flow / "Report"

            return report_base

        elif report_type == "CONDITION2REPORT":
            # Condition reports go in Report folder relative to condition file
            return base_path.parent / "Report"

        return base_path / "Report"

    @staticmethod
    def get_report_path(
        base_path: str,
        parameter: Dict,
        report_type: str
    ) -> Path:
        """
        Generate full report file path based on parameters and report type.

        Args:
            base_path: Base directory path
            parameter: Parameter dictionary with metadata
            report_type: Type of report

        Returns:
            Path object for report file

        Example:
            >>> path = FileRepository.get_report_path(
            ...     "/base/path",
            ...     {"TITLE": "My Report"},
            ...     "DATA2REPORT"
            ... )
        """
        report_dir = FileRepository.get_report_directory(
            base_path,
            parameter,
            report_type
        )

        # CHAR reports return directory, not file
        if report_type == "CHAR":
            return report_dir

        # Other reports return HTML file path
        report_filename = parameter.get("TITLE", "report") + ".html"
        return report_dir / report_filename

    @staticmethod
    def ensure_report_directory(
        base_path: str,
        parameter: Dict,
        report_type: str
    ) -> Path:
        """
        Ensure report directory exists, creating it if necessary.

        Args:
            base_path: Base directory path
            parameter: Parameter dictionary with metadata
            report_type: Type of report

        Returns:
            Path object for report directory

        Example:
            >>> path = FileRepository.ensure_report_directory(
            ...     "/base/path",
            ...     {"TYPE": "VOLUME"},
            ...     "DATA2REPORT"
            ... )
        """
        report_dir = FileRepository.get_report_directory(
            base_path,
            parameter,
            report_type
        )

        report_dir.mkdir(parents=True, exist_ok=True)
        return report_dir

    @staticmethod
    def get_parquet_directory(base_path: str) -> Path:
        """
        Get parquet directory path for a given base path.

        Args:
            base_path: Base directory path

        Returns:
            Path to parquet directory

        Example:
            >>> path = FileRepository.get_parquet_directory("/stdf/file/path")
            >>> print(path)
            /stdf/file/path/parquet
        """
        return Path(base_path) / settings.PARQUET_FOLDER_NAME

    @staticmethod
    def ensure_parquet_directory(base_path: str) -> Path:
        """
        Ensure parquet directory exists, creating it if necessary.

        Args:
            base_path: Base directory path

        Returns:
            Path to parquet directory

        Example:
            >>> path = FileRepository.ensure_parquet_directory("/stdf/file/path")
        """
        parquet_dir = FileRepository.get_parquet_directory(base_path)
        parquet_dir.mkdir(parents=True, exist_ok=True)
        return parquet_dir

    @staticmethod
    def find_files(path: str, pattern: str = "*") -> List[Path]:
        """
        Find files in a directory matching a pattern.

        Args:
            path: Directory path to search
            pattern: Glob pattern (default: "*")

        Returns:
            List of Path objects matching the pattern

        Example:
            >>> files = FileRepository.find_files("/path/to/dir", "*.std")
            >>> files = FileRepository.find_files("/path/to/dir", "**/*.std")  # recursive
        """
        directory = Path(path)
        if not directory.exists():
            return []

        # Check if pattern is recursive (contains **)
        if "**" in pattern:
            # Use rglob for recursive search
            pattern_without_stars = pattern.replace("**/", "")
            return sorted(directory.rglob(pattern_without_stars))
        else:
            # Use glob for non-recursive search
            return sorted(directory.glob(pattern))

    @staticmethod
    def ensure_directory(path: str) -> Path:
        """
        Ensure a directory exists, creating it if necessary.

        Args:
            path: Directory path to ensure exists

        Returns:
            Path object for the directory

        Example:
            >>> path = FileRepository.ensure_directory("/path/to/new/dir")
        """
        directory = Path(path)
        directory.mkdir(parents=True, exist_ok=True)
        return directory
