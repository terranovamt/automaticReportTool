"""
ART.stdf - Completion Tracker Service

Service for tracking processing completion status.
Extracted from original polling.py logic.

Author: Matteo Terranova (matteo.terranova@st.com)
Organization: STMicroelectronics - MDRF GPAM
"""

from pathlib import Path
from typing import Dict, Set, Optional
from dataclasses import dataclass, field
from datetime import datetime

from src.infrastructure.storage.file_repository import FileRepository


@dataclass
class ProcessingStatus:
    """
    Status of processing for a path.

    Attributes:
        path: Path being processed
        is_complete: Whether processing is complete
        started_at: When processing started
        completed_at: When processing completed
        file_count: Number of files processed
        error: Error message if failed
    """

    path: Path
    is_complete: bool = False
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    file_count: int = 0
    error: Optional[str] = None


class CompletionTracker:
    """
    Service for tracking processing completion across the system.

    This service maintains state about what has been processed,
    what's in progress, and what failed.

    Responsibilities:
    - Check if path has been processed
    - Mark paths as complete
    - Track processing history
    - Prevent duplicate processing
    """

    def __init__(self, file_repository: Optional[FileRepository] = None):
        """
        Initialize completion tracker.

        Args:
            file_repository: File repository for marker operations
        """
        self.file_repository = file_repository or FileRepository()
        self._in_progress: Set[str] = set()
        self._status_cache: Dict[str, ProcessingStatus] = {}

    def is_complete(self, path: str, marker_name: Optional[str] = None) -> bool:
        """
        Check if processing is complete for a path.

        Args:
            path: Path to check
            marker_name: Optional marker file name

        Returns:
            True if processing is complete

        Example:
            >>> tracker = CompletionTracker()
            >>> if not tracker.is_complete("/path/to/data"):
            ...     process_data()
            ...     tracker.mark_complete("/path/to/data")
        """
        # Check cache first
        if path in self._status_cache:
            return self._status_cache[path].is_complete

        # Check for completion marker
        is_complete = self.file_repository.check_completion_marker(path, marker_name)

        # Update cache
        self._status_cache[path] = ProcessingStatus(
            path=Path(path),
            is_complete=is_complete
        )

        return is_complete

    def mark_complete(
        self,
        path: str,
        file_count: int = 0,
        marker_name: Optional[str] = None,
        marker_content: Optional[str] = None
    ) -> None:
        """
        Mark a path as completely processed.

        Args:
            path: Path that was processed
            file_count: Number of files processed
            marker_name: Optional marker file name
            marker_content: Optional marker content

        Example:
            >>> tracker = CompletionTracker()
            >>> tracker.mark_complete("/path/to/data", file_count=5)
        """
        # Create completion marker
        self.file_repository.create_completion_marker(
            path,
            marker_name,
            marker_content
        )

        # Update cache
        status = self._status_cache.get(path)
        if status:
            status.is_complete = True
            status.completed_at = datetime.now()
            status.file_count = file_count
        else:
            self._status_cache[path] = ProcessingStatus(
                path=Path(path),
                is_complete=True,
                completed_at=datetime.now(),
                file_count=file_count
            )

        # Remove from in-progress
        self._in_progress.discard(path)

    def mark_in_progress(self, path: str) -> bool:
        """
        Mark a path as being processed.

        Args:
            path: Path being processed

        Returns:
            True if marked successfully, False if already in progress

        Example:
            >>> tracker = CompletionTracker()
            >>> if tracker.mark_in_progress("/path/to/data"):
            ...     process_data()
            ...     tracker.mark_complete("/path/to/data")
        """
        if path in self._in_progress:
            return False

        self._in_progress.add(path)

        # Update cache
        status = self._status_cache.get(path)
        if status:
            status.started_at = datetime.now()
        else:
            self._status_cache[path] = ProcessingStatus(
                path=Path(path),
                started_at=datetime.now()
            )

        return True

    def mark_failed(self, path: str, error: str) -> None:
        """
        Mark a path as failed processing.

        Args:
            path: Path that failed
            error: Error message

        Example:
            >>> tracker = CompletionTracker()
            >>> try:
            ...     process_data()
            ... except Exception as e:
            ...     tracker.mark_failed("/path/to/data", str(e))
        """
        # Update cache
        status = self._status_cache.get(path)
        if status:
            status.error = error
        else:
            self._status_cache[path] = ProcessingStatus(
                path=Path(path),
                error=error
            )

        # Remove from in-progress
        self._in_progress.discard(path)

    def is_in_progress(self, path: str) -> bool:
        """
        Check if path is currently being processed.

        Args:
            path: Path to check

        Returns:
            True if processing is in progress

        Example:
            >>> tracker = CompletionTracker()
            >>> if tracker.is_in_progress("/path/to/data"):
            ...     print("Already processing...")
        """
        return path in self._in_progress

    def get_status(self, path: str) -> Optional[ProcessingStatus]:
        """
        Get processing status for a path.

        Args:
            path: Path to check

        Returns:
            ProcessingStatus or None if not tracked

        Example:
            >>> tracker = CompletionTracker()
            >>> status = tracker.get_status("/path/to/data")
            >>> if status and status.is_complete:
            ...     print(f"Completed at {status.completed_at}")
        """
        return self._status_cache.get(path)

    def get_in_progress_count(self) -> int:
        """
        Get count of paths currently in progress.

        Returns:
            Number of paths being processed

        Example:
            >>> tracker = CompletionTracker()
            >>> print(f"Processing {tracker.get_in_progress_count()} items")
        """
        return len(self._in_progress)

    def get_completed_count(self) -> int:
        """
        Get count of completed paths.

        Returns:
            Number of completed paths

        Example:
            >>> tracker = CompletionTracker()
            >>> print(f"Completed {tracker.get_completed_count()} items")
        """
        return sum(
            1 for status in self._status_cache.values()
            if status.is_complete
        )

    def clear_cache(self) -> None:
        """
        Clear the status cache.

        Use this to force re-checking completion markers from disk.

        Example:
            >>> tracker = CompletionTracker()
            >>> tracker.clear_cache()  # Force re-check
        """
        self._status_cache.clear()
        self._in_progress.clear()

    def needs_conversion(self, stdf_path: str, min_parquet_count: int = 8) -> bool:
        """
        Check if STDF file needs conversion to Parquet.

        Args:
            stdf_path: Path to STDF file
            min_parquet_count: Minimum expected parquet files

        Returns:
            True if conversion is needed

        Example:
            >>> tracker = CompletionTracker()
            >>> if tracker.needs_conversion("/path/file.std"):
            ...     convert_stdf_to_parquet()
        """
        stdf_path = Path(stdf_path)
        parquet_dir = stdf_path.parent / "parquet"

        if not parquet_dir.exists():
            return True

        # Count parquet files
        parquet_files = list(parquet_dir.glob(f"{stdf_path.name}.*.parquet"))
        return len(parquet_files) < min_parquet_count

    def needs_report_generation(self, data_path: str) -> bool:
        """
        Check if report generation is needed.

        Args:
            data_path: Path to data directory

        Returns:
            True if report generation is needed

        Example:
            >>> tracker = CompletionTracker()
            >>> if tracker.needs_report_generation("/path/to/data"):
            ...     generate_reports()
        """
        data_path = Path(data_path)

        # Check for completion marker
        if self.is_complete(str(data_path)):
            return False

        # Check if Report directory exists
        report_dir = data_path / "Report"
        if not report_dir.exists():
            return True

        # If Report directory exists but no completion marker, assume incomplete
        return True

    def get_statistics(self) -> Dict[str, int]:
        """
        Get processing statistics.

        Returns:
            Dictionary with statistics

        Example:
            >>> tracker = CompletionTracker()
            >>> stats = tracker.get_statistics()
            >>> print(f"Completed: {stats['completed']}")
            >>> print(f"In progress: {stats['in_progress']}")
            >>> print(f"Failed: {stats['failed']}")
        """
        completed = sum(
            1 for status in self._status_cache.values()
            if status.is_complete
        )

        failed = sum(
            1 for status in self._status_cache.values()
            if status.error is not None
        )

        return {
            "total": len(self._status_cache),
            "completed": completed,
            "in_progress": len(self._in_progress),
            "failed": failed,
            "pending": len(self._status_cache) - completed - failed - len(self._in_progress)
        }
