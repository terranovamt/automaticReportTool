"""File management service"""

from pathlib import Path
from typing import List, Optional
from src.core.models import STDFFile, FileStatus
from src.core.constants import STDF_EXTENSIONS
from src.utils.file_utils import (
    find_files_by_extension,
    wait_for_file_stable,
    is_file_locked,
    get_file_size_mb
)


class FileService:
    """
    Service for managing STDF files and directories.

    Handles file discovery, validation, and status tracking.
    """

    def __init__(self):
        self.tracked_files: dict[Path, STDFFile] = {}

    def discover_stdf_files(
        self,
        directory: Path,
        recursive: bool = True,
        wait_stable: bool = True
    ) -> List[STDFFile]:
        """
        Discover STDF files in directory.

        Args:
            directory: Directory to search
            recursive: Search subdirectories
            wait_stable: Wait for files to stabilize before returning

        Returns:
            List of STDFFile objects
        """
        directory = Path(directory)
        if not directory.exists():
            return []

        # Find all STDF files
        file_paths = find_files_by_extension(
            directory,
            list(STDF_EXTENSIONS),
            recursive=recursive
        )

        stdf_files = []
        for file_path in file_paths:
            # Skip locked files
            if is_file_locked(file_path):
                print(f"Skipping locked file: {file_path}")
                continue

            # Wait for file to stabilize if requested
            if wait_stable:
                if not wait_for_file_stable(file_path, timeout=30):
                    print(f"File not stable, skipping: {file_path}")
                    continue

            # Create STDFFile object
            stdf_file = STDFFile(
                path=file_path,
                filename=file_path.name,
                status=FileStatus.PENDING
            )

            stdf_files.append(stdf_file)
            self.tracked_files[file_path] = stdf_file

        return stdf_files

    def get_file_status(self, file_path: Path) -> Optional[FileStatus]:
        """Get status of tracked file."""
        stdf_file = self.tracked_files.get(Path(file_path))
        return stdf_file.status if stdf_file else None

    def update_file_status(self, file_path: Path, status: FileStatus):
        """Update status of tracked file."""
        if file_path in self.tracked_files:
            self.tracked_files[file_path].status = status

    def filter_by_status(self, status: FileStatus) -> List[STDFFile]:
        """Get all files with specified status."""
        return [
            stdf_file
            for stdf_file in self.tracked_files.values()
            if stdf_file.status == status
        ]

    def get_pending_files(self) -> List[STDFFile]:
        """Get all pending files."""
        return self.filter_by_status(FileStatus.PENDING)

    def get_processing_files(self) -> List[STDFFile]:
        """Get all files currently processing."""
        return self.filter_by_status(FileStatus.PROCESSING)

    def get_completed_files(self) -> List[STDFFile]:
        """Get all completed files."""
        return self.filter_by_status(FileStatus.COMPLETED)

    def get_failed_files(self) -> List[STDFFile]:
        """Get all failed files."""
        return self.filter_by_status(FileStatus.FAILED)

    def get_statistics(self) -> dict:
        """Get processing statistics."""
        return {
            "total": len(self.tracked_files),
            "pending": len(self.get_pending_files()),
            "processing": len(self.get_processing_files()),
            "completed": len(self.get_completed_files()),
            "failed": len(self.get_failed_files()),
        }

    def reset(self):
        """Reset all tracked files."""
        self.tracked_files.clear()
