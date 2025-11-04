"""
ART.stdf - Process Shmoo Use Case

Use case for processing shmoo files (.shm) into interactive plots.
Extracted and refactored from original polling.py and shmoo.py.

Author: Matteo Terranova (matteo.terranova@st.com)
Organization: STMicroelectronics - MDRF GPAM
"""

import logging
from pathlib import Path
from typing import Optional

from src.infrastructure.storage.file_repository import FileRepository


class ProcessShmooUseCase:
    """
    Use case for processing shmoo files.

    This use case handles the conversion of .shm files into
    interactive shmoo plot visualizations.

    Responsibilities:
    - Validate shmoo directory exists
    - Find .shm files
    - Generate shmoo plot visualizations
    - Save reports
    """

    def __init__(
        self,
        file_repository: Optional[FileRepository] = None,
        logger: Optional[logging.Logger] = None
    ):
        """
        Initialize the use case.

        Args:
            file_repository: File repository instance (injected)
            logger: Logger instance (injected)
        """
        self.file_repository = file_repository or FileRepository()
        self.logger = logger or logging.getLogger(__name__)

    def execute(self, shmoo_directory: str) -> Path:
        """
        Execute shmoo processing.

        Args:
            shmoo_directory: Path to directory containing .shm files

        Returns:
            Path to directory with generated shmoo reports

        Raises:
            FileNotFoundError: If shmoo directory doesn't exist
            ValueError: If no .shm files found

        Example:
            >>> use_case = ProcessShmooUseCase()
            >>> report_dir = use_case.execute("/path/to/SHMOO")
        """
        shmoo_dir = Path(shmoo_directory)

        if not shmoo_dir.exists():
            raise FileNotFoundError(f"Shmoo directory not found: {shmoo_dir}")

        # Find .shm files
        shm_files = list(shmoo_dir.glob("*.shm"))

        if not shm_files:
            raise ValueError(f"No .shm files found in {shmoo_dir}")

        # Log start
        self._log_start(shmoo_dir, len(shm_files))

        try:
            # Import and use shmoo processor
            from shmoo import ShmooVisualizer

            visualizer = ShmooVisualizer()
            visualizer.process_shmoo_files(str(shmoo_dir))

            # Log completion
            self._log_completion(shmoo_dir)

            return shmoo_dir

        except ImportError:
            error_msg = f"[SHMOO] Shmoo library not available for {shmoo_dir}"
            self.logger.error(error_msg)
            raise ImportError(error_msg)

        except Exception as e:
            self.logger.error(f"[SHMOO] Processing failed for {shmoo_dir}: {e}")
            raise

    def _log_start(self, shmoo_dir: Path, file_count: int):
        """Log shmoo processing start."""
        clean_path = str(shmoo_dir).replace(
            "\\\\gpm-pe-data.gnb.st.com\\ENGI_MCD_STDF\\", ""
        ).replace("\\", " ")

        self.logger.info(
            f"[SHMOO] Start processing {file_count} files in {clean_path}"
        )

    def _log_completion(self, shmoo_dir: Path):
        """Log shmoo processing completion."""
        clean_path = str(shmoo_dir).replace(
            "\\\\gpm-pe-data.gnb.st.com\\ENGI_MCD_STDF\\", ""
        ).replace("\\", " ")

        self.logger.info(f"[SHMOO] Completed processing {clean_path}")

    def has_shmoo_files(self, directory: str) -> bool:
        """
        Check if directory contains .shm files.

        Args:
            directory: Directory path to check

        Returns:
            True if .shm files found

        Example:
            >>> use_case = ProcessShmooUseCase()
            >>> if use_case.has_shmoo_files("/path/to/dir"):
            ...     use_case.execute("/path/to/dir")
        """
        try:
            shmoo_dir = Path(directory)
            return len(list(shmoo_dir.glob("*.shm"))) > 0
        except Exception:
            return False
