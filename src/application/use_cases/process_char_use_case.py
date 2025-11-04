"""
ART.stdf - Process Characterization Use Case

Use case for processing characterization (CHAR) test data.
Extracted and refactored from original polling.py and charv3.py.

Author: Matteo Terranova (matteo.terranova@st.com)
Organization: STMicroelectronics - MDRF GPAM
"""

import logging
from pathlib import Path
from typing import Optional

from src.domain.models.parameter import Parameter
from src.infrastructure.storage.file_repository import FileRepository


class ProcessCharUseCase:
    """
    Use case for processing characterization data.

    This use case handles the generation of characterization reports
    from multi-corner STDF test data.

    Responsibilities:
    - Load characterization test data
    - Process multiple test corners
    - Generate characterization reports
    - Create main menu for navigation
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

    def execute(
        self,
        parameter: Parameter,
        composite_list: list,
        char_path: str
    ) -> Path:
        """
        Execute characterization processing.

        Args:
            parameter: Parameter object with metadata
            composite_list: List of composites to process
            char_path: Base path for characterization data

        Returns:
            Path to report directory

        Example:
            >>> use_case = ProcessCharUseCase()
            >>> report_dir = use_case.execute(
            ...     parameter,
            ...     ["INIT", "COMP1"],
            ...     "/path/to/char"
            ... )
        """
        # Log start
        self._log_start(parameter)

        try:
            # Import char processing logic
            import charv3 as char

            # Get report path
            report_path = self.file_repository.get_report_directory(
                char_path,
                parameter.to_dict(),
                "CHAR"
            )

            # Process each composite
            for composite in composite_list:
                # Skip if already processed
                if self._is_composite_processed(report_path, composite):
                    self.logger.info(f"[CHAR] Already exists: {composite}")
                    continue

                # Skip TTIME and YIELD
                if self._should_skip_composite(composite):
                    continue

                # Update parameter for this composite
                local_param = parameter
                local_param.com = composite
                local_param.title = self._create_title(parameter, composite)

                try:
                    # Generate report for this composite
                    char.run(
                        report_path=str(report_path),
                        parameter=local_param.to_dict(),
                        composite=composite
                    )
                    self.logger.info(f"[CHAR] Generated report for {composite}")

                except Exception as e:
                    self.logger.error(f"[CHAR] Error processing {composite}: {e}")
                    continue

            # Generate main menu
            char.gen_mainmenu(
                parameter=parameter.to_dict(),
                path=str(report_path)
            )

            # Mark completion
            self._mark_completion(char_path, parameter)

            # Log completion
            self._log_completion(parameter)

            return report_path

        except Exception as e:
            self.logger.error(f"[CHAR] Processing failed: {e}")
            raise

    def _is_composite_processed(self, report_path: Path, composite: str) -> bool:
        """
        Check if composite has already been processed.

        Args:
            report_path: Base report path
            composite: Composite name

        Returns:
            True if composite directory exists
        """
        composite_dir = report_path / composite
        return composite_dir.exists()

    def _should_skip_composite(self, composite: str) -> bool:
        """
        Check if composite should be skipped.

        Args:
            composite: Composite name

        Returns:
            True if should skip
        """
        skip_list = ["TTIME", "YIELD"]
        return composite.upper() in skip_list

    def _create_title(self, parameter: Parameter, composite: str) -> str:
        """
        Create report title.

        Args:
            parameter: Parameter object
            composite: Composite name

        Returns:
            Report title string
        """
        return f"{composite.upper().replace('_', ' ')} {parameter.flow.upper()} {parameter.type.lower()}"

    def _mark_completion(self, char_path: str, parameter: Parameter):
        """
        Mark characterization processing as complete.

        Args:
            char_path: Characterization data path
            parameter: Parameter object
        """
        # Mark at the main char folder level
        main_folder = str(Path(char_path).parent / "CHAR")
        self.file_repository.create_completion_marker(main_folder)

    def _log_start(self, parameter: Parameter):
        """Log processing start."""
        self.logger.info(
            f"[CHAR] Start processing {parameter.cut} {parameter.flow} "
            f"{parameter.lot} {parameter.wafer}"
        )

    def _log_completion(self, parameter: Parameter):
        """Log processing completion."""
        self.logger.info(
            f"[CHAR] Completed processing {parameter.cut} {parameter.flow} "
            f"{parameter.lot} {parameter.wafer}"
        )

    def is_already_processed(self, char_path: str) -> bool:
        """
        Check if characterization has already been processed.

        Args:
            char_path: Characterization data path

        Returns:
            True if already processed
        """
        main_folder = str(Path(char_path).parent / "CHAR")
        return self.file_repository.check_completion_marker(main_folder)
