"""
ART.stdf - Process Condition Use Case

Use case for processing condition files (anaflow HTML) into reports.
Extracted and refactored from original polling.py and condition.py.

Author: Matteo Terranova (matteo.terranova@st.com)
Organization: STMicroelectronics - MDRF GPAM
"""

import logging
from pathlib import Path
from typing import Optional

from src.domain.models.parameter import Parameter
from src.infrastructure.storage.file_repository import FileRepository


class ProcessConditionUseCase:
    """
    Use case for processing condition files.

    This use case handles the conversion of anaflow HTML files
    into condition reports.

    Responsibilities:
    - Validate condition file exists
    - Extract test conditions from HTML
    - Generate condition report
    - Mark processing as complete
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
        condition_file_path: str,
        parameter: Parameter,
        composite_list: list
    ) -> Path:
        """
        Execute condition processing.

        Args:
            condition_file_path: Path to anaflow HTML file
            parameter: Parameter object with metadata
            composite_list: List of composites to process

        Returns:
            Path to generated report directory

        Example:
            >>> use_case = ProcessConditionUseCase()
            >>> report_dir = use_case.execute(
            ...     "/path/anaflow.html",
            ...     parameter,
            ...     ["INIT", "COMP1"]
            ... )
        """
        condition_file = Path(condition_file_path)

        if not condition_file.exists():
            raise FileNotFoundError(f"Condition file not found: {condition_file}")

        # Log start
        self._log_start(parameter)

        try:
            # Import actual processing logic
            from core import process_condition

            # Process each composite
            for composite in composite_list:
                # Skip specific composites
                if self._should_skip_composite(composite):
                    continue

                # Update parameter for this composite
                local_param = parameter
                local_param.com = composite
                local_param.title = self._create_title(parameter, composite)

                # Generate report
                report_path = self.file_repository.get_report_path(
                    str(condition_file.parent),
                    local_param.to_dict(),
                    "CONDITION2REPORT"
                )

                if not report_path.exists():
                    process_condition(
                        local_param.to_dict(),
                        str(condition_file),
                        None
                    )
                    self.logger.info(f"[CONDITION] Generated {report_path.name}")
                else:
                    self.logger.info(f"[CONDITION] Already exists: {report_path.name}")

            # Mark completion
            self._mark_completion(condition_file.parent)

            # Log completion
            self._log_completion(parameter)

            return self.file_repository.get_report_directory(
                str(condition_file.parent),
                parameter.to_dict(),
                "CONDITION2REPORT"
            )

        except Exception as e:
            self.logger.error(f"[CONDITION] Processing failed: {e}")
            raise

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
        return f"{composite.upper().replace('_', ' ')} {parameter.flow.upper()} condition"

    def _mark_completion(self, directory: Path):
        """
        Mark condition processing as complete.

        Args:
            directory: Directory containing condition file
        """
        self.file_repository.create_completion_marker(str(directory))

    def _log_start(self, parameter: Parameter):
        """Log processing start."""
        self.logger.info(
            f"[CONDITION] Start processing {parameter.code} {parameter.flow}"
        )

    def _log_completion(self, parameter: Parameter):
        """Log processing completion."""
        self.logger.info(
            f"[CONDITION] Completed processing {parameter.code} {parameter.flow}"
        )

    def is_already_processed(self, condition_file_path: str) -> bool:
        """
        Check if condition file has already been processed.

        Args:
            condition_file_path: Path to condition file

        Returns:
            True if already processed
        """
        condition_dir = Path(condition_file_path).parent
        return self.file_repository.check_completion_marker(str(condition_dir))
