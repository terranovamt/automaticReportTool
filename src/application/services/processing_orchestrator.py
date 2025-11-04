"""
ART.stdf - Processing Orchestrator Service

Central orchestrator for coordinating all processing workflows.
Extracted and refactored from original polling.py STDFProcessingSystem.

Author: Matteo Terranova (matteo.terranova@st.com)
Organization: STMicroelectronics - MDRF GPAM
"""

import logging
from typing import List, Optional
from pathlib import Path

from src.domain.models.parameter import Parameter
from src.application.use_cases.convert_stdf_use_case import ConvertSTDFUseCase
from src.application.use_cases.generate_report_use_case import GenerateReportUseCase
from src.application.use_cases.process_condition_use_case import ProcessConditionUseCase
from src.application.use_cases.process_shmoo_use_case import ProcessShmooUseCase
from src.application.use_cases.process_char_use_case import ProcessCharUseCase
from src.application.dto.processing_result_dto import CycleResultDTO
from src.application.services.completion_tracker import CompletionTracker
from src.infrastructure.logging.logger_factory import LoggerFactory


class ProcessingOrchestrator:
    """
    Central orchestrator for all processing operations.

    This service coordinates the execution of different processing
    workflows using the use cases.

    Responsibilities:
    - Coordinate STDF conversion
    - Coordinate report generation
    - Coordinate condition processing
    - Coordinate shmoo processing
    - Coordinate characterization
    - Track completion status
    - Handle errors gracefully
    """

    def __init__(
        self,
        convert_use_case: Optional[ConvertSTDFUseCase] = None,
        generate_report_use_case: Optional[GenerateReportUseCase] = None,
        condition_use_case: Optional[ProcessConditionUseCase] = None,
        shmoo_use_case: Optional[ProcessShmooUseCase] = None,
        char_use_case: Optional[ProcessCharUseCase] = None,
        completion_tracker: Optional[CompletionTracker] = None,
        logger: Optional[logging.Logger] = None
    ):
        """
        Initialize processing orchestrator.

        Args:
            convert_use_case: STDF conversion use case
            generate_report_use_case: Report generation use case
            condition_use_case: Condition processing use case
            shmoo_use_case: Shmoo processing use case
            char_use_case: Characterization use case
            completion_tracker: Completion tracking service
            logger: Logger instance
        """
        # Inject dependencies (with defaults)
        self.convert_use_case = convert_use_case or ConvertSTDFUseCase()
        self.generate_report_use_case = generate_report_use_case or GenerateReportUseCase()
        self.condition_use_case = condition_use_case or ProcessConditionUseCase()
        self.shmoo_use_case = shmoo_use_case or ProcessShmooUseCase()
        self.char_use_case = char_use_case or ProcessCharUseCase()
        self.completion_tracker = completion_tracker or CompletionTracker()

        # Logger
        self.logger = logger or LoggerFactory.get_logger("orchestrator")

    def process_stdf_files(
        self,
        stdf_files: List[str],
        parameters: Optional[List[Parameter]] = None
    ) -> int:
        """
        Process STDF files (convert to Parquet).

        Args:
            stdf_files: List of STDF file paths
            parameters: Optional list of parameters (one per file)

        Returns:
            Number of files successfully processed

        Example:
            >>> orchestrator = ProcessingOrchestrator()
            >>> count = orchestrator.process_stdf_files(["/path/file1.std", "/path/file2.std"])
            >>> print(f"Processed {count} files")
        """
        processed_count = 0

        for i, stdf_file in enumerate(stdf_files):
            # Get parameter if available
            parameter = parameters[i] if parameters and i < len(parameters) else None

            try:
                # Check if already in progress
                if self.completion_tracker.is_in_progress(stdf_file):
                    self.logger.warning(f"Already processing: {stdf_file}")
                    continue

                # Mark as in progress
                self.completion_tracker.mark_in_progress(stdf_file)

                # Execute conversion
                self.convert_use_case.execute(stdf_file, parameter)

                # Mark as complete
                self.completion_tracker.mark_complete(stdf_file)

                processed_count += 1

            except Exception as e:
                self.logger.error(f"Failed to process STDF {stdf_file}: {e}")
                self.completion_tracker.mark_failed(stdf_file, str(e))

        return processed_count

    def process_data_files(
        self,
        data_files: List[str],
        parameters: Optional[List[Parameter]] = None
    ) -> int:
        """
        Process data files (generate reports from Parquet).

        Args:
            data_files: List of data file paths
            parameters: Optional list of parameters

        Returns:
            Number of reports successfully generated

        Example:
            >>> orchestrator = ProcessingOrchestrator()
            >>> count = orchestrator.process_data_files(["/path/data1", "/path/data2"])
        """
        processed_count = 0

        for i, data_file in enumerate(data_files):
            parameter = parameters[i] if parameters and i < len(parameters) else None

            if not parameter:
                self.logger.warning(f"No parameter for data file: {data_file}")
                continue

            try:
                # Check if already complete
                if self.completion_tracker.is_complete(data_file):
                    continue

                # Mark as in progress
                self.completion_tracker.mark_in_progress(data_file)

                # Execute report generation
                self.generate_report_use_case.execute(
                    parameter,
                    data_file
                )

                # Mark as complete
                self.completion_tracker.mark_complete(data_file)

                processed_count += 1

            except Exception as e:
                self.logger.error(f"Failed to generate report for {data_file}: {e}")
                self.completion_tracker.mark_failed(data_file, str(e))

        return processed_count

    def process_condition_files(
        self,
        condition_files: List[str],
        parameters: Optional[List[Parameter]] = None,
        composite_lists: Optional[List[List[str]]] = None
    ) -> int:
        """
        Process condition files.

        Args:
            condition_files: List of condition file paths
            parameters: Optional list of parameters
            composite_lists: Optional list of composite lists

        Returns:
            Number of condition files successfully processed

        Example:
            >>> orchestrator = ProcessingOrchestrator()
            >>> count = orchestrator.process_condition_files(
            ...     ["/path/anaflow.html"],
            ...     parameters=[param],
            ...     composite_lists=[["INIT", "COMP1"]]
            ... )
        """
        processed_count = 0

        for i, condition_file in enumerate(condition_files):
            parameter = parameters[i] if parameters and i < len(parameters) else None
            composite_list = composite_lists[i] if composite_lists and i < len(composite_lists) else []

            if not parameter:
                self.logger.warning(f"No parameter for condition file: {condition_file}")
                continue

            try:
                # Check if already processed
                if self.condition_use_case.is_already_processed(condition_file):
                    continue

                # Mark as in progress
                self.completion_tracker.mark_in_progress(condition_file)

                # Execute condition processing
                self.condition_use_case.execute(
                    condition_file,
                    parameter,
                    composite_list
                )

                # Mark as complete
                self.completion_tracker.mark_complete(condition_file)

                processed_count += 1

            except Exception as e:
                self.logger.error(f"Failed to process condition {condition_file}: {e}")
                self.completion_tracker.mark_failed(condition_file, str(e))

        return processed_count

    def process_shmoo_directories(self, shmoo_dirs: List[str]) -> int:
        """
        Process shmoo directories.

        Args:
            shmoo_dirs: List of shmoo directory paths

        Returns:
            Number of shmoo directories successfully processed

        Example:
            >>> orchestrator = ProcessingOrchestrator()
            >>> count = orchestrator.process_shmoo_directories(["/path/SHMOO"])
        """
        processed_count = 0

        for shmoo_dir in shmoo_dirs:
            try:
                # Check if has shmoo files
                if not self.shmoo_use_case.has_shmoo_files(shmoo_dir):
                    continue

                # Mark as in progress
                self.completion_tracker.mark_in_progress(shmoo_dir)

                # Execute shmoo processing
                self.shmoo_use_case.execute(shmoo_dir)

                # Mark as complete
                self.completion_tracker.mark_complete(shmoo_dir)

                processed_count += 1

            except Exception as e:
                self.logger.error(f"Failed to process shmoo {shmoo_dir}: {e}")
                self.completion_tracker.mark_failed(shmoo_dir, str(e))

        return processed_count

    def process_char_datasets(
        self,
        char_paths: List[str],
        parameters: Optional[List[Parameter]] = None,
        composite_lists: Optional[List[List[str]]] = None
    ) -> int:
        """
        Process characterization datasets.

        Args:
            char_paths: List of characterization paths
            parameters: Optional list of parameters
            composite_lists: Optional list of composite lists

        Returns:
            Number of char datasets successfully processed

        Example:
            >>> orchestrator = ProcessingOrchestrator()
            >>> count = orchestrator.process_char_datasets(
            ...     ["/path/char"],
            ...     parameters=[param],
            ...     composite_lists=[["INIT"]]
            ... )
        """
        processed_count = 0

        for i, char_path in enumerate(char_paths):
            parameter = parameters[i] if parameters and i < len(parameters) else None
            composite_list = composite_lists[i] if composite_lists and i < len(composite_lists) else []

            if not parameter:
                self.logger.warning(f"No parameter for char path: {char_path}")
                continue

            try:
                # Check if already processed
                if self.char_use_case.is_already_processed(char_path):
                    continue

                # Mark as in progress
                self.completion_tracker.mark_in_progress(char_path)

                # Execute characterization processing
                self.char_use_case.execute(
                    parameter,
                    composite_list,
                    char_path
                )

                # Mark as complete
                self.completion_tracker.mark_complete(char_path)

                processed_count += 1

            except Exception as e:
                self.logger.error(f"Failed to process char {char_path}: {e}")
                self.completion_tracker.mark_failed(char_path, str(e))

        return processed_count

    def process_cycle(
        self,
        stdf_files: List[str] = None,
        data_files: List[str] = None,
        condition_files: List[str] = None,
        shmoo_dirs: List[str] = None,
        char_paths: List[str] = None,
        **kwargs
    ) -> CycleResultDTO:
        """
        Process a complete cycle of operations.

        Args:
            stdf_files: List of STDF files to convert
            data_files: List of data files for report generation
            condition_files: List of condition files
            shmoo_dirs: List of shmoo directories
            char_paths: List of characterization paths
            **kwargs: Additional arguments (parameters, composite_lists, etc.)

        Returns:
            CycleResultDTO with processing statistics

        Example:
            >>> orchestrator = ProcessingOrchestrator()
            >>> result = orchestrator.process_cycle(
            ...     stdf_files=["/path/file.std"],
            ...     data_files=["/path/data"]
            ... )
            >>> print(result)  # STDF=1, Report=1, ...
        """
        result = CycleResultDTO()

        # Process condition files first (usually quickest)
        if condition_files:
            result.condition_count = self.process_condition_files(
                condition_files,
                parameters=kwargs.get('condition_parameters'),
                composite_lists=kwargs.get('condition_composites')
            )

        # Process shmoo directories
        if shmoo_dirs:
            result.shmoo_count = self.process_shmoo_directories(shmoo_dirs)

        # Process data files (report generation)
        if data_files:
            result.report_count = self.process_data_files(
                data_files,
                parameters=kwargs.get('data_parameters')
            )

        # Process STDF files (conversion)
        if stdf_files:
            result.stdf_count = self.process_stdf_files(
                stdf_files,
                parameters=kwargs.get('stdf_parameters')
            )

        # Process characterization
        if char_paths:
            result.char_count = self.process_char_datasets(
                char_paths,
                parameters=kwargs.get('char_parameters'),
                composite_lists=kwargs.get('char_composites')
            )

        return result

    def get_statistics(self) -> dict:
        """
        Get processing statistics.

        Returns:
            Dictionary with statistics

        Example:
            >>> orchestrator = ProcessingOrchestrator()
            >>> stats = orchestrator.get_statistics()
            >>> print(f"Completed: {stats['completed']}")
        """
        return self.completion_tracker.get_statistics()
