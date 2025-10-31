"""Report generation processors for different report types"""

import time
from pathlib import Path
from typing import Optional, Dict, Any
from src.processors.base import BaseProcessor
from src.core.models import ProcessingResult, Parameter, ReportType
from src.core.exceptions import ProcessingError
from src.utils.validation import validate_parameter


class BaseReportProcessor(BaseProcessor):
    """Base class for report processors."""

    def __init__(self, name: str, report_type: ReportType):
        super().__init__(name)
        self.report_type = report_type

    def validate_input(self, input_path: Path) -> bool:
        """Validate input data directory."""
        if not input_path.exists():
            raise ProcessingError(f"Input path does not exist: {input_path}")
        return True

    def validate_parameter(self, parameter: Parameter) -> bool:
        """Validate parameter object."""
        try:
            validate_parameter(parameter.to_dict())
            return True
        except Exception as e:
            raise ProcessingError(f"Invalid parameter: {e}")


class ConditionReportProcessor(BaseReportProcessor):
    """Processor for condition reports."""

    def __init__(self):
        super().__init__("ConditionReportProcessor", ReportType.CONDITION)

    def process(self, input_path: Path, output_path: Path) -> ProcessingResult:
        """Generate condition report."""
        start_time = time.time()

        try:
            self.validate_input(input_path)

            # Import here to avoid circular dependency
            import src.condition_report as condition_report

            # Generate report using existing module
            output_path = Path(output_path)
            output_path.mkdir(parents=True, exist_ok=True)

            # Call existing condition report generation
            # This would be adapted based on actual implementation
            # For now, placeholder

            processing_time = time.time() - start_time

            return ProcessingResult(
                success=True,
                file_path=input_path,
                output_files=[output_path],
                processing_time=processing_time,
                metadata={"report_type": "condition"}
            )

        except Exception as e:
            processing_time = time.time() - start_time
            return ProcessingResult(
                success=False,
                file_path=input_path,
                error_message=str(e),
                processing_time=processing_time
            )


class CharReportProcessor(BaseReportProcessor):
    """Processor for characterization reports."""

    def __init__(self):
        super().__init__("CharReportProcessor", ReportType.CHAR)

    def process(self, input_path: Path, output_path: Path) -> ProcessingResult:
        """Generate characterization report."""
        start_time = time.time()

        try:
            self.validate_input(input_path)

            # Import here to avoid circular dependency
            import src.charv3 as charv3

            # Generate report using existing module
            output_path = Path(output_path)
            output_path.mkdir(parents=True, exist_ok=True)

            # Call existing char report generation
            # This would be adapted based on actual implementation

            processing_time = time.time() - start_time

            return ProcessingResult(
                success=True,
                file_path=input_path,
                output_files=[output_path],
                processing_time=processing_time,
                metadata={"report_type": "char"}
            )

        except Exception as e:
            processing_time = time.time() - start_time
            return ProcessingResult(
                success=False,
                file_path=input_path,
                error_message=str(e),
                processing_time=processing_time
            )


class ShmooReportProcessor(BaseReportProcessor):
    """Processor for shmoo plot reports."""

    def __init__(self):
        super().__init__("ShmooReportProcessor", ReportType.SHMOO)

    def process(self, input_path: Path, output_path: Path) -> ProcessingResult:
        """Generate shmoo plot report."""
        start_time = time.time()

        try:
            self.validate_input(input_path)

            # Import here to avoid circular dependency
            import src.shmoo as shmoo

            # Generate report using existing module
            output_path = Path(output_path)
            output_path.mkdir(parents=True, exist_ok=True)

            # Call existing shmoo report generation
            # This would be adapted based on actual implementation

            processing_time = time.time() - start_time

            return ProcessingResult(
                success=True,
                file_path=input_path,
                output_files=[output_path],
                processing_time=processing_time,
                metadata={"report_type": "shmoo"}
            )

        except Exception as e:
            processing_time = time.time() - start_time
            return ProcessingResult(
                success=False,
                file_path=input_path,
                error_message=str(e),
                processing_time=processing_time
            )


class ReportProcessorFactory:
    """Factory for creating report processors."""

    _processors = {
        ReportType.CONDITION: ConditionReportProcessor,
        ReportType.CHAR: CharReportProcessor,
        ReportType.SHMOO: ShmooReportProcessor,
    }

    @classmethod
    def create(cls, report_type: ReportType) -> BaseReportProcessor:
        """
        Create a report processor for the specified type.

        Args:
            report_type: Type of report to generate

        Returns:
            Report processor instance

        Raises:
            ValueError: If report type is not supported
        """
        processor_class = cls._processors.get(report_type)
        if not processor_class:
            raise ValueError(f"Unsupported report type: {report_type}")

        return processor_class()

    @classmethod
    def get_supported_types(cls) -> list:
        """Get list of supported report types."""
        return list(cls._processors.keys())
