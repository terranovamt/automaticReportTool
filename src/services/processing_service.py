"""Main processing service for orchestrating STDF to report pipeline"""

from pathlib import Path
from typing import List, Optional
import time

from src.core.models import (
    STDFFile,
    FileStatus,
    ProcessingResult,
    ReportType
)
from src.processors.stdf_processor import STDFProcessor
from src.processors.report_processor import ReportProcessorFactory
from src.services.file_service import FileService
from src.utils.parallel import parallel_process_files
from config.settings import settings


class ProcessingService:
    """
    Main service for orchestrating the complete STDF processing pipeline.

    Handles:
    - STDF file discovery
    - STDF to Parquet conversion
    - Report generation
    - Parallel processing coordination
    """

    def __init__(self):
        self.file_service = FileService()
        self.stdf_processor = STDFProcessor()
        self.results: List[ProcessingResult] = []

    def process_directory(
        self,
        input_dir: Path,
        output_dir: Optional[Path] = None,
        recursive: bool = True,
        parallel: bool = True
    ) -> List[ProcessingResult]:
        """
        Process all STDF files in directory.

        Args:
            input_dir: Directory containing STDF files
            output_dir: Output directory (default: input_dir/parquet)
            recursive: Search subdirectories
            parallel: Use parallel processing

        Returns:
            List of processing results
        """
        input_dir = Path(input_dir)
        if output_dir is None:
            output_dir = input_dir / "parquet"

        print(f"[ProcessingService] Discovering STDF files in {input_dir}...")

        # Discover files
        stdf_files = self.file_service.discover_stdf_files(
            input_dir,
            recursive=recursive,
            wait_stable=True
        )

        if not stdf_files:
            print(f"[ProcessingService] No STDF files found")
            return []

        print(f"[ProcessingService] Found {len(stdf_files)} STDF files")

        # Process files
        if parallel and len(stdf_files) > 1:
            results = self._process_parallel(stdf_files, output_dir)
        else:
            results = self._process_sequential(stdf_files, output_dir)

        self.results.extend(results)

        # Update file statuses
        for result in results:
            status = FileStatus.COMPLETED if result.success else FileStatus.FAILED
            self.file_service.update_file_status(result.file_path, status)

        # Print statistics
        stats = self.get_statistics()
        print(f"\n[ProcessingService] Processing complete:")
        print(f"  Total: {stats['total']}")
        print(f"  Success: {stats['successful']}")
        print(f"  Failed: {stats['failed']}")
        print(f"  Total time: {stats['total_time']:.2f}s")

        return results

    def _process_sequential(
        self,
        stdf_files: List[STDFFile],
        output_dir: Path
    ) -> List[ProcessingResult]:
        """Process files sequentially."""
        results = []

        for stdf_file in stdf_files:
            print(f"[ProcessingService] Processing {stdf_file.filename}...")

            self.file_service.update_file_status(
                stdf_file.path,
                FileStatus.PROCESSING
            )

            # Determine output path for this file
            file_output_dir = output_dir / stdf_file.path.parent.name

            result = self.stdf_processor.process(
                stdf_file.path,
                file_output_dir
            )

            results.append(result)

            status = "SUCCESS" if result.success else "FAILED"
            print(f"[ProcessingService] {status}: {stdf_file.filename}")

        return results

    def _process_parallel(
        self,
        stdf_files: List[STDFFile],
        output_dir: Path
    ) -> List[ProcessingResult]:
        """Process files in parallel."""
        print(f"[ProcessingService] Using parallel processing with "
              f"{settings.processing.parallel_stdf_workers} workers")

        # Mark all as processing
        for stdf_file in stdf_files:
            self.file_service.update_file_status(
                stdf_file.path,
                FileStatus.PROCESSING
            )

        # Create processing function
        def process_file(stdf_file: STDFFile) -> ProcessingResult:
            file_output_dir = output_dir / stdf_file.path.parent.name
            return self.stdf_processor.process(
                stdf_file.path,
                file_output_dir
            )

        # Process in parallel
        results = parallel_process_files(
            stdf_files,
            process_file,
            num_workers=settings.processing.parallel_stdf_workers
        )

        # Handle results (if parallel_process_files returns tuples)
        if results and isinstance(results[0], tuple):
            # Convert tuples to ProcessingResult objects
            processed_results = []
            for success, file_path, error in results:
                result = ProcessingResult(
                    success=success,
                    file_path=file_path,
                    error_message=error if not success else None
                )
                processed_results.append(result)
            return processed_results

        return results

    def generate_reports(
        self,
        data_dir: Path,
        output_dir: Path,
        report_types: Optional[List[ReportType]] = None
    ) -> List[ProcessingResult]:
        """
        Generate reports from processed data.

        Args:
            data_dir: Directory containing Parquet data
            output_dir: Output directory for reports
            report_types: Types of reports to generate (default: all enabled)

        Returns:
            List of processing results
        """
        if report_types is None:
            # Use enabled report types from settings
            report_types = []
            if settings.report.enable_condition:
                report_types.append(ReportType.CONDITION)
            if settings.report.enable_char:
                report_types.append(ReportType.CHAR)
            if settings.report.enable_shmoo:
                report_types.append(ReportType.SHMOO)

        results = []

        for report_type in report_types:
            print(f"[ProcessingService] Generating {report_type.value} report...")

            try:
                processor = ReportProcessorFactory.create(report_type)
                result = processor.process(data_dir, output_dir)
                results.append(result)

                status = "SUCCESS" if result.success else "FAILED"
                print(f"[ProcessingService] {status}: {report_type.value} report")

            except Exception as e:
                print(f"[ProcessingService] Error generating {report_type.value} report: {e}")
                results.append(ProcessingResult(
                    success=False,
                    file_path=data_dir,
                    error_message=str(e)
                ))

        return results

    def get_statistics(self) -> dict:
        """Get processing statistics."""
        successful = sum(1 for r in self.results if r.success)
        failed = sum(1 for r in self.results if not r.success)
        total_time = sum(r.processing_time for r in self.results if r.processing_time)

        return {
            "total": len(self.results),
            "successful": successful,
            "failed": failed,
            "success_rate": successful / len(self.results) if self.results else 0,
            "total_time": total_time,
            "avg_time": total_time / len(self.results) if self.results else 0,
        }

    def reset(self):
        """Reset service state."""
        self.results.clear()
        self.file_service.reset()
