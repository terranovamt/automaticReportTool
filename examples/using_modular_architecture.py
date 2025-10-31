"""
Example usage of the modular ART.stdf architecture

This demonstrates how to use the new modular service layer
for processing STDF files and generating reports.
"""

from pathlib import Path
from src.services import ProcessingService, FileService
from src.core.models import ReportType
from config.settings import settings

def example_basic_processing():
    """Basic example: Process STDF files in a directory."""
    print("=" * 60)
    print("Example 1: Basic STDF Processing")
    print("=" * 60)

    # Create service
    service = ProcessingService()

    # Process all STDF files in directory
    input_dir = Path("./STDF/P6AX86/LOT01")
    results = service.process_directory(
        input_dir=input_dir,
        recursive=True,
        parallel=True  # Use parallel processing
    )

    # Check results
    for result in results:
        if result.success:
            print(f"✓ {result.file_path.name}: {len(result.output_files)} files created")
        else:
            print(f"✗ {result.file_path.name}: {result.error_message}")

    # Print statistics
    stats = service.get_statistics()
    print(f"\nStatistics:")
    print(f"  Success rate: {stats['success_rate']*100:.1f}%")
    print(f"  Average time: {stats['avg_time']:.2f}s per file")


def example_with_custom_settings():
    """Example with custom configuration."""
    print("\n" + "=" * 60)
    print("Example 2: Custom Settings")
    print("=" * 60)

    # Configure processing
    settings.processing.parallel_stdf_workers = 4
    settings.processing.compression = "lz4"
    settings.processing.use_polars = True

    # Create service
    service = ProcessingService()

    # Process files
    results = service.process_directory(
        input_dir=Path("./STDF"),
        output_dir=Path("./output/parquet"),
        recursive=True,
        parallel=True
    )

    print(f"Processed {len(results)} files with {settings.processing.parallel_stdf_workers} workers")


def example_report_generation():
    """Example: Generate reports from processed data."""
    print("\n" + "=" * 60)
    print("Example 3: Report Generation")
    print("=" * 60)

    # Create service
    service = ProcessingService()

    # Generate specific reports
    data_dir = Path("./output/parquet")
    output_dir = Path("./output/reports")

    results = service.generate_reports(
        data_dir=data_dir,
        output_dir=output_dir,
        report_types=[
            ReportType.CONDITION,
            ReportType.CHAR,
        ]
    )

    for result in results:
        if result.success:
            print(f"✓ {result.metadata.get('report_type')} report generated")
        else:
            print(f"✗ Report generation failed: {result.error_message}")


def example_file_discovery():
    """Example: Discover and track STDF files."""
    print("\n" + "=" * 60)
    print("Example 4: File Discovery")
    print("=" * 60)

    # Create file service
    file_service = FileService()

    # Discover files
    stdf_files = file_service.discover_stdf_files(
        directory=Path("./STDF"),
        recursive=True,
        wait_stable=True  # Wait for files to finish copying
    )

    print(f"Found {len(stdf_files)} STDF files")

    for stdf_file in stdf_files:
        print(f"  {stdf_file.filename}: {stdf_file.size_bytes / 1024:.1f} KB")

    # Get statistics
    stats = file_service.get_statistics()
    print(f"\nFile statistics:")
    print(f"  Pending: {stats['pending']}")
    print(f"  Processing: {stats['processing']}")
    print(f"  Completed: {stats['completed']}")


def example_complete_pipeline():
    """Example: Complete pipeline from STDF to reports."""
    print("\n" + "=" * 60)
    print("Example 5: Complete Pipeline")
    print("=" * 60)

    service = ProcessingService()

    # Step 1: Process STDF files
    print("\n[Step 1] Processing STDF files...")
    stdf_results = service.process_directory(
        input_dir=Path("./STDF/P6AX86"),
        output_dir=Path("./output/data"),
        recursive=True,
        parallel=True
    )

    successful_count = sum(1 for r in stdf_results if r.success)
    print(f"Converted {successful_count}/{len(stdf_results)} STDF files")

    # Step 2: Generate reports
    if successful_count > 0:
        print("\n[Step 2] Generating reports...")
        report_results = service.generate_reports(
            data_dir=Path("./output/data"),
            output_dir=Path("./output/reports")
        )

        print(f"Generated {len(report_results)} reports")

    # Step 3: Show final statistics
    print("\n[Step 3] Final Statistics:")
    stats = service.get_statistics()
    print(f"  Total operations: {stats['total']}")
    print(f"  Success rate: {stats['success_rate']*100:.1f}%")
    print(f"  Total time: {stats['total_time']:.2f}s")


if __name__ == "__main__":
    # Run examples
    # Uncomment the example you want to run

    # example_basic_processing()
    # example_with_custom_settings()
    # example_report_generation()
    # example_file_discovery()
    example_complete_pipeline()
