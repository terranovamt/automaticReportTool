"""
Example: Generate Reports Using New Modular System

This example shows how to use the new modular report generation system
to create professional HTML reports with interactive charts.

Author: Matteo Terranova (matteo.terranova@st.com)
Organization: STMicroelectronics - MDRF GPAM
"""

from pathlib import Path
import polars as pl

from src.reports import (
    ConditionReportGenerator,
    YieldReportGenerator,
    VolumeReportGenerator,
    LoopTimeReportGenerator,
    CharReportGenerator
)


def generate_condition_report(parquet_dir: Path, output_dir: Path):
    """Generate condition report from Parquet files."""
    print("[Example] Generating Condition Report...")

    # Load data from Parquet files
    data = {
        'ptr': pl.read_parquet(parquet_dir / "*.ptr.parquet"),
        'ftr': pl.read_parquet(parquet_dir / "*.ftr.parquet"),
        'prr': pl.read_parquet(parquet_dir / "*.prr.parquet"),
        'mir': pl.read_parquet(parquet_dir / "*.mir.parquet"),
    }

    # Parameters from test
    parameter = {
        'CUT': '44E',
        'FLOW': 'EWS1',
        'LOT': 'LOT123',
        'WAFER': 'W01',
        'temperature': 25,
        'TYPE': 'CONDITION'
    }

    # Generate report
    generator = ConditionReportGenerator()
    report_path = generator.generate(
        parameter=parameter,
        data=data,
        output_path=output_dir
    )

    print(f"✓ Condition report generated: {report_path}")
    return report_path


def generate_yield_report(parquet_dir: Path, output_dir: Path):
    """Generate yield report from Parquet files."""
    print("[Example] Generating Yield Report...")

    # Load data
    data = {
        'prr': pl.read_parquet(parquet_dir / "*.prr.parquet"),
        'hbr': pl.read_parquet(parquet_dir / "*.hbr.parquet"),
        'sbr': pl.read_parquet(parquet_dir / "*.sbr.parquet"),
    }

    parameter = {
        'CUT': '44E',
        'FLOW': 'FT',
        'LOT': 'LOT123',
        'WAFER': 'W01',
        'TYPE': 'YIELD'
    }

    # Generate report
    generator = YieldReportGenerator()
    report_path = generator.generate(
        parameter=parameter,
        data=data,
        output_path=output_dir
    )

    print(f"✓ Yield report generated: {report_path}")
    return report_path


def generate_volume_report(parquet_dir: Path, output_dir: Path):
    """Generate volume report from Parquet files."""
    print("[Example] Generating Volume Report...")

    # Load data
    data = {
        'ptr': pl.read_parquet(parquet_dir / "*.ptr.parquet"),
        'ftr': pl.read_parquet(parquet_dir / "*.ftr.parquet"),
        'prr': pl.read_parquet(parquet_dir / "*.prr.parquet"),
    }

    parameter = {
        'CUT': '44E',
        'FLOW': 'FT',
        'LOT': 'LOT123',
        'WAFER': 'W01',
        'TYPE': 'VOLUME'
    }

    # Generate report
    generator = VolumeReportGenerator()
    report_path = generator.generate(
        parameter=parameter,
        data=data,
        output_path=output_dir
    )

    print(f"✓ Volume report generated: {report_path}")
    return report_path


def generate_char_report(parquet_dir: Path, output_dir: Path):
    """Generate characterization report from Parquet files."""
    print("[Example] Generating Characterization Report...")

    # Load data
    data = {
        'ptr': pl.read_parquet(parquet_dir / "*.ptr.parquet"),
        'mir': pl.read_parquet(parquet_dir / "*.mir.parquet"),
    }

    parameter = {
        'CUT': '44E',
        'FLOW': 'CHAR',
        'LOT': 'LOT123',
        'WAFER': 'W01',
        'temperature': 25,
        'TYPE': 'CHAR'
    }

    # Generate report
    generator = CharReportGenerator()
    report_path = generator.generate(
        parameter=parameter,
        data=data,
        output_path=output_dir
    )

    print(f"✓ Characterization report generated: {report_path}")
    return report_path


def generate_all_reports(parquet_dir: Path, output_dir: Path):
    """Generate all report types."""
    print("\n" + "="*60)
    print("Generating All Reports")
    print("="*60 + "\n")

    reports = []

    try:
        reports.append(generate_condition_report(parquet_dir, output_dir))
    except Exception as e:
        print(f"✗ Condition report failed: {e}")

    try:
        reports.append(generate_yield_report(parquet_dir, output_dir))
    except Exception as e:
        print(f"✗ Yield report failed: {e}")

    try:
        reports.append(generate_volume_report(parquet_dir, output_dir))
    except Exception as e:
        print(f"✗ Volume report failed: {e}")

    try:
        reports.append(generate_char_report(parquet_dir, output_dir))
    except Exception as e:
        print(f"✗ Characterization report failed: {e}")

    print(f"\n✓ Generated {len(reports)} reports")
    print("\nReport files:")
    for report in reports:
        print(f"  - {report}")


if __name__ == "__main__":
    # Example usage
    parquet_dir = Path("./data/parquet")
    output_dir = Path("./output/reports")

    # Ensure directories exist
    output_dir.mkdir(parents=True, exist_ok=True)

    # Generate all reports
    generate_all_reports(parquet_dir, output_dir)
