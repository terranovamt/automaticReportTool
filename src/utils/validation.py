"""Validation utilities"""

from pathlib import Path
import polars as pl
from src.core.exceptions import ValidationError
from src.core.constants import STDF_EXTENSIONS


def validate_stdf_file(file_path: Path) -> bool:
    """Validate STDF file exists and has correct extension."""
    if not file_path.exists():
        raise ValidationError(f"File not found: {file_path}")
    if not any(str(file_path).lower().endswith(ext) for ext in STDF_EXTENSIONS):
        raise ValidationError(f"Invalid STDF extension: {file_path}")
    return True


def validate_parameter(parameter: dict) -> bool:
    """Validate parameter dictionary has required fields."""
    required = ["CUT", "FLOW", "LOT", "WAFER", "TYPE"]
    for field in required:
        if field not in parameter or not parameter[field]:
            raise ValidationError(f"Missing required field: {field}")
    return True


def validate_dataframe(df: pl.DataFrame, required_columns: list) -> bool:
    """Validate DataFrame has required columns."""
    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        raise ValidationError(f"Missing columns: {missing}")
    return True
