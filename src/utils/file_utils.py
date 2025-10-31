"""File utility functions"""

import hashlib
import os
from pathlib import Path
from typing import Optional


def ensure_directory(path: Path) -> Path:
    """Ensure directory exists, create if needed."""
    path = Path(path)
    path.mkdir(parents=True, exist_ok=True)
    return path


def get_file_checksum(file_path: Path, algorithm: str = "md5") -> str:
    """Calculate file checksum."""
    hash_func = hashlib.new(algorithm)
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            hash_func.update(chunk)
    return hash_func.hexdigest()


def is_file_locked(file_path: Path) -> bool:
    """Check if file is locked by another process."""
    try:
        with open(file_path, "a"):
            return False
    except IOError:
        return True


def safe_remove_file(file_path: Path) -> bool:
    """Safely remove file if it exists."""
    try:
        if Path(file_path).exists():
            Path(file_path).unlink()
        return True
    except Exception:
        return False
