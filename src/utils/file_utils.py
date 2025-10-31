"""File utility functions for ART.stdf"""

import hashlib
import os
import shutil
from pathlib import Path
from typing import Optional, List
import time


def ensure_directory(path: Path) -> Path:
    """
    Ensure directory exists, create if needed.

    Args:
        path: Directory path to ensure

    Returns:
        Path object of created/existing directory
    """
    path = Path(path)
    path.mkdir(parents=True, exist_ok=True)
    return path


def get_file_checksum(file_path: Path, algorithm: str = "md5") -> str:
    """
    Calculate file checksum using specified algorithm.

    Args:
        file_path: Path to file
        algorithm: Hash algorithm (md5, sha1, sha256)

    Returns:
        Hex digest of file hash
    """
    hash_func = hashlib.new(algorithm)
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            hash_func.update(chunk)
    return hash_func.hexdigest()


def is_file_locked(file_path: Path) -> bool:
    """
    Check if file is locked by another process.

    Args:
        file_path: Path to file to check

    Returns:
        True if file is locked, False otherwise
    """
    try:
        with open(file_path, "a"):
            return False
    except IOError:
        return True


def safe_remove_file(file_path: Path) -> bool:
    """
    Safely remove file if it exists.

    Args:
        file_path: Path to file to remove

    Returns:
        True if removed successfully, False otherwise
    """
    try:
        if Path(file_path).exists():
            Path(file_path).unlink()
        return True
    except Exception:
        return False


def safe_remove_directory(dir_path: Path, recursive: bool = True) -> bool:
    """
    Safely remove directory and optionally its contents.

    Args:
        dir_path: Path to directory
        recursive: If True, remove all contents recursively

    Returns:
        True if removed successfully, False otherwise
    """
    try:
        dir_path = Path(dir_path)
        if not dir_path.exists():
            return True

        if recursive:
            shutil.rmtree(dir_path, ignore_errors=True)
        else:
            dir_path.rmdir()
        return True
    except Exception as e:
        print(f"Error removing directory {dir_path}: {e}")
        return False


def get_file_size_mb(file_path: Path) -> float:
    """Get file size in megabytes."""
    return Path(file_path).stat().st_size / (1024 * 1024)


def get_directory_size_mb(dir_path: Path) -> float:
    """Get total size of directory in MB."""
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(dir_path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            if os.path.exists(fp):
                total_size += os.path.getsize(fp)
    return total_size / (1024 * 1024)


def find_files_by_extension(
    directory: Path,
    extensions: List[str],
    recursive: bool = True
) -> List[Path]:
    """Find all files with specified extensions."""
    directory = Path(directory)
    files = []

    if recursive:
        for ext in extensions:
            files.extend(directory.rglob(f"*{ext}"))
    else:
        for ext in extensions:
            files.extend(directory.glob(f"*{ext}"))

    return sorted(files)


def wait_for_file_stable(
    file_path: Path,
    timeout: int = 60,
    check_interval: float = 0.5
) -> bool:
    """Wait for file to stop growing (stable size)."""
    file_path = Path(file_path)
    if not file_path.exists():
        return False

    start_time = time.time()
    last_size = -1

    while time.time() - start_time < timeout:
        current_size = file_path.stat().st_size

        if current_size == last_size and current_size > 0:
            return True

        last_size = current_size
        time.sleep(check_interval)

    return False


def copy_file_safe(src: Path, dst: Path, overwrite: bool = False) -> bool:
    """Safely copy file with optional overwrite protection."""
    try:
        src = Path(src)
        dst = Path(dst)

        if not src.exists():
            return False

        if dst.exists() and not overwrite:
            return False

        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dst)
        return True
    except Exception as e:
        print(f"Error copying file: {e}")
        return False


def move_file_safe(src: Path, dst: Path, overwrite: bool = False) -> bool:
    """Safely move file with optional overwrite protection."""
    try:
        src = Path(src)
        dst = Path(dst)

        if not src.exists():
            return False

        if dst.exists() and not overwrite:
            return False

        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(src), str(dst))
        return True
    except Exception as e:
        print(f"Error moving file: {e}")
        return False


def get_temp_directory(base_name: str = "art_tmp") -> Path:
    """Create a temporary directory with unique name."""
    import tempfile
    temp_dir = Path(tempfile.mkdtemp(prefix=f"{base_name}_"))
    return temp_dir


def cleanup_old_files(
    directory: Path,
    age_days: int = 7,
    extensions: Optional[List[str]] = None
) -> int:
    """Remove files older than specified days."""
    directory = Path(directory)
    if not directory.exists():
        return 0

    current_time = time.time()
    cutoff_time = current_time - (age_days * 86400)
    removed_count = 0

    for file_path in directory.rglob("*"):
        if not file_path.is_file():
            continue

        if extensions and file_path.suffix not in extensions:
            continue

        if file_path.stat().st_mtime < cutoff_time:
            try:
                file_path.unlink()
                removed_count += 1
            except Exception:
                pass

    return removed_count
