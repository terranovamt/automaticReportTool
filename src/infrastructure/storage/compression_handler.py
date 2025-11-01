"""
ART.stdf - Compression Handler

Handles compression and decompression of STDF files in various formats.
Extracted and refactored from original stdf2data.py module.

Author: Matteo Terranova (matteo.terranova@st.com)
Organization: STMicroelectronics - MDRF GPAM
"""

import os
import gzip
import shutil
import subprocess
import zipfile
import tarfile
from pathlib import Path
from typing import Optional

try:
    import py7zr
    HAS_PY7ZR = True
except ImportError:
    HAS_PY7ZR = False

from config.settings import settings


class CompressionHandler:
    """
    Handler for file compression and decompression operations.

    Supports multiple compression formats:
    - gzip (.gz)
    - 7z (.7z)
    - zip (.zip)
    - bzip2 (.bz2)
    - xz (.xz)
    - tar (.tar, .tar.gz, .tar.bz2, .tar.xz)
    - rar (.rar)
    """

    SUPPORTED_EXTENSIONS = settings.COMPRESSION_EXTENSIONS

    @staticmethod
    def is_compressed(file_path: str) -> bool:
        """
        Check if a file is compressed based on its extension.

        Args:
            file_path: Path to the file to check

        Returns:
            True if file appears to be compressed, False otherwise

        Example:
            >>> CompressionHandler.is_compressed("file.std.gz")
            True
            >>> CompressionHandler.is_compressed("file.std")
            False
        """
        file_path_lower = file_path.lower()
        return any(file_path_lower.endswith(ext) for ext in CompressionHandler.SUPPORTED_EXTENSIONS)

    @staticmethod
    def decompress_file(
        compressed_path: str,
        output_dir: str,
        remove_compressed: bool = False
    ) -> Optional[Path]:
        """
        Decompress a file to the specified output directory.

        Args:
            compressed_path: Path to compressed file
            output_dir: Directory where file should be extracted
            remove_compressed: Whether to remove compressed file after extraction

        Returns:
            Path to decompressed file, or None if decompression failed

        Raises:
            ValueError: If file format is not supported

        Example:
            >>> path = CompressionHandler.decompress_file(
            ...     "file.std.gz",
            ...     "/tmp/output"
            ... )
        """
        compressed_path = Path(compressed_path)
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        filename = compressed_path.name.lower()

        try:
            # Handle different compression formats
            if filename.endswith(".zip"):
                extracted = CompressionHandler._decompress_zip(compressed_path, output_dir)

            elif filename.endswith(".gz"):
                extracted = CompressionHandler._decompress_gzip(compressed_path, output_dir)

            elif filename.endswith((".tar", ".tar.gz", ".tar.bz2", ".tar.xz")):
                extracted = CompressionHandler._decompress_tar(compressed_path, output_dir)

            elif filename.endswith(".7z"):
                extracted = CompressionHandler._decompress_7z(compressed_path, output_dir)

            elif filename.endswith(".rar"):
                extracted = CompressionHandler._decompress_rar(compressed_path, output_dir)

            elif filename.endswith(".bz2"):
                extracted = CompressionHandler._decompress_bz2(compressed_path, output_dir)

            elif filename.endswith(".xz"):
                extracted = CompressionHandler._decompress_xz(compressed_path, output_dir)

            else:
                raise ValueError(f"Unsupported compression format: {filename}")

            # Remove compressed file if requested
            if remove_compressed and extracted:
                compressed_path.unlink()

            return extracted

        except Exception as e:
            print(f"Error decompressing {compressed_path}: {e}")
            return None

    @staticmethod
    def _decompress_zip(compressed_path: Path, output_dir: Path) -> Optional[Path]:
        """Decompress ZIP file."""
        with zipfile.ZipFile(compressed_path, "r") as zip_ref:
            zip_ref.extractall(output_dir)

        # Find the extracted STDF file
        return CompressionHandler._find_stdf_file(output_dir)

    @staticmethod
    def _decompress_gzip(compressed_path: Path, output_dir: Path) -> Optional[Path]:
        """Decompress GZIP file."""
        output_filename = compressed_path.stem  # Remove .gz extension
        output_path = output_dir / output_filename

        with gzip.open(compressed_path, "rb") as gz_file:
            with open(output_path, "wb") as output_file:
                shutil.copyfileobj(gz_file, output_file)

        return output_path

    @staticmethod
    def _decompress_tar(compressed_path: Path, output_dir: Path) -> Optional[Path]:
        """Decompress TAR file (including .tar.gz, .tar.bz2, .tar.xz)."""
        with tarfile.open(compressed_path, "r:*") as tar_ref:
            tar_ref.extractall(output_dir)

        return CompressionHandler._find_stdf_file(output_dir)

    @staticmethod
    def _decompress_7z(compressed_path: Path, output_dir: Path) -> Optional[Path]:
        """Decompress 7Z file."""
        if HAS_PY7ZR:
            # Use py7zr library if available
            with py7zr.SevenZipFile(compressed_path, mode="r") as archive:
                archive.extractall(output_dir)
        else:
            # Fall back to command line 7z
            subprocess.run(
                f'7z x "{compressed_path}" -o"{output_dir}"',
                shell=True,
                check=True,
                stderr=subprocess.DEVNULL
            )

        return CompressionHandler._find_stdf_file(output_dir)

    @staticmethod
    def _decompress_rar(compressed_path: Path, output_dir: Path) -> Optional[Path]:
        """Decompress RAR file using command line tool."""
        subprocess.run(
            f'unrar x "{compressed_path}" "{output_dir}/"',
            shell=True,
            check=True,
            stderr=subprocess.DEVNULL
        )

        return CompressionHandler._find_stdf_file(output_dir)

    @staticmethod
    def _decompress_bz2(compressed_path: Path, output_dir: Path) -> Optional[Path]:
        """Decompress BZ2 file using command line tool."""
        # Copy file to output directory first
        temp_path = output_dir / compressed_path.name
        shutil.copy2(compressed_path, temp_path)

        # Decompress in place
        subprocess.run(
            f'bzip2 -dk "{temp_path}"',
            shell=True,
            check=True
        )

        return CompressionHandler._find_stdf_file(output_dir)

    @staticmethod
    def _decompress_xz(compressed_path: Path, output_dir: Path) -> Optional[Path]:
        """Decompress XZ file using command line tool."""
        # Copy file to output directory first
        temp_path = output_dir / compressed_path.name
        shutil.copy2(compressed_path, temp_path)

        # Decompress in place
        subprocess.run(
            f'xz -dk "{temp_path}"',
            shell=True,
            check=True
        )

        return CompressionHandler._find_stdf_file(output_dir)

    @staticmethod
    def _find_stdf_file(directory: Path) -> Optional[Path]:
        """
        Find an STDF file in a directory.

        Args:
            directory: Directory to search

        Returns:
            Path to STDF file, or None if not found
        """
        for ext in settings.STDF_EXTENSIONS:
            for file in directory.glob(f"*{ext}"):
                return file

        return None

    @staticmethod
    def compress_file(
        input_path: str,
        output_path: Optional[str] = None,
        compression_format: str = "gz",
        remove_original: bool = False
    ) -> Optional[Path]:
        """
        Compress a file.

        Args:
            input_path: Path to file to compress
            output_path: Optional output path (default: input_path + extension)
            compression_format: Compression format (gz, 7z, zip, etc.)
            remove_original: Whether to remove original file after compression

        Returns:
            Path to compressed file, or None if compression failed

        Example:
            >>> path = CompressionHandler.compress_file(
            ...     "file.std",
            ...     compression_format="gz"
            ... )
        """
        input_path = Path(input_path)

        if output_path is None:
            output_path = Path(str(input_path) + f".{compression_format}")
        else:
            output_path = Path(output_path)

        try:
            if compression_format == "gz":
                with open(input_path, "rb") as f_in:
                    with gzip.open(output_path, "wb") as f_out:
                        shutil.copyfileobj(f_in, f_out)

            elif compression_format == "zip":
                with zipfile.ZipFile(output_path, "w", zipfile.ZIP_DEFLATED) as zipf:
                    zipf.write(input_path, input_path.name)

            else:
                raise ValueError(f"Unsupported compression format: {compression_format}")

            # Remove original if requested
            if remove_original:
                input_path.unlink()

            return output_path

        except Exception as e:
            print(f"Error compressing {input_path}: {e}")
            return None
