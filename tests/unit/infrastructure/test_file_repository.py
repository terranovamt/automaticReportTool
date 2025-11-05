"""
ART.stdf - FileRepository Tests

Unit tests for FileRepository infrastructure.

Author: Matteo Terranova (matteo.terranova@st.com)
Organization: STMicroelectronics - MDRF GPAM
"""

import pytest
from pathlib import Path

from src.infrastructure.storage.file_repository import FileRepository


@pytest.mark.unit
class TestFileRepository:
    """Test cases for FileRepository."""

    def test_check_completion_marker_exists(self, temp_dir):
        """Test checking for existing completion marker."""
        # Arrange
        marker_file = temp_dir / ".DONE"
        marker_file.touch()

        # Act
        result = FileRepository.check_completion_marker(str(temp_dir))

        # Assert
        assert result is True

    def test_check_completion_marker_not_exists(self, temp_dir):
        """Test checking for non-existent completion marker."""
        # Act
        result = FileRepository.check_completion_marker(str(temp_dir))

        # Assert
        assert result is False

    def test_check_completion_marker_custom_name(self, temp_dir):
        """Test checking for custom marker name."""
        # Arrange
        marker_file = temp_dir / ".CUSTOM_MARKER"
        marker_file.touch()

        # Act
        result = FileRepository.check_completion_marker(
            str(temp_dir),
            marker_name=".CUSTOM_MARKER"
        )

        # Assert
        assert result is True

    def test_create_completion_marker(self, temp_dir):
        """Test creating a completion marker."""
        # Act
        FileRepository.create_completion_marker(str(temp_dir))

        # Assert
        marker_file = temp_dir / ".DONE"
        assert marker_file.exists()

    def test_create_completion_marker_with_content(self, temp_dir):
        """Test creating marker with custom content."""
        # Act
        file_count = 5
        FileRepository.create_completion_marker(
            str(temp_dir),
            file_count=file_count
        )

        # Assert
        marker_file = temp_dir / ".DONE"
        assert marker_file.exists()
        content = marker_file.read_text()
        assert str(file_count) in content

    def test_remove_completion_marker(self, temp_dir):
        """Test removing a completion marker."""
        # Arrange
        marker_file = temp_dir / ".DONE"
        marker_file.touch()
        assert marker_file.exists()

        # Act
        FileRepository.remove_completion_marker(str(temp_dir))

        # Assert
        assert not marker_file.exists()

    def test_remove_non_existent_marker(self, temp_dir):
        """Test removing non-existent marker doesn't raise error."""
        # Act & Assert (should not raise)
        FileRepository.remove_completion_marker(str(temp_dir))

    def test_find_files_by_pattern(self, temp_dir):
        """Test finding files by pattern."""
        # Arrange
        (temp_dir / "test1.std").touch()
        (temp_dir / "test2.std").touch()
        (temp_dir / "other.txt").touch()

        # Act
        std_files = FileRepository.find_files(str(temp_dir), "*.std")

        # Assert
        assert len(std_files) == 2
        assert all(f.suffix == ".std" for f in std_files)

    def test_find_files_recursive(self, temp_dir):
        """Test finding files recursively."""
        # Arrange
        subdir = temp_dir / "subdir"
        subdir.mkdir()
        (temp_dir / "test1.std").touch()
        (subdir / "test2.std").touch()

        # Act
        std_files = FileRepository.find_files(str(temp_dir), "**/*.std")

        # Assert
        assert len(std_files) == 2

    def test_ensure_directory_creates_new(self, temp_dir):
        """Test that ensure_directory creates new directories."""
        # Arrange
        new_dir = temp_dir / "new" / "nested" / "dir"

        # Act
        FileRepository.ensure_directory(str(new_dir))

        # Assert
        assert new_dir.exists()
        assert new_dir.is_dir()

    def test_ensure_directory_idempotent(self, temp_dir):
        """Test that ensure_directory is idempotent."""
        # Arrange
        test_dir = temp_dir / "test"
        test_dir.mkdir()

        # Act & Assert (should not raise)
        FileRepository.ensure_directory(str(test_dir))
        assert test_dir.exists()
