"""
ART.stdf - ConvertSTDFUseCase Tests

Unit tests for STDF conversion use case.

Author: Matteo Terranova (matteo.terranova@st.com)
Organization: STMicroelectronics - MDRF GPAM
"""

import pytest
from pathlib import Path
from unittest.mock import Mock, MagicMock

from src.application.use_cases.convert_stdf_use_case import ConvertSTDFUseCase
from src.domain.models.parameter import Parameter


@pytest.mark.unit
class TestConvertSTDFUseCase:
    """Test cases for ConvertSTDFUseCase."""

    def test_execute_success(self, sample_parameter_object, mock_stdf_parser, test_logger, temp_dir):
        """Test successful STDF conversion."""
        # Arrange
        # Create a temporary STDF file
        stdf_path = temp_dir / "test.std"
        stdf_path.touch()

        use_case = ConvertSTDFUseCase(
            stdf_parser=mock_stdf_parser,
            logger=test_logger
        )

        # Act
        result = use_case.execute(
            stdf_path=str(stdf_path),
            parameter=sample_parameter_object
        )

        # Assert
        assert result is not None
        assert isinstance(result, dict)
        mock_stdf_parser.parse_to_parquet.assert_called_once()

    def test_execute_with_compression(self, sample_parameter_object, mock_stdf_parser, test_logger, temp_dir):
        """Test STDF conversion with custom compression."""
        # Arrange
        # Create a temporary STDF file
        stdf_path = temp_dir / "test.std"
        stdf_path.touch()

        use_case = ConvertSTDFUseCase(
            stdf_parser=mock_stdf_parser,
            logger=test_logger
        )

        # Act
        result = use_case.execute(
            stdf_path=str(stdf_path),
            parameter=sample_parameter_object,
            compression="zstd"
        )

        # Assert
        assert result is not None
        call_args = mock_stdf_parser.parse_to_parquet.call_args
        assert call_args.kwargs.get("compression") == "zstd"

    def test_execute_handles_parser_error(self, sample_parameter_object, mock_stdf_parser, test_logger, temp_dir):
        """Test that use case handles parser errors gracefully."""
        # Arrange
        # Create a temporary STDF file
        stdf_path = temp_dir / "test.std"
        stdf_path.touch()

        mock_stdf_parser.parse_to_parquet.side_effect = Exception("Parser error")

        use_case = ConvertSTDFUseCase(
            stdf_parser=mock_stdf_parser,
            logger=test_logger
        )

        # Act & Assert
        with pytest.raises(Exception) as exc_info:
            use_case.execute(
                stdf_path=str(stdf_path),
                parameter=sample_parameter_object
            )

        assert "Parser error" in str(exc_info.value)

    def test_execute_without_parameter(self, mock_stdf_parser, test_logger, temp_dir):
        """Test STDF conversion without parameter object."""
        # Arrange
        # Create a temporary STDF file
        stdf_path = temp_dir / "test.std"
        stdf_path.touch()

        use_case = ConvertSTDFUseCase(
            stdf_parser=mock_stdf_parser,
            logger=test_logger
        )

        # Act
        result = use_case.execute(
            stdf_path=str(stdf_path)
        )

        # Assert
        assert result is not None
        mock_stdf_parser.parse_to_parquet.assert_called_once()

    def test_default_dependencies(self):
        """Test that use case creates default dependencies."""
        # Act
        use_case = ConvertSTDFUseCase()

        # Assert
        assert use_case.stdf_parser is not None
        assert use_case.logger is not None
