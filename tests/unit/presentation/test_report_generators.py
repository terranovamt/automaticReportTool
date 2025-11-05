"""
ART.stdf - Report Generator Tests

Unit tests for report generator factory and base class.

Author: Matteo Terranova (matteo.terranova@st.com)
Organization: STMicroelectronics - MDRF GPAM
"""

import pytest
from pathlib import Path

from src.presentation.report_generators import (
    create_report_generator,
    VolumeReportGenerator,
    LoopReportGenerator,
    TTimeReportGenerator,
    YieldReportGenerator,
    ConditionReportGenerator,
)


@pytest.mark.unit
class TestReportGeneratorFactory:
    """Test cases for report generator factory function."""

    def test_create_volume_generator(self, sample_parameter_object):
        """Test creating VOLUME report generator."""
        generator = create_report_generator("VOLUME", sample_parameter_object)

        assert isinstance(generator, VolumeReportGenerator)
        assert generator.get_report_type() == "VOLUME"

    def test_create_loop_generator(self, sample_parameter_object):
        """Test creating LOOP report generator."""
        generator = create_report_generator("LOOP", sample_parameter_object)

        assert isinstance(generator, LoopReportGenerator)
        assert generator.get_report_type() == "LOOP"

    def test_create_ttime_generator(self, sample_parameter_object):
        """Test creating TTIME report generator."""
        generator = create_report_generator("TTIME", sample_parameter_object)

        assert isinstance(generator, TTimeReportGenerator)
        assert generator.get_report_type() == "TTIME"

    def test_create_yield_generator(self, sample_parameter_object):
        """Test creating YIELD report generator."""
        generator = create_report_generator("YIELD", sample_parameter_object)

        assert isinstance(generator, YieldReportGenerator)
        assert generator.get_report_type() == "YIELD"

    def test_create_condition_generator(self, sample_parameter_object):
        """Test creating CONDITION report generator."""
        generator = create_report_generator("CONDITION", sample_parameter_object)

        assert isinstance(generator, ConditionReportGenerator)
        assert generator.get_report_type() == "CONDITION"

    def test_create_generator_case_insensitive(self, sample_parameter_object):
        """Test that factory is case-insensitive."""
        generator1 = create_report_generator("volume", sample_parameter_object)
        generator2 = create_report_generator("VOLUME", sample_parameter_object)
        generator3 = create_report_generator("Volume", sample_parameter_object)

        assert type(generator1) == type(generator2) == type(generator3)

    def test_create_generator_invalid_type(self, sample_parameter_object):
        """Test that invalid report type raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            create_report_generator("INVALID", sample_parameter_object)

        assert "Unknown report type" in str(exc_info.value)
        assert "INVALID" in str(exc_info.value)

    def test_create_generator_with_logger(self, sample_parameter_object, test_logger):
        """Test creating generator with custom logger."""
        generator = create_report_generator(
            "VOLUME",
            sample_parameter_object,
            logger=test_logger
        )

        assert generator.logger == test_logger


@pytest.mark.unit
class TestBaseReportGenerator:
    """Test cases for BaseReportGenerator functionality."""

    def test_build_html_header(self, sample_parameter_object):
        """Test HTML header generation."""
        generator = VolumeReportGenerator(sample_parameter_object)

        header = generator.build_html_header("Test Title")

        assert "<!DOCTYPE html>" in header
        assert "<html" in header
        assert "Test Title" in header
        assert "STMicroelectronics" in header or "st.com" in header

    def test_build_html_footer(self, sample_parameter_object):
        """Test HTML footer generation."""
        generator = VolumeReportGenerator(sample_parameter_object)

        footer = generator.build_html_footer()

        assert "</body>" in footer
        assert "</html>" in footer

    def test_build_report_info_table(self, sample_parameter_object):
        """Test report info table generation."""
        generator = VolumeReportGenerator(sample_parameter_object)

        table = generator.build_report_info_table()

        assert "<table" in table
        assert sample_parameter_object.flow in table
        assert sample_parameter_object.lot in table
        assert sample_parameter_object.wafer in table

    def test_save_report_creates_directory(self, sample_parameter_object, temp_dir):
        """Test that save_report creates parent directories."""
        generator = VolumeReportGenerator(sample_parameter_object)

        report_path = temp_dir / "new" / "nested" / "report.html"
        html_content = "<html><body>Test</body></html>"

        # Act
        generator.save_report(html_content, report_path)

        # Assert
        assert report_path.exists()
        assert report_path.read_text() == html_content

    def test_get_template_content_missing(self, sample_parameter_object, test_logger):
        """Test that missing template returns placeholder."""
        generator = VolumeReportGenerator(sample_parameter_object, logger=test_logger)

        content = generator.get_template_content("nonexistent.html")

        assert "not found" in content.lower()

    def test_get_product_image_missing(self, sample_parameter_object):
        """Test that missing product image returns empty string."""
        generator = VolumeReportGenerator(sample_parameter_object)

        image = generator.get_product_image("NONEXISTENT_CODE")

        assert image == ""


@pytest.mark.unit
class TestVolumeReportGenerator:
    """Test cases specific to VolumeReportGenerator."""

    def test_get_report_type(self, sample_parameter_object):
        """Test report type identification."""
        generator = VolumeReportGenerator(sample_parameter_object)

        assert generator.get_report_type() == "VOLUME"

    def test_generator_initialization(self, sample_parameter_object):
        """Test generator initialization."""
        generator = VolumeReportGenerator(sample_parameter_object)

        assert generator.parameter == sample_parameter_object
        assert generator.logger is not None
        assert generator.template_dir is not None
