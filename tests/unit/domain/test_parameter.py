"""
ART.stdf - Parameter Model Tests

Unit tests for the Parameter domain model.

Author: Matteo Terranova (matteo.terranova@st.com)
Organization: STMicroelectronics - MDRF GPAM
"""

import pytest
from src.domain.models.parameter import Parameter, FileCorner


@pytest.mark.unit
class TestParameter:
    """Test cases for Parameter domain model."""

    def test_parameter_creation(self, sample_parameter_object):
        """Test basic Parameter object creation."""
        param = sample_parameter_object

        assert param.code == "44E"
        assert param.cut == "44EZ"
        assert param.flow == "EWSCHAR"
        assert param.type == "CHAR"
        assert param.lot == "Q445172"
        assert param.wafer == "05"

    def test_parameter_with_file_corner(self):
        """Test Parameter with FileCorner."""
        file_corner = FileCorner(
            corner="TTTT",
            path="/test/path"
        )

        param = Parameter(
            code="123",
            cut="123A",
            flow="EWS1",
            type="VOLUME",
            lot="L001",
            wafer="01",
            file={"01": file_corner}
        )

        assert "01" in param.file
        assert param.file["01"].corner == "TTTT"
        assert param.file["01"].path == "/test/path"

    def test_parameter_to_dict(self, sample_parameter_object):
        """Test conversion to dictionary (legacy format)."""
        param_dict = sample_parameter_object.to_dict()

        assert isinstance(param_dict, dict)
        assert param_dict["CODE"] == "44E"
        assert param_dict["CUT"] == "44EZ"
        assert param_dict["FLOW"] == "EWSCHAR"
        assert param_dict["TYPE"] == "CHAR"
        assert param_dict["LOT"] == "Q445172"
        assert param_dict["WAFER"] == "05"
        assert "FILE" in param_dict

    def test_parameter_from_dict(self, sample_parameter):
        """Test creation from dictionary (legacy format)."""
        param = Parameter.from_dict(sample_parameter)

        assert param.code == sample_parameter["CODE"]
        assert param.cut == sample_parameter["CUT"]
        assert param.flow == sample_parameter["FLOW"]
        assert param.type == sample_parameter["TYPE"]

    def test_is_ews_flow(self):
        """Test EWS flow detection."""
        # EWS flows
        for flow in ["EWS1", "EWS2", "EWSCHAR", "EWSDIE"]:
            param = Parameter(
                code="123", cut="123A", flow=flow, type="TEST",
                lot="L001", wafer="01"
            )
            assert param.is_ews_flow is True

        # Non-EWS flows
        for flow in ["FT", "FT1", "FT2"]:
            param = Parameter(
                code="123", cut="123A", flow=flow, type="TEST",
                lot="L001", wafer="01"
            )
            assert param.is_ews_flow is False

    def test_title_property(self):
        """Test title generation."""
        param = Parameter(
            code="44E",
            cut="44EZ",
            flow="EWSCHAR",
            type="CHAR",
            lot="Q445172",
            wafer="05"
        )

        title = param.title
        assert "Q445172" in title
        assert "05" in title

    def test_com_property(self):
        """Test COM property generation."""
        param = Parameter(
            code="44E",
            cut="44EZ",
            flow="EWSCHAR",
            type="CHAR",
            lot="Q445172",
            wafer="05"
        )

        # COM should be wafer-specific for non-CHAR
        param.type = "VOLUME"
        assert param.wafer in param.com

    def test_parameter_equality(self):
        """Test Parameter equality comparison."""
        param1 = Parameter(
            code="123", cut="123A", flow="EWS1", type="VOLUME",
            lot="L001", wafer="01"
        )
        param2 = Parameter(
            code="123", cut="123A", flow="EWS1", type="VOLUME",
            lot="L001", wafer="01"
        )
        param3 = Parameter(
            code="456", cut="456A", flow="FT", type="VOLUME",
            lot="L002", wafer="02"
        )

        assert param1 == param2
        assert param1 != param3

    def test_parameter_serialization_roundtrip(self, sample_parameter_object):
        """Test that Parameter can be converted to dict and back."""
        original = sample_parameter_object

        # Convert to dict
        param_dict = original.to_dict()

        # Convert back to Parameter
        restored = Parameter.from_dict(param_dict)

        # Check equality
        assert restored.code == original.code
        assert restored.cut == original.cut
        assert restored.flow == original.flow
        assert restored.type == original.type
        assert restored.lot == original.lot
        assert restored.wafer == original.wafer


@pytest.mark.unit
class TestFileCorner:
    """Test cases for FileCorner model."""

    def test_file_corner_creation(self):
        """Test FileCorner creation."""
        fc = FileCorner(corner="TTTT", path="/test/path")

        assert fc.corner == "TTTT"
        assert fc.path == "/test/path"

    def test_file_corner_to_dict(self):
        """Test FileCorner to dict conversion."""
        fc = FileCorner(corner="SSTT", path="/another/path")
        fc_dict = fc.to_dict()

        assert fc_dict["corner"] == "SSTT"
        assert fc_dict["path"] == "/another/path"

    def test_file_corner_from_dict(self):
        """Test FileCorner from dict creation."""
        fc_dict = {"corner": "FFTT", "path": "/third/path"}
        fc = FileCorner.from_dict(fc_dict)

        assert fc.corner == "FFTT"
        assert fc.path == "/third/path"
