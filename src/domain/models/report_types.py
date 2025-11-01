"""
ART.stdf - Report Type Enumerations

Defines all report types and processing types used in the system.

Author: Matteo Terranova (matteo.terranova@st.com)
Organization: STMicroelectronics - MDRF GPAM
"""

from enum import Enum


class ProcessType(Enum):
    """
    Enumeration for different processing types.

    Attributes:
        STDF2DATA: STDF file to Parquet conversion
        DATA2REPORT: Parquet data to report generation
        CONDITION2REPORT: Condition file to report generation
        SHMOO: Shmoo plot processing
        CHAR: Characterization processing
    """

    STDF2DATA = "stdf2data"
    DATA2REPORT = "data2report"
    CONDITION2REPORT = "condition2report"
    SHMOO = "shmoo"
    CHAR = "char"


class ReportType(Enum):
    """
    Enumeration for report types.

    Attributes:
        LOOP: Stability report (30 test loops)
        VOLUME: Volume validation report
        TTIME: Test time analysis report
        YIELD: Yield analysis report
        CONDITION: Test condition report
        CHAR: Characterization report
        X30: Special stability type
    """

    LOOP = "LOOP"
    VOLUME = "VOLUME"
    TTIME = "TTIME"
    YIELD = "YIELD"
    CONDITION = "CONDITION"
    CHAR = "CHAR"
    X30 = "X30"


class FlowType(Enum):
    """
    Enumeration for flow types.

    Attributes:
        EWS1: Engineering Wafer Sort 1
        EWS2: Engineering Wafer Sort 2
        EWS3: Engineering Wafer Sort 3
        EWSDIE: Engineering Wafer Sort Die
        FT: Final Test
        FT1: Final Test 1
        FT2: Final Test 2
        EWSCHAR: Engineering Wafer Sort Characterization
    """

    EWS1 = "EWS1"
    EWS2 = "EWS2"
    EWS3 = "EWS3"
    EWSDIE = "EWSDIE"
    FT = "FT"
    FT1 = "FT1"
    FT2 = "FT2"
    EWSCHAR = "EWSCHAR"

    @classmethod
    def is_ews_flow(cls, flow: str) -> bool:
        """
        Check if a flow is an EWS (Engineering Wafer Sort) flow.

        Args:
            flow: Flow name to check

        Returns:
            True if flow starts with "EWS", False otherwise

        Example:
            >>> FlowType.is_ews_flow("EWS1")
            True
            >>> FlowType.is_ews_flow("FT")
            False
        """
        return flow.upper().startswith("EWS")


class PackageType(Enum):
    """
    Enumeration for package types.

    Attributes:
        QFP: Quad Flat Package
        QFN: Quad Flat No-lead
        DIP: Dual In-line Package
        WLCSP: Wafer Level Chip Scale Package
        CSP: Chip Scale Package
        BGA: Ball Grid Array
    """

    QFP = "QFP"
    QFN = "QFN"
    DIP = "DIP"
    WLCSP = "WLCSP"
    CSP = "CSP"
    BGA = "BGA"
