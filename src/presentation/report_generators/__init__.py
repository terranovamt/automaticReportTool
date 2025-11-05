"""
ART.stdf - Report Generators Package

Pure Python HTML report generators that replace Jupyter notebooks.

Available Generators:
- VolumeReportGenerator: Standard volume production reports
- LoopReportGenerator: Loop test reports
- TTimeReportGenerator: Test time analysis reports
- YieldReportGenerator: Yield analysis reports
- ConditionReportGenerator: Test condition reports

Author: Matteo Terranova (matteo.terranova@st.com)
Organization: STMicroelectronics - MDRF GPAM
"""

from src.presentation.report_generators.base_report_generator import BaseReportGenerator

__all__ = [
    "BaseReportGenerator",
]
