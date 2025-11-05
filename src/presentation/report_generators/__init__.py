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

from typing import Optional
import logging

from src.domain.models.parameter import Parameter
from src.presentation.report_generators.base_report_generator import BaseReportGenerator
from src.presentation.report_generators.volume_report_generator import VolumeReportGenerator
from src.presentation.report_generators.loop_report_generator import LoopReportGenerator
from src.presentation.report_generators.ttime_report_generator import TTimeReportGenerator
from src.presentation.report_generators.yield_report_generator import YieldReportGenerator
from src.presentation.report_generators.condition_report_generator import ConditionReportGenerator


def create_report_generator(
    report_type: str,
    parameter: Parameter,
    template_dir: Optional[str] = None,
    logger: Optional[logging.Logger] = None
) -> BaseReportGenerator:
    """
    Factory function to create report generators.

    Args:
        report_type: Type of report ("VOLUME", "LOOP", "TTIME", "YIELD", "CONDITION")
        parameter: Parameter object with report metadata
        template_dir: Directory containing HTML templates
        logger: Logger instance

    Returns:
        Appropriate report generator instance

    Raises:
        ValueError: If report_type is not recognized
    """
    report_type_upper = report_type.upper()

    generators = {
        "VOLUME": VolumeReportGenerator,
        "LOOP": LoopReportGenerator,
        "TTIME": TTimeReportGenerator,
        "YIELD": YieldReportGenerator,
        "CONDITION": ConditionReportGenerator,
    }

    generator_class = generators.get(report_type_upper)

    if generator_class is None:
        raise ValueError(
            f"Unknown report type: {report_type}. "
            f"Valid types are: {', '.join(generators.keys())}"
        )

    return generator_class(parameter, template_dir, logger)


__all__ = [
    "BaseReportGenerator",
    "VolumeReportGenerator",
    "LoopReportGenerator",
    "TTimeReportGenerator",
    "YieldReportGenerator",
    "ConditionReportGenerator",
    "create_report_generator",
]
