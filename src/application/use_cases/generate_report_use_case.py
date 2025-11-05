"""
ART.stdf - Generate Report Use Case

Use case for generating test reports from Parquet data.
Uses pure Python report generators (no Jupyter dependency).

Author: Matteo Terranova (matteo.terranova@st.com)
Organization: STMicroelectronics - MDRF GPAM
"""

import logging
import os
from pathlib import Path
from typing import Dict, Optional

import polars as pl

from src.domain.models.parameter import Parameter
from src.infrastructure.storage.file_repository import FileRepository
from src.presentation.report_generators import create_report_generator


class GenerateReportUseCase:
    """
    Use case for generating test reports.

    This use case orchestrates the generation of HTML reports
    from processed Parquet data.

    Responsibilities:
    - Load required data from Parquet files
    - Determine report type and configuration
    - Generate report using appropriate template
    - Save report to correct location
    - Track completion
    """

    def __init__(
        self,
        file_repository: Optional[FileRepository] = None,
        logger: Optional[logging.Logger] = None
    ):
        """
        Initialize the use case.

        Args:
            file_repository: File repository instance (injected)
            logger: Logger instance (injected)
        """
        self.file_repository = file_repository or FileRepository()
        self.logger = logger or logging.getLogger(__name__)

    def execute(
        self,
        parameter: Parameter,
        data_path: str,
        df_stdf: Optional[Dict[str, pl.DataFrame]] = None
    ) -> Path:
        """
        Execute report generation.

        Args:
            parameter: Parameter object with report metadata
            data_path: Base path to parquet data files
            df_stdf: Optional pre-loaded DataFrames

        Returns:
            Path to generated report file

        Raises:
            FileNotFoundError: If required data files don't exist
            ValueError: If report generation fails

        Example:
            >>> use_case = GenerateReportUseCase()
            >>> report_path = use_case.execute(parameter, "/data/test.std")
        """
        # Log start
        self._log_start(parameter)

        try:
            # Load data if not provided
            if df_stdf is None:
                df_stdf = self._load_data(data_path)

            # Determine report type and generate
            report_type = self._determine_report_type(parameter)

            if report_type == "TTIME":
                report_path = self._generate_ttime_report(parameter, df_stdf, data_path)
            elif report_type == "YIELD":
                report_path = self._generate_yield_report(parameter, df_stdf, data_path)
            elif report_type == "CONDITION":
                report_path = self._generate_condition_report(parameter, data_path)
            else:
                report_path = self._generate_standard_report(parameter, df_stdf, data_path)

            # Log completion
            self._log_completion(parameter, report_path)

            return report_path

        except Exception as e:
            self.logger.error(
                f"[REPORT] Generation failed for {parameter.title}: {e}"
            )
            raise

    def _load_data(self, data_path: str) -> Dict[str, pl.DataFrame]:
        """
        Load all required parquet data files.

        Args:
            data_path: Base path to data files

        Returns:
            Dictionary of DataFrames by record type
        """
        df_stdf = {}

        # Define required files with column subsets
        files_to_load = {
            "ptr": [0, 1, 5, 6, 7, 10, 11, 12, 13, 14, 15],  # Parametric test records
            "ftr": [0, 1, 4, 23],  # Functional test records
            "mir": None,  # Master information record
            "prr": None,  # Part result record
            "pcr": None,  # Part count record
            "hbr": None,  # Hardware bin record
            "sbr": None,  # Software bin record
        }

        for record_type, columns in files_to_load.items():
            file_path = f"{data_path}.{record_type}.parquet"

            if os.path.exists(file_path):
                try:
                    df = pl.read_parquet(file_path, columns=columns)
                    df_stdf[record_type] = df
                except Exception as e:
                    self.logger.warning(f"Failed to load {record_type}: {e}")
                    df_stdf[record_type] = pl.DataFrame()
            else:
                self.logger.warning(f"File not found: {file_path}")
                df_stdf[record_type] = pl.DataFrame()

        return df_stdf

    def _determine_report_type(self, parameter: Parameter) -> str:
        """
        Determine the type of report to generate.

        Args:
            parameter: Parameter object

        Returns:
            Report type string
        """
        if str(parameter.com).upper() == "TTIME":
            return "TTIME"
        elif str(parameter.com).upper() == "YIELD":
            return "YIELD"
        elif str(parameter.type).upper() == "CONDITION":
            return "CONDITION"
        else:
            return "STANDARD"

    def _generate_standard_report(
        self,
        parameter: Parameter,
        df_stdf: Dict[str, pl.DataFrame],
        data_path: str
    ) -> Path:
        """Generate standard composite report using VolumeReportGenerator."""
        # Use new pure Python report generator
        generator = create_report_generator("VOLUME", parameter, logger=self.logger)

        # Determine output directory
        output_dir = os.path.dirname(data_path)

        # Generate report
        report_path = generator.generate(
            data_path=data_path,
            output_path=Path(output_dir),
            df_stdf=df_stdf
        )

        return report_path

    def _generate_ttime_report(
        self,
        parameter: Parameter,
        df_stdf: Dict[str, pl.DataFrame],
        data_path: str
    ) -> Path:
        """Generate test time analysis report using TTimeReportGenerator."""
        # Use new pure Python report generator
        generator = create_report_generator("TTIME", parameter, logger=self.logger)

        # Determine output directory
        output_dir = os.path.dirname(data_path)

        # Generate report
        report_path = generator.generate(
            data_path=data_path,
            output_path=Path(output_dir),
            df_stdf=df_stdf
        )

        return report_path

    def _generate_yield_report(
        self,
        parameter: Parameter,
        df_stdf: Dict[str, pl.DataFrame],
        data_path: str
    ) -> Path:
        """Generate yield analysis report using YieldReportGenerator."""
        # Skip for X30 type
        if parameter.type.upper() == "X30":
            self.logger.warning("Yield reports not supported for X30 type")
            raise ValueError("Yield reports not supported for X30 type")

        # Use new pure Python report generator
        generator = create_report_generator("YIELD", parameter, logger=self.logger)

        # Determine output directory
        output_dir = os.path.dirname(data_path)

        # Generate report
        report_path = generator.generate(
            data_path=data_path,
            output_path=Path(output_dir),
            df_stdf=df_stdf
        )

        return report_path

    def _generate_condition_report(
        self,
        parameter: Parameter,
        data_path: str
    ) -> Path:
        """Generate condition report using ConditionReportGenerator."""
        # Use new pure Python report generator
        generator = create_report_generator("CONDITION", parameter, logger=self.logger)

        # Determine output directory
        output_dir = os.path.dirname(data_path)

        # Generate report (note: CONDITION doesn't need df_stdf pre-loaded)
        report_path = generator.generate(
            data_path=data_path,
            output_path=Path(output_dir)
        )

        return report_path

    def _log_start(self, parameter: Parameter):
        """Log report generation start."""
        self.logger.info(
            f"[REPORT] Start generation: {parameter.title}"
        )

    def _log_completion(self, parameter: Parameter, report_path: Path):
        """Log report generation completion."""
        self.logger.info(
            f"[REPORT] Completed: {parameter.title} -> {report_path.name}"
        )

    def is_report_generated(self, parameter: Parameter, base_path: str) -> bool:
        """
        Check if report has already been generated.

        Args:
            parameter: Parameter object
            base_path: Base path for report

        Returns:
            True if report exists, False otherwise
        """
        report_path = self.file_repository.get_report_path(
            base_path,
            parameter.to_dict(),
            "DATA2REPORT"
        )

        return report_path.exists()
