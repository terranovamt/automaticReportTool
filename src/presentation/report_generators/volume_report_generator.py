"""
ART.stdf - Volume Report Generator

Generates comprehensive HTML reports for volume production test data.
Replaces VOLUME.ipynb with pure Python implementation.

Author: Matteo Terranova (matteo.terranova@st.com)
Organization: STMicroelectronics - MDRF GPAM
"""

import os
import json5
import logging
from pathlib import Path
from typing import Dict, Optional
import polars as pl
import plotly.offline as pyo

from src.domain.models.parameter import Parameter
from src.presentation.report_generators.base_report_generator import BaseReportGenerator
from src.presentation.visualizers import plotly_builder as graph
from src.presentation.visualizers import html_builder


class VolumeReportGenerator(BaseReportGenerator):
    """
    Generate volume production reports with PTR/FTR analysis.

    Features:
    - Test list index page
    - Individual PTR (parametric) test reports
    - Individual FTR (functional) test reports
    - Statistical analysis (Cp, Cpk, Yield)
    - Limit recommendations
    - Interactive Plotly visualizations
    """

    def get_report_type(self) -> str:
        """Get report type identifier."""
        return "VOLUME"

    def get_personalization(self, name: str) -> Dict:
        """
        Load personalization settings from ART.jsonc.

        Args:
            name: Setting name to retrieve

        Returns:
            Dictionary with settings or empty dict
        """
        file_path = os.path.join(
            self.parameter.to_dict()["MAIN"].split(self.parameter.code)[0],
            self.parameter.code,
            "ART.jsonc"
        )

        if not os.path.isfile(file_path):
            return {}

        try:
            with open(file_path, "r", encoding="utf-8") as file:
                dati = json5.load(file)
                return dati.get(name, {})
        except Exception as e:
            self.logger.warning(f"Error loading personalization: {e}")
            return {}

    def generate_composite_index(
        self,
        df_stdf: Dict[str, pl.DataFrame],
        report_path: Path
    ) -> tuple:
        """
        Generate composite test list index page.

        Args:
            df_stdf: Dictionary of DataFrames by record type
            report_path: Base report directory

        Returns:
            Tuple of (ptr_test_names, ftr_test_names)
        """
        self.logger.info("[REPORT] Generating composite test list")

        mir = df_stdf["mir"]
        ptr = df_stdf.get("ptr", pl.DataFrame())
        ftr = df_stdf.get("ftr", pl.DataFrame())

        # Get unique test names
        ptrtname = []
        if ptr.height > 0:
            ptrtname = (
                ptr.select(["TestName", "TestNumber"])
                .filter(~pl.col("TestName").cast(pl.Utf8).str.contains("LOG_TTIME"))
                .sort("TestNumber")
                .unique(subset=["TestName"])
                .select("TestName")
                .to_series()
                .to_list()
            )
            ptrtname = self._order_test_names(ptrtname)

        ftrtname = []
        if ftr.height > 0:
            ftrtname = (
                ftr.select(["TestName", "TestNumber"])
                .filter(~pl.col("TestName").cast(pl.Utf8).str.contains("LOG_TTIME"))
                .sort("TestNumber")
                .unique(subset=["TestName"])
                .select("TestName")
                .to_series()
                .to_list()
            )

        # Build HTML
        param_dict = self.parameter.to_dict()
        html_content = self.build_html_header(
            f"{self.parameter.com.replace('_', ' ')} {self.parameter.cut} {self.parameter.flow}"
        )

        html_content += f"""
        <div class="contentconteiner">
        {self.get_template_content("stlogo.html")}
        <p>
        {self.get_template_content("homebutton.html")}
        </p>
        </div>
        <h1 style="font-family:Arial; font-weight: normal; text-align:center; font-size: 4em; color:#03234B">
            {self.parameter.com.replace('_', ' ')} {self.parameter.flow}
        </h1>
        <h1 style="font-family:Arial; font-weight: normal; text-align:center; font-size: 3em; color:#03234B">
            {param_dict.get('PRODUCT', '')} {self.parameter.cut}
        </h1>
        <div encoding="UTF-8" standalone="no" style="width: 700px; margin: 0 auto; text-align: center;">
        {self.get_product_image(self.parameter.code)}
        </div>
        <hr>
        {self._build_mir_table(mir, param_dict)}
        """

        # Add test list
        html_content += '<h2 id="TableOfContent">Table of Contents<a class="anchor-link" href="#TableOfContent"></a></h2><hr>'

        testnames = sorted(ptrtname + ftrtname)
        if testnames:
            html_content += '<div class="contentconteiner">'
            for tname in testnames:
                html_content += f'<a class="btn" href="{tname.replace(":", "_")}.html">{tname}</a>\n'
            html_content += '<a class="btn" href="limits.html">Limits</a></div>'

        html_content += self.build_html_footer()

        # Save index
        index_path = report_path / self.parameter.com / "index.html"
        self.save_report(html_content, index_path)

        return ptrtname, ftrtname

    def _build_mir_table(self, mir: pl.DataFrame, param_dict: Dict) -> str:
        """Build MIR (Master Information Record) table."""
        import datetime

        return f"""
        <table border="1" align="center" style="height: 100%; width: 100%;">
            <tr>
                <td>FLOW</td>
                <td>{param_dict.get('FLOW', '')}</td>
            </tr>
            <tr>
                <td>Part ID-CUT</td>
                <td>{str(mir.select(pl.col("FAMLY_ID")).item(0, 0))}</td>
            </tr>
            <tr>
                <td>Division</td>
                <td>{param_dict.get('GROUP', '')}</td>
            </tr>
            <tr>
                <td>Revision</td>
                <td>{param_dict.get('REVISION', '')}</td>
            </tr>
            <tr>
                <td>Date</td>
                <td>{datetime.datetime.now().strftime("%d %B %Y %H:%M")}</td>
            </tr>
        </table>
        """

    def _order_test_names(self, test_names: list) -> list:
        """
        Order test names with special handling for Untrimmed/TrimValue/Trimmed.

        Args:
            test_names: List of test names

        Returns:
            Ordered list
        """
        def sort_key(name):
            prefix, suffix = name.split(":", 1) if ":" in name else (name, "")
            if "Untrimmed" in suffix:
                return (prefix, 0)
            elif "TrimValue" in suffix:
                return (prefix, 1)
            elif "Trimmed" in suffix:
                return (prefix, 2)
            return (prefix, 3)

        # Group by prefix and sort within groups
        grouped_tests = {}
        for test in test_names:
            prefix = test.split(":", 1)[0]
            if prefix not in grouped_tests:
                grouped_tests[prefix] = []
            grouped_tests[prefix].append(test)

        sorted_tests = []
        for prefix in test_names:
            prefix_key = prefix.split(":", 1)[0]
            if prefix_key in grouped_tests:
                sorted_tests.extend(sorted(grouped_tests[prefix_key], key=sort_key))
                del grouped_tests[prefix_key]

        return sorted_tests

    def generate_ptr_report(
        self,
        tname: str,
        df_stdf: Dict[str, pl.DataFrame],
        report_path: Path,
        stats_dict: Dict
    ):
        """
        Generate individual PTR (Parametric Test Record) report.

        Args:
            tname: Test name
            df_stdf: Dictionary of DataFrames
            report_path: Base report directory
            stats_dict: Dictionary to store statistics
        """
        self.logger.info(f"[REPORT] Generating PTR report: {tname}")

        td = df_stdf["ptr"].filter(pl.col("TestName") == tname)

        if td.is_empty():
            return

        # Process data
        from htmlgenv2 import process_ptr  # Import the existing logic
        stats, td_filtered, is_ftr = process_ptr(td)

        if is_ftr:
            # This is actually FTR data, handle accordingly
            df_stdf["ftr"] = df_stdf["ptr"].filter(pl.col("TestName") == tname)
            df_stdf["ftr"] = df_stdf["ftr"].with_columns(
                pl.col("PARM_FLG")
                .map_elements(lambda x: 1 if x == 192 else 0, return_dtype=pl.Int32)
                .alias("RESULT")
            )
            self.generate_ftr_report(tname, df_stdf, report_path)
            return

        # Store stats for limits generation
        stats_dict[tname] = stats

        # Get personalization
        STPaletteChar = self.get_personalization("STPaletteChar")
        xwafer = self.get_personalization("xwafer")
        ywafer = self.get_personalization("ywafer")

        # Get limits and units
        temp_30_data = td.filter(pl.col("°C") == "30")
        if temp_30_data.height > 0:
            ul = temp_30_data.select(pl.col("High Limit")).to_series().mode()[0]
            ll = temp_30_data.select(pl.col("Low Limit")).to_series().mode()[0]
            units = temp_30_data.select(pl.col("Unit")).to_series().mode()[0]
        else:
            ul = td.select(pl.col("High Limit")).to_series().mode()[0]
            ll = td.select(pl.col("Low Limit")).to_series().mode()[0]
            units = td.select(pl.col("Unit")).to_series().mode()[0]

        # Generate graph
        pl_types = td_filtered.select(pl.col("pltype")).unique().to_series().to_list()

        if len(pl_types) > 1 and "SPLIT" in pl_types:
            td_plot = td_filtered.filter(pl.col("pltype") == "SPLIT")
        else:
            td_plot = td_filtered

        fig = graph.boxploth(td_plot, ll, ul, units, STPaletteChar)

        # Generate HTML
        html_plot = pyo.plot(
            fig, output_type="div", include_plotlyjs=True, config={"responsive": True}
        )
        html_table = html_builder.generate_colored_ptrtable_html(stats)

        html_content = self.build_html_header(
            f"{self.parameter.com.replace('_', ' ')} {self.parameter.cut} {self.parameter.flow}"
        )
        html_content += html_plot
        html_content += html_table
        html_content += self.build_html_footer()

        # Save report
        file_path = report_path / self.parameter.com / f"{tname.replace(':', '_')}.html"
        self.save_report(html_content, file_path)

    def generate_ftr_report(
        self,
        tname: str,
        df_stdf: Dict[str, pl.DataFrame],
        report_path: Path
    ):
        """
        Generate individual FTR (Functional Test Record) report.

        Args:
            tname: Test name
            df_stdf: Dictionary of DataFrames
            report_path: Base report directory
        """
        self.logger.info(f"[REPORT] Generating FTR report: {tname}")

        td = df_stdf["ftr"].filter(pl.col("TestName") == tname)

        if td.is_empty():
            return

        # Process data
        from htmlgenv2 import process_ftr
        stats, td_filtered = process_ftr(td)

        # Get personalization
        STPaletteChar = self.get_personalization("STPaletteChar")

        # Generate graph
        pl_types = td_filtered.select(pl.col("pltype")).unique().to_series().to_list()

        if len(pl_types) > 1 and "SPLIT" in pl_types:
            td_plot = td_filtered.filter(pl.col("pltype") == "SPLIT")
        else:
            td_plot = td_filtered

        fig = graph.scatter(td_plot, STPaletteChar)

        # Generate HTML
        html_plot = pyo.plot(
            fig, output_type="div", include_plotlyjs=True, config={"responsive": True}
        )
        html_table = html_builder.generate_colored_ftrtable_html(stats)

        html_content = self.build_html_header(
            f"{self.parameter.com.replace('_', ' ')} {self.parameter.cut} {self.parameter.flow}"
        )
        html_content += html_plot
        html_content += html_table
        html_content += self.build_html_footer()

        # Save report
        file_path = report_path / self.parameter.com / f"{tname.replace(':', '_')}.html"
        self.save_report(html_content, file_path)

    def generate_limits_report(
        self,
        stats_dict: Dict,
        report_path: Path
    ):
        """
        Generate limits recommendation report.

        Args:
            stats_dict: Dictionary of test statistics
            report_path: Base report directory
        """
        self.logger.info("[REPORT] Generating limits report")

        from htmlgenv2 import get_limit_data

        STPaletteChar = self.get_personalization("STPaletteChar")

        html_content = self.build_html_header(
            f"{self.parameter.com.replace('_', ' ')} Limits"
        )

        # Group stats by prefix
        grouped_stats = {}
        for key, value in stats_dict.items():
            prefix = key.split(':')[0] if ':' in key else key
            value = value.select([col for col in value.columns if "clamp" not in col.lower()])

            if value.width == 0:
                continue

            if prefix not in grouped_stats:
                grouped_stats[prefix] = value
            else:
                try:
                    grouped_stats[prefix] = grouped_stats[prefix].vstack(value)
                except Exception:
                    # Handle type conflicts
                    existing_df = grouped_stats[prefix]
                    common_schema = {}
                    for col in existing_df.columns:
                        if col in value.columns:
                            type1 = existing_df[col].dtype
                            type2 = value[col].dtype
                            if type1 != type2:
                                if type1 == pl.String or type2 == pl.String:
                                    common_schema[col] = pl.String
                                elif type1 == pl.Float64 or type2 == pl.Float64:
                                    common_schema[col] = pl.Float64
                                else:
                                    common_schema[col] = pl.String

                    if common_schema:
                        existing_df_casted = existing_df.cast(common_schema)
                        value_casted = value.cast(common_schema)
                        grouped_stats[prefix] = existing_df_casted.vstack(value_casted)
                    else:
                        grouped_stats[prefix] = existing_df.vstack(value)

        # Generate limits for each test group
        for prefix, combined_df in grouped_stats.items():
            if combined_df.is_empty():
                continue

            limits_data = get_limit_data(prefix, combined_df)
            if len(limits_data):
                fig = graph.generate_limits(prefix, limits_data, STPaletteChar)
                html_plot = pyo.plot(
                    fig, output_type="div", include_plotlyjs=True, config={"responsive": True}
                )
                html_content += html_plot
                html_table = html_builder.generate_colored_limittable_html(
                    df=limits_data, tname=prefix
                )
                html_content += html_table

        html_content += self.build_html_footer()

        # Save report
        file_path = report_path / self.parameter.com / "limits.html"
        self.save_report(html_content, file_path)

    def generate(
        self,
        data_path: str,
        output_path: Path,
        df_stdf: Optional[Dict[str, pl.DataFrame]] = None,
        **kwargs
    ) -> Path:
        """
        Generate complete VOLUME report.

        Args:
            data_path: Path to parquet data files
            output_path: Base output directory
            df_stdf: Pre-loaded DataFrames (optional)
            **kwargs: Additional parameters

        Returns:
            Path to report directory
        """
        self.logger.info(f"[REPORT] Generating VOLUME report for {self.parameter.title}")

        # Load data if not provided
        if df_stdf is None:
            df_stdf = self._load_data(data_path)

        report_path = Path(output_path)

        # Generate composite index
        ptr_tests, ftr_tests = self.generate_composite_index(df_stdf, report_path)

        # Generate individual PTR reports
        stats_dict = {}
        for tname in ptr_tests:
            self.generate_ptr_report(tname, df_stdf, report_path, stats_dict)

        # Generate individual FTR reports
        for tname in ftr_tests:
            self.generate_ftr_report(tname, df_stdf, report_path)

        # Generate limits report
        if stats_dict:
            self.generate_limits_report(stats_dict, report_path)

        self.logger.info(f"[REPORT] VOLUME report completed: {report_path}")
        return report_path / self.parameter.com

    def _load_data(self, data_path: str) -> Dict[str, pl.DataFrame]:
        """Load required parquet files."""
        df_stdf = {}

        files_to_load = {
            "ptr": [0, 1, 5, 6, 7, 10, 11, 12, 13, 14, 15],
            "ftr": [0, 1, 4, 23],
            "mir": None,
            "prr": None,
            "pcr": None,
            "hbr": None,
            "sbr": None,
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
                df_stdf[record_type] = pl.DataFrame()

        return df_stdf
