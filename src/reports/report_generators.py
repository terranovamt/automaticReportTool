"""
Report Generators for Different Report Types

Modular, optimized generators for all STDF report types.

Author: Matteo Terranova (matteo.terranova@st.com)
Organization: STMicroelectronics - MDRF GPAM
"""

import polars as pl
from pathlib import Path
from typing import Dict, List, Optional
import os

from src.charts.chart_generator import ChartGenerator
from src.reports.html_template import HTMLTemplate


class BaseReportGenerator:
    """Base class for report generators."""

    def __init__(self):
        self.chart_gen = ChartGenerator(theme="plotly_white")
        self.html_template = HTMLTemplate()

    def generate(
        self,
        parameter: Dict,
        data: Dict[str, pl.DataFrame],
        output_path: Path
    ) -> Path:
        """
        Generate report.

        Args:
            parameter: Test parameters
            data: Dictionary of DataFrames (ptr, ftr, prr, etc.)
            output_path: Output directory

        Returns:
            Path to generated HTML report
        """
        raise NotImplementedError("Subclasses must implement generate()")

    def _save_report(self, html: str, output_path: Path, filename: str) -> Path:
        """Save HTML report to file."""
        output_path = Path(output_path)
        output_path.mkdir(parents=True, exist_ok=True)

        report_file = output_path / filename
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write(html)

        return report_file


class ConditionReportGenerator(BaseReportGenerator):
    """Generate Condition Report."""

    def generate(
        self,
        parameter: Dict,
        data: Dict[str, pl.DataFrame],
        output_path: Path
    ) -> Path:
        """Generate condition report with test limits and results."""

        ptr_df = data.get('ptr', pl.DataFrame())
        prr_df = data.get('prr', pl.DataFrame())

        if ptr_df.is_empty():
            raise ValueError("No PTR data available for condition report")

        # Calculate summary statistics
        summary_data = self._calculate_summary(ptr_df, prr_df)

        # Generate charts
        charts_html = []

        # 1. Test Results Scatter
        for test_num in ptr_df['TEST_NUM'].unique().to_list()[:10]:  # Limit to first 10 tests
            test_df = ptr_df.filter(pl.col('TEST_NUM') == test_num)
            test_name = test_df['TEST_TXT'].to_series()[0] if 'TEST_TXT' in test_df.columns else f"Test {test_num}"

            fig = self.chart_gen.create_scatter(
                df=test_df,
                x_col='PART_ID',
                y_col='RESULT',
                title=f"Test Results: {test_name}",
                color_col='SOFT_BIN' if 'SOFT_BIN' in test_df.columns else None,
                show_limits=True,
                lower_limit=test_df['LO_LIMIT'].to_series()[0] if 'LO_LIMIT' in test_df.columns else None,
                upper_limit=test_df['HI_LIMIT'].to_series()[0] if 'HI_LIMIT' in test_df.columns else None
            )
            charts_html.append(self.chart_gen.to_div(fig))

        # 2. Result Distribution Histogram
        fig_hist = self.chart_gen.create_histogram(
            df=ptr_df,
            value_col='RESULT',
            title="Result Distribution",
            bins=50
        )
        charts_html.append(self.chart_gen.to_div(fig_hist))

        # 3. Wafer Map (if coordinates available)
        if 'X_COORD' in ptr_df.columns and 'Y_COORD' in ptr_df.columns:
            fig_wafer = self.chart_gen.create_wafer_map(
                df=ptr_df,
                x_col='X_COORD',
                y_col='Y_COORD',
                color_col='RESULT',
                title="Wafer Map - Test Results"
            )
            charts_html.append(self.chart_gen.to_div(fig_wafer))

        # Generate HTML report
        html = self.html_template.create_report(
            title=f"Condition Report - {parameter.get('CUT', 'N/A')}",
            report_type="CONDITION",
            parameter=parameter,
            summary_data=summary_data,
            charts_html=charts_html
        )

        # Save report
        filename = f"condition_report_{parameter.get('LOT', 'unknown')}_{parameter.get('WAFER', 'unknown')}.html"
        return self._save_report(html, output_path, filename)

    def _calculate_summary(self, ptr_df: pl.DataFrame, prr_df: pl.DataFrame) -> Dict:
        """Calculate summary statistics."""
        total_parts = len(ptr_df['PART_ID'].unique()) if not ptr_df.is_empty() else 0
        total_tests = len(ptr_df['TEST_NUM'].unique()) if not ptr_df.is_empty() else 0

        passed_parts = 0
        if not prr_df.is_empty() and 'PART_FLG' in prr_df.columns:
            passed_parts = len(prr_df.filter(pl.col('PART_FLG') == 0))

        yield_pct = (passed_parts / total_parts * 100) if total_parts > 0 else 0

        return {
            'Total Parts': total_parts,
            'Total Tests': total_tests,
            'Passed Parts': passed_parts,
            'Yield': yield_pct
        }


class YieldReportGenerator(BaseReportGenerator):
    """Generate Yield Report."""

    def generate(
        self,
        parameter: Dict,
        data: Dict[str, pl.DataFrame],
        output_path: Path
    ) -> Path:
        """Generate yield report with bin analysis."""

        prr_df = data.get('prr', pl.DataFrame())
        hbr_df = data.get('hbr', pl.DataFrame())
        sbr_df = data.get('sbr', pl.DataFrame())

        if prr_df.is_empty():
            raise ValueError("No PRR data available for yield report")

        # Calculate summary
        summary_data = self._calculate_yield_summary(prr_df, hbr_df, sbr_df)

        # Generate charts
        charts_html = []

        # 1. Bin Distribution Bar Chart
        if 'SOFT_BIN' in prr_df.columns:
            bin_counts = prr_df.groupby('SOFT_BIN').agg(pl.count().alias('COUNT'))
            fig_bins = self.chart_gen.create_bar_chart(
                df=bin_counts,
                x_col='SOFT_BIN',
                y_col='COUNT',
                title="Bin Distribution",
                x_label="Soft Bin",
                y_label="Count"
            )
            charts_html.append(self.chart_gen.to_div(fig_bins))

        # 2. Yield Over Time (if PART_ID represents sequence)
        if 'PART_ID' in prr_df.columns and 'PART_FLG' in prr_df.columns:
            prr_with_pass = prr_df.with_columns([
                (pl.col('PART_FLG') == 0).cast(pl.Int32).alias('PASS')
            ])

            # Rolling yield calculation
            rolling_yield = prr_with_pass.sort('PART_ID').with_columns([
                pl.col('PASS').rolling_mean(window_size=100).alias('ROLLING_YIELD')
            ])

            fig_rolling = self.chart_gen.create_line_plot(
                df=rolling_yield,
                x_col='PART_ID',
                y_col='ROLLING_YIELD',
                title="Yield Trend (Rolling Average)",
                x_label="Part ID",
                y_label="Yield"
            )
            charts_html.append(self.chart_gen.to_div(fig_rolling))

        # Generate HTML
        html = self.html_template.create_report(
            title=f"Yield Report - {parameter.get('CUT', 'N/A')}",
            report_type="YIELD",
            parameter=parameter,
            summary_data=summary_data,
            charts_html=charts_html
        )

        filename = f"yield_report_{parameter.get('LOT', 'unknown')}_{parameter.get('WAFER', 'unknown')}.html"
        return self._save_report(html, output_path, filename)

    def _calculate_yield_summary(
        self,
        prr_df: pl.DataFrame,
        hbr_df: pl.DataFrame,
        sbr_df: pl.DataFrame
    ) -> Dict:
        """Calculate yield summary statistics."""
        total_parts = len(prr_df)

        passed_parts = 0
        if 'PART_FLG' in prr_df.columns:
            passed_parts = len(prr_df.filter(pl.col('PART_FLG') == 0))

        failed_parts = total_parts - passed_parts
        yield_pct = (passed_parts / total_parts * 100) if total_parts > 0 else 0

        return {
            'Total Parts': total_parts,
            'Passed Parts': passed_parts,
            'Failed Parts': failed_parts,
            'Yield': yield_pct,
            'Unique Bins': len(prr_df['SOFT_BIN'].unique()) if 'SOFT_BIN' in prr_df.columns else 0
        }


class VolumeReportGenerator(BaseReportGenerator):
    """Generate Volume Report."""

    def generate(
        self,
        parameter: Dict,
        data: Dict[str, pl.DataFrame],
        output_path: Path
    ) -> Path:
        """Generate volume report showing test coverage and statistics."""

        ptr_df = data.get('ptr', pl.DataFrame())
        ftr_df = data.get('ftr', pl.DataFrame())
        prr_df = data.get('prr', pl.DataFrame())

        if ptr_df.is_empty() and ftr_df.is_empty():
            raise ValueError("No test data available for volume report")

        # Calculate summary
        summary_data = self._calculate_volume_summary(ptr_df, ftr_df, prr_df)

        # Generate charts
        charts_html = []

        # 1. Test Count by Type
        if not ptr_df.is_empty():
            test_counts = ptr_df.groupby('TEST_NUM').agg(pl.count().alias('COUNT'))
            fig_tests = self.chart_gen.create_bar_chart(
                df=test_counts.head(20),  # Top 20 tests
                x_col='TEST_NUM',
                y_col='COUNT',
                title="Test Execution Count (Top 20)",
                x_label="Test Number",
                y_label="Executions"
            )
            charts_html.append(self.chart_gen.to_div(fig_tests))

        # 2. Parts per Bin
        if 'SOFT_BIN' in prr_df.columns:
            bin_volume = prr_df.groupby('SOFT_BIN').agg(pl.count().alias('VOLUME'))
            fig_volume = self.chart_gen.create_bar_chart(
                df=bin_volume,
                x_col='SOFT_BIN',
                y_col='VOLUME',
                title="Volume per Bin",
                x_label="Soft Bin",
                y_label="Parts"
            )
            charts_html.append(self.chart_gen.to_div(fig_volume))

        # Generate HTML
        html = self.html_template.create_report(
            title=f"Volume Report - {parameter.get('CUT', 'N/A')}",
            report_type="VOLUME",
            parameter=parameter,
            summary_data=summary_data,
            charts_html=charts_html
        )

        filename = f"volume_report_{parameter.get('LOT', 'unknown')}_{parameter.get('WAFER', 'unknown')}.html"
        return self._save_report(html, output_path, filename)

    def _calculate_volume_summary(
        self,
        ptr_df: pl.DataFrame,
        ftr_df: pl.DataFrame,
        prr_df: pl.DataFrame
    ) -> Dict:
        """Calculate volume summary statistics."""
        total_ptr = len(ptr_df) if not ptr_df.is_empty() else 0
        total_ftr = len(ftr_df) if not ftr_df.is_empty() else 0
        total_parts = len(prr_df) if not prr_df.is_empty() else 0

        unique_tests = 0
        if not ptr_df.is_empty():
            unique_tests += len(ptr_df['TEST_NUM'].unique())
        if not ftr_df.is_empty():
            unique_tests += len(ftr_df['TEST_NUM'].unique())

        return {
            'Total Parts Tested': total_parts,
            'Parametric Tests': total_ptr,
            'Functional Tests': total_ftr,
            'Unique Tests': unique_tests,
            'Avg Tests per Part': ((total_ptr + total_ftr) / total_parts) if total_parts > 0 else 0
        }


class LoopTimeReportGenerator(BaseReportGenerator):
    """Generate Loop Time Report."""

    def generate(
        self,
        parameter: Dict,
        data: Dict[str, pl.DataFrame],
        output_path: Path
    ) -> Path:
        """Generate loop time report analyzing test execution time."""

        ptr_df = data.get('ptr', pl.DataFrame())
        prr_df = data.get('prr', pl.DataFrame())

        # For loop time, we need test time information
        # This is typically calculated or available in certain STDF fields

        summary_data = {
            'Total Parts': len(prr_df) if not prr_df.is_empty() else 0,
            'Tests Analyzed': len(ptr_df['TEST_NUM'].unique()) if not ptr_df.is_empty() else 0
        }

        charts_html = []

        # Generate HTML
        html = self.html_template.create_report(
            title=f"Loop Time Report - {parameter.get('CUT', 'N/A')}",
            report_type="LOOP TIME",
            parameter=parameter,
            summary_data=summary_data,
            charts_html=charts_html
        )

        filename = f"looptime_report_{parameter.get('LOT', 'unknown')}_{parameter.get('WAFER', 'unknown')}.html"
        return self._save_report(html, output_path, filename)


class CharReportGenerator(BaseReportGenerator):
    """Generate Characterization Report."""

    def generate(
        self,
        parameter: Dict,
        data: Dict[str, pl.DataFrame],
        output_path: Path
    ) -> Path:
        """Generate characterization report with detailed analysis."""

        ptr_df = data.get('ptr', pl.DataFrame())

        if ptr_df.is_empty():
            raise ValueError("No PTR data available for char report")

        # Calculate summary
        summary_data = self._calculate_char_summary(ptr_df)

        # Generate charts
        charts_html = []

        # 1. Correlation matrix for multiple tests
        unique_tests = ptr_df['TEST_NUM'].unique().to_list()[:5]  # Top 5 tests

        for test_num in unique_tests:
            test_df = ptr_df.filter(pl.col('TEST_NUM') == test_num)
            test_name = test_df['TEST_TXT'].to_series()[0] if 'TEST_TXT' in test_df.columns else f"Test {test_num}"

            # Distribution
            fig_dist = self.chart_gen.create_histogram(
                df=test_df,
                value_col='RESULT',
                title=f"Distribution: {test_name}",
                bins=50,
                show_limits=True,
                lower_limit=test_df['LO_LIMIT'].to_series()[0] if 'LO_LIMIT' in test_df.columns else None,
                upper_limit=test_df['HI_LIMIT'].to_series()[0] if 'HI_LIMIT' in test_df.columns else None
            )
            charts_html.append(self.chart_gen.to_div(fig_dist))

            # Box plot by temperature/corner
            if 'TEMPERATURE' in test_df.columns:
                fig_box = self.chart_gen.create_box_plot(
                    df=test_df,
                    y_col='RESULT',
                    title=f"Box Plot by Temperature: {test_name}",
                    group_col='TEMPERATURE'
                )
                charts_html.append(self.chart_gen.to_div(fig_box))

        # Generate HTML
        html = self.html_template.create_report(
            title=f"Characterization Report - {parameter.get('CUT', 'N/A')}",
            report_type="CHARACTERIZATION",
            parameter=parameter,
            summary_data=summary_data,
            charts_html=charts_html
        )

        filename = f"char_report_{parameter.get('LOT', 'unknown')}_{parameter.get('WAFER', 'unknown')}.html"
        return self._save_report(html, output_path, filename)

    def _calculate_char_summary(self, ptr_df: pl.DataFrame) -> Dict:
        """Calculate characterization summary statistics."""
        unique_tests = len(ptr_df['TEST_NUM'].unique())
        total_measurements = len(ptr_df)

        # Calculate pass/fail if limits available
        passed = total_measurements
        if 'LO_LIMIT' in ptr_df.columns and 'HI_LIMIT' in ptr_df.columns:
            passed = len(ptr_df.filter(
                (pl.col('RESULT') >= pl.col('LO_LIMIT')) &
                (pl.col('RESULT') <= pl.col('HI_LIMIT'))
            ))

        return {
            'Unique Tests': unique_tests,
            'Total Measurements': total_measurements,
            'Passed Measurements': passed,
            'Pass Rate': (passed / total_measurements * 100) if total_measurements > 0 else 0
        }
