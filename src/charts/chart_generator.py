"""
Chart Generation Module - Modular and Optimized

This module provides a clean, reusable system for generating charts
using Plotly. Supports all common chart types used in STDF reporting.

Author: Matteo Terranova (matteo.terranova@st.com)
Organization: STMicroelectronics - MDRF GPAM
"""

import plotly.graph_objects as go
import plotly.express as px
from typing import List, Dict, Optional, Union, Tuple
import polars as pl
import numpy as np


class ChartGenerator:
    """
    Modular chart generator for STDF reports.

    Supports:
    - Scatter plots
    - Line plots
    - Bar charts
    - Histograms
    - Box plots
    - Heatmaps
    - Wafer maps
    """

    def __init__(self, theme: str = "plotly_white"):
        """
        Initialize chart generator.

        Args:
            theme: Plotly theme (plotly, plotly_white, plotly_dark, etc.)
        """
        self.theme = theme
        self.default_config = {
            'displayModeBar': True,
            'displaylogo': False,
            'modeBarButtonsToRemove': ['pan2d', 'lasso2d', 'select2d']
        }

    def create_scatter(
        self,
        df: pl.DataFrame,
        x_col: str,
        y_col: str,
        title: str,
        color_col: Optional[str] = None,
        size_col: Optional[str] = None,
        hover_data: Optional[List[str]] = None,
        x_label: Optional[str] = None,
        y_label: Optional[str] = None,
        show_limits: bool = False,
        lower_limit: Optional[float] = None,
        upper_limit: Optional[float] = None,
        width: int = 1200,
        height: int = 600
    ) -> go.Figure:
        """
        Create scatter plot.

        Args:
            df: Polars DataFrame
            x_col: Column for x-axis
            y_col: Column for y-axis
            title: Chart title
            color_col: Column for color grouping
            size_col: Column for marker size
            hover_data: Additional columns for hover info
            x_label: X-axis label
            y_label: Y-axis label
            show_limits: Show limit lines
            lower_limit: Lower limit value
            upper_limit: Upper limit value
            width: Chart width
            height: Chart height

        Returns:
            Plotly figure
        """
        # Convert to pandas for plotly express
        df_pd = df.to_pandas()

        fig = px.scatter(
            df_pd,
            x=x_col,
            y=y_col,
            color=color_col,
            size=size_col,
            hover_data=hover_data,
            title=title,
            template=self.theme
        )

        # Add limit lines if requested
        if show_limits:
            if lower_limit is not None:
                fig.add_hline(
                    y=lower_limit,
                    line_dash="dash",
                    line_color="red",
                    annotation_text="Lower Limit"
                )
            if upper_limit is not None:
                fig.add_hline(
                    y=upper_limit,
                    line_dash="dash",
                    line_color="red",
                    annotation_text="Upper Limit"
                )

        # Update layout
        fig.update_layout(
            xaxis_title=x_label or x_col,
            yaxis_title=y_label or y_col,
            width=width,
            height=height,
            hovermode='closest'
        )

        return fig

    def create_histogram(
        self,
        df: pl.DataFrame,
        value_col: str,
        title: str,
        bins: int = 50,
        color_col: Optional[str] = None,
        x_label: Optional[str] = None,
        show_limits: bool = False,
        lower_limit: Optional[float] = None,
        upper_limit: Optional[float] = None,
        width: int = 1200,
        height: int = 600
    ) -> go.Figure:
        """Create histogram with optional limit lines."""
        df_pd = df.to_pandas()

        fig = px.histogram(
            df_pd,
            x=value_col,
            color=color_col,
            nbins=bins,
            title=title,
            template=self.theme
        )

        # Add limit lines
        if show_limits:
            if lower_limit is not None:
                fig.add_vline(
                    x=lower_limit,
                    line_dash="dash",
                    line_color="red",
                    annotation_text="LL"
                )
            if upper_limit is not None:
                fig.add_vline(
                    x=upper_limit,
                    line_dash="dash",
                    line_color="red",
                    annotation_text="UL"
                )

        fig.update_layout(
            xaxis_title=x_label or value_col,
            yaxis_title="Count",
            width=width,
            height=height
        )

        return fig

    def create_box_plot(
        self,
        df: pl.DataFrame,
        y_col: str,
        title: str,
        group_col: Optional[str] = None,
        y_label: Optional[str] = None,
        width: int = 1200,
        height: int = 600
    ) -> go.Figure:
        """Create box plot."""
        df_pd = df.to_pandas()

        fig = px.box(
            df_pd,
            y=y_col,
            x=group_col,
            title=title,
            template=self.theme
        )

        fig.update_layout(
            yaxis_title=y_label or y_col,
            width=width,
            height=height
        )

        return fig

    def create_line_plot(
        self,
        df: pl.DataFrame,
        x_col: str,
        y_col: str,
        title: str,
        color_col: Optional[str] = None,
        x_label: Optional[str] = None,
        y_label: Optional[str] = None,
        width: int = 1200,
        height: int = 600
    ) -> go.Figure:
        """Create line plot."""
        df_pd = df.to_pandas()

        fig = px.line(
            df_pd,
            x=x_col,
            y=y_col,
            color=color_col,
            title=title,
            template=self.theme,
            markers=True
        )

        fig.update_layout(
            xaxis_title=x_label or x_col,
            yaxis_title=y_label or y_col,
            width=width,
            height=height
        )

        return fig

    def create_bar_chart(
        self,
        df: pl.DataFrame,
        x_col: str,
        y_col: str,
        title: str,
        color_col: Optional[str] = None,
        x_label: Optional[str] = None,
        y_label: Optional[str] = None,
        orientation: str = 'v',
        width: int = 1200,
        height: int = 600
    ) -> go.Figure:
        """Create bar chart."""
        df_pd = df.to_pandas()

        fig = px.bar(
            df_pd,
            x=x_col,
            y=y_col,
            color=color_col,
            title=title,
            template=self.theme,
            orientation=orientation
        )

        fig.update_layout(
            xaxis_title=x_label or x_col,
            yaxis_title=y_label or y_col,
            width=width,
            height=height
        )

        return fig

    def create_wafer_map(
        self,
        df: pl.DataFrame,
        x_col: str,
        y_col: str,
        color_col: str,
        title: str,
        color_label: Optional[str] = None,
        colorscale: str = 'RdYlGn_r',
        width: int = 800,
        height: int = 800
    ) -> go.Figure:
        """
        Create wafer map heatmap.

        Args:
            df: DataFrame with wafer coordinates
            x_col: X coordinate column
            y_col: Y coordinate column
            color_col: Value column for coloring
            title: Chart title
            color_label: Label for color scale
            colorscale: Plotly colorscale
            width: Chart width
            height: Chart height
        """
        df_pd = df.to_pandas()

        # Pivot data for heatmap
        pivot_df = df_pd.pivot_table(
            index=y_col,
            columns=x_col,
            values=color_col,
            aggfunc='mean'
        )

        fig = go.Figure(data=go.Heatmap(
            z=pivot_df.values,
            x=pivot_df.columns,
            y=pivot_df.index,
            colorscale=colorscale,
            colorbar=dict(title=color_label or color_col)
        ))

        fig.update_layout(
            title=title,
            xaxis_title="X Coordinate",
            yaxis_title="Y Coordinate",
            width=width,
            height=height,
            template=self.theme,
            yaxis=dict(scaleanchor="x", scaleratio=1)
        )

        return fig

    def create_heatmap(
        self,
        df: pl.DataFrame,
        x_col: str,
        y_col: str,
        value_col: str,
        title: str,
        colorscale: str = 'Viridis',
        width: int = 1200,
        height: int = 600
    ) -> go.Figure:
        """Create general heatmap."""
        df_pd = df.to_pandas()

        pivot_df = df_pd.pivot_table(
            index=y_col,
            columns=x_col,
            values=value_col,
            aggfunc='mean'
        )

        fig = go.Figure(data=go.Heatmap(
            z=pivot_df.values,
            x=pivot_df.columns,
            y=pivot_df.index,
            colorscale=colorscale
        ))

        fig.update_layout(
            title=title,
            xaxis_title=x_col,
            yaxis_title=y_col,
            width=width,
            height=height,
            template=self.theme
        )

        return fig

    def save_html(
        self,
        fig: go.Figure,
        filepath: str,
        include_plotlyjs: Union[bool, str] = 'cdn',
        config: Optional[Dict] = None
    ):
        """
        Save figure as HTML file.

        Args:
            fig: Plotly figure
            filepath: Output file path
            include_plotlyjs: How to include plotly.js ('cdn', True, False)
            config: Custom config dict
        """
        config = config or self.default_config
        fig.write_html(
            filepath,
            include_plotlyjs=include_plotlyjs,
            config=config
        )

    def to_div(self, fig: go.Figure, include_plotlyjs: bool = False) -> str:
        """
        Convert figure to HTML div string.

        Args:
            fig: Plotly figure
            include_plotlyjs: Include plotly.js in output

        Returns:
            HTML div string
        """
        return fig.to_html(
            include_plotlyjs=include_plotlyjs,
            div_id=None,
            full_html=False
        )


# Convenience functions for common chart types
def create_test_result_chart(
    df: pl.DataFrame,
    test_name: str,
    chart_gen: Optional[ChartGenerator] = None
) -> go.Figure:
    """Create standard test result scatter plot with limits."""
    if chart_gen is None:
        chart_gen = ChartGenerator()

    # Get limits from dataframe if available
    lower_limit = df.select("LO_LIMIT").to_series()[0] if "LO_LIMIT" in df.columns else None
    upper_limit = df.select("HI_LIMIT").to_series()[0] if "HI_LIMIT" in df.columns else None

    return chart_gen.create_scatter(
        df=df,
        x_col="PART_ID",
        y_col="RESULT",
        title=f"Test Results: {test_name}",
        color_col="SOFT_BIN" if "SOFT_BIN" in df.columns else None,
        x_label="Part ID",
        y_label="Result",
        show_limits=True,
        lower_limit=lower_limit,
        upper_limit=upper_limit
    )


def create_yield_chart(
    df: pl.DataFrame,
    chart_gen: Optional[ChartGenerator] = None
) -> go.Figure:
    """Create yield bar chart."""
    if chart_gen is None:
        chart_gen = ChartGenerator()

    return chart_gen.create_bar_chart(
        df=df,
        x_col="BIN",
        y_col="COUNT",
        title="Yield by Bin",
        color_col="BIN_TYPE",
        x_label="Bin",
        y_label="Count"
    )
