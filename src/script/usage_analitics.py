"""
Resource Analysis Report Generator

This module generates an interactive HTML report analyzing resource usage and execution times
from a PARQUET history file. It creates various visualizations using Plotly to provide insights
into product usage, flow distribution, and execution patterns.

Usage:
    from report_generator import generate_usage

    # Simple usage with defaults
    generate_usage()

    # Custom input/output files
    generate_usage(
        input_file="my_data.parquet",
        output_file="my_report.html"
    )

    # Custom top N for IDs chart
    generate_usage(top_n_ids=20)
"""

from datetime import datetime
from typing import Dict, List, Optional

import polars as pl
import plotly.graph_objects as go


# Color scheme constants based on UI design system
class Colors:
    """Color palette for consistent visualization styling."""

    PRIMARY = "#03234b"  # Dark blue - main UI color
    YELLOW = "#ffd200"  # Bright yellow - accent color
    CYAN = "#3cb4e6"  # Light blue - secondary accent
    MAGENTA = "#e6007e"  # Pink - tertiary accent
    GREEN = "#49b170"  # Green - quaternary accent
    WHITE = "#ffffff"  # White - background
    GREY = "#eeeff1"  # Light grey - subtle background


# Multi-chart color palette
COLOR_PALETTE = [
    Colors.CYAN,
    Colors.MAGENTA,
    Colors.YELLOW,
    Colors.GREEN,
    Colors.PRIMARY,
]


def parse_datetime(dt_str: str) -> datetime:
    """
    Parse custom datetime string format to datetime object.

    Expected format: "HH:MM:SS:mmm DD-MM-YYYY"

    Args:
        dt_str: Datetime string in custom format

    Returns:
        datetime: Parsed datetime object
    """
    parts = dt_str.strip().split(" ")
    time_part = parts[0]
    date_part = parts[1]

    time_vals = time_part.split(":")
    date_vals = date_part.split("-")

    return datetime(
        year=int(date_vals[2]),
        month=int(date_vals[1]),
        day=int(date_vals[0]),
        hour=int(time_vals[0]),
        minute=int(time_vals[1]),
        second=int(time_vals[2]),
        microsecond=int(time_vals[3]) * 1000,
    )


def load_and_preprocess_data(filepath: str) -> pl.DataFrame:
    """
    Load PARQUET data and preprocess it for analysis.

    This function:
    1. Reads the PARQUET file
    2. Parses datetime columns
    3. Calculates execution duration in hours
    4. Filters out zero-duration executions
    5. Removes duplicate paths

    Args:
        filepath: Path to the PARQUET file

    Returns:
        pl.DataFrame: Preprocessed dataframe ready for analysis
    """
    # Load data
    df = pl.read_parquet(filepath)

    # Parse datetime columns
    df = df.with_columns(
        [
            pl.col("creation_time")
            .map_elements(parse_datetime, return_dtype=pl.Datetime)
            .alias("start_date"),
            pl.col("end_time")
            .map_elements(parse_datetime, return_dtype=pl.Datetime)
            .alias("end_date"),
        ]
    )

    # Calculate duration in hours
    df = df.with_columns(
        [
            (
                (pl.col("end_date") - pl.col("start_date")).dt.total_milliseconds()
                / (1000 * 60 * 60)
            ).alias("duration_hours")
        ]
    )

    # Filter and deduplicate
    # df = df.filter(pl.col("duration_hours") > 0)
    df = df.unique(subset=["path"])

    return df


def create_product_usage_chart(df: pl.DataFrame) -> str:
    """
    Create bar chart showing execution count by product.

    Args:
        df: Preprocessed dataframe

    Returns:
        str: JavaScript code to render the chart
    """
    product_counts = (
        df.group_by("productcut").agg(pl.len().alias("count")).sort("productcut")
    )

    fig = go.Figure(
        data=[
            go.Bar(
                x=product_counts["productcut"].to_list(),
                y=product_counts["count"].to_list(),
                marker_color=Colors.PRIMARY,
                text=product_counts["count"].to_list(),
                textposition="auto",
            )
        ]
    )

    fig.update_layout(
        xaxis_title="Product",
        yaxis_title="Number of Executions",
        height=500,
        plot_bgcolor="white",
    )

    return f"Plotly.newPlot('chart1', {fig.to_json()});"


def create_flow_distribution_chart(df: pl.DataFrame) -> str:
    """
    Create pie chart showing distribution across flows.

    Args:
        df: Preprocessed dataframe

    Returns:
        str: JavaScript code to render the chart
    """
    flow_counts = df.group_by("flow").agg(pl.len().alias("count")).sort("flow")

    fig = go.Figure(
        data=[
            go.Pie(
                labels=flow_counts["flow"].to_list(),
                values=flow_counts["count"].to_list(),
                hole=0.4,
                marker=dict(colors=COLOR_PALETTE),
            )
        ]
    )

    fig.update_layout(height=500)

    return f"Plotly.newPlot('chart2', {fig.to_json()});"


def create_type_usage_chart(df: pl.DataFrame) -> str:
    """
    Create bar chart showing execution count by type.

    Args:
        df: Preprocessed dataframe

    Returns:
        str: JavaScript code to render the chart
    """
    type_counts = df.group_by("type").agg(pl.len().alias("count")).sort("type")

    fig = go.Figure(
        data=[
            go.Bar(
                x=type_counts["type"].to_list(),
                y=type_counts["count"].to_list(),
                marker_color=Colors.CYAN,
                text=type_counts["count"].to_list(),
                textposition="auto",
            )
        ]
    )

    fig.update_layout(
        xaxis_title="Type",
        yaxis_title="Number of Executions",
        height=500,
        plot_bgcolor="white",
    )

    return f"Plotly.newPlot('chart3', {fig.to_json()});"


def create_product_flow_heatmap(df: pl.DataFrame) -> str:
    """
    Create heatmap showing execution count matrix of Product x Flow.

    Args:
        df: Preprocessed dataframe

    Returns:
        str: JavaScript code to render the chart
    """
    products = sorted(df["productcut"].unique().to_list())
    flows = sorted(df["flow"].unique().to_list())

    # Build matrix
    matrix = []
    for flow in flows:
        row = []
        for product in products:
            count = df.filter(
                (pl.col("productcut") == product) & (pl.col("flow") == flow)
            ).height
            row.append(count)
        matrix.append(row)

    fig = go.Figure(
        data=go.Heatmap(
            z=matrix,
            x=products,
            y=flows,
            colorscale=[
                [0, Colors.WHITE],
                [0.5, Colors.CYAN],
                [1, Colors.MAGENTA],
            ],
            text=matrix,
            texttemplate="%{text}",
            textfont={"size": 12},
            colorbar=dict(title="Executions"),
        )
    )

    fig.update_layout(
        xaxis_title="Product",
        yaxis_title="Flow",
        height=500,
    )

    return f"Plotly.newPlot('chart4', {fig.to_json()});"


def create_duration_boxplot(df: pl.DataFrame) -> str:
    """
    Create box plot showing duration distribution by product.

    Args:
        df: Preprocessed dataframe

    Returns:
        str: JavaScript code to render the chart
    """
    products = sorted(df["productcut"].unique().to_list())

    fig = go.Figure()
    for i, product in enumerate(products):
        product_data = df.filter(pl.col("productcut") == product)
        fig.add_trace(
            go.Box(
                y=product_data["duration_hours"].to_list(),
                name=product,
                boxmean="sd",
                marker_color=COLOR_PALETTE[i % len(COLOR_PALETTE)],
            )
        )

    fig.update_layout(
        yaxis_title="Duration (hours)",
        height=500,
        showlegend=True,
    )

    return f"Plotly.newPlot('chart5', {fig.to_json()});"


def create_avg_duration_by_flow_chart(df: pl.DataFrame) -> str:
    """
    Create bar chart showing average execution duration by flow.

    Args:
        df: Preprocessed dataframe

    Returns:
        str: JavaScript code to render the chart
    """
    flow_avg = (
        df.group_by("flow")
        .agg(pl.col("duration_hours").mean().alias("avg_duration"))
        .sort("flow")
    )

    fig = go.Figure(
        data=[
            go.Bar(
                x=flow_avg["flow"].to_list(),
                y=flow_avg["avg_duration"].to_list(),
                marker=dict(
                    color=flow_avg["avg_duration"].to_list(),
                    colorscale=[
                        [0, Colors.GREEN],
                        [0.5, Colors.YELLOW],
                        [1, Colors.MAGENTA],
                    ],
                    showscale=True,
                    colorbar=dict(title="Hours"),
                ),
                text=[f"{val:.2f}h" for val in flow_avg["avg_duration"].to_list()],
                textposition="auto",
            )
        ]
    )

    fig.update_layout(
        xaxis_title="Flow",
        yaxis_title="Average Duration (hours)",
        height=500,
        plot_bgcolor="white",
    )

    return f"Plotly.newPlot('chart6', {fig.to_json()});"


def create_avg_duration_by_type_chart(df: pl.DataFrame) -> str:
    """
    Create bar chart showing average execution duration by type.

    Args:
        df: Preprocessed dataframe

    Returns:
        str: JavaScript code to render the chart
    """
    type_avg = (
        df.group_by("type")
        .agg(pl.col("duration_hours").mean().alias("avg_duration"))
        .sort("type")
    )

    fig = go.Figure(
        data=[
            go.Bar(
                x=type_avg["type"].to_list(),
                y=type_avg["avg_duration"].to_list(),
                marker_color=Colors.MAGENTA,
                text=[f"{val:.2f}h" for val in type_avg["avg_duration"].to_list()],
                textposition="auto",
            )
        ]
    )

    fig.update_layout(
        xaxis_title="Type",
        yaxis_title="Average Duration (hours)",
        height=500,
        plot_bgcolor="white",
    )

    return f"Plotly.newPlot('chart7', {fig.to_json()});"


def create_timeline_chart(df: pl.DataFrame) -> str:
    """
    Create scatter plot showing execution timeline over time.

    Args:
        df: Preprocessed dataframe

    Returns:
        str: JavaScript code to render the chart
    """
    df_sorted = df.sort("start_date")

    fig = go.Figure(
        data=[
            go.Scatter(
                x=df_sorted["start_date"].to_list(),
                y=df_sorted["duration_hours"].to_list(),
                mode="markers+lines",
                marker=dict(
                    size=10,
                    color=df_sorted["duration_hours"].to_list(),
                    colorscale=[[0, Colors.CYAN], [1, Colors.MAGENTA]],
                    showscale=True,
                    colorbar=dict(title="Duration (hours)"),
                ),
                line=dict(color=Colors.CYAN, width=1),
                text=[
                    f"{row['productcut']} - {row['flow']}<br>"
                    f"Duration: {row['duration_hours']:.2f}h"
                    for row in df_sorted.iter_rows(named=True)
                ],
                hovertemplate="%{text}<extra></extra>",
            )
        ]
    )

    fig.update_layout(
        xaxis_title="Start Date",
        yaxis_title="Duration (hours)",
        height=500,
        plot_bgcolor="white",
    )

    return f"Plotly.newPlot('chart8', {fig.to_json()});"


def create_top_ids_chart(df: pl.DataFrame, top_n: int = 15) -> str:
    """
    Create bar chart showing top IDs by average execution duration.
    Uses combined key: product|cut|flow_type

    Args:
        df: Preprocessed dataframe
        top_n: Number of top IDs to display

    Returns:
        str: JavaScript code to render the chart
    """
    # Create combined ID from product, cut, flow, type
    df_with_id = df.with_columns(
        pl.concat_str(
            [pl.col("productcut"), pl.col("flow"), pl.col("type")], separator="|"
        ).alias("combined_id")
    )

    id_avg = (
        df_with_id.group_by("combined_id")
        .agg(
            [
                pl.col("duration_hours").mean().alias("avg_duration"),
                pl.len().alias("count"),
            ]
        )
        .sort("avg_duration", descending=True)
        .head(top_n)
    )

    fig = go.Figure(
        data=[
            go.Bar(
                x=id_avg["combined_id"].to_list(),
                y=id_avg["avg_duration"].to_list(),
                marker=dict(
                    color=id_avg["avg_duration"].to_list(),
                    colorscale=[[0, Colors.YELLOW], [1, Colors.MAGENTA]],
                    showscale=True,
                    colorbar=dict(title="Hours"),
                ),
                text=[
                    f"{avg:.2f}h ({cnt}x)"
                    for avg, cnt in zip(
                        id_avg["avg_duration"].to_list(), id_avg["count"].to_list()
                    )
                ],
                textposition="auto",
            )
        ]
    )

    fig.update_layout(
        xaxis_title="Product|Cut|Flow_Type",
        yaxis_title="Average Duration (hours)",
        height=500,
        plot_bgcolor="white",
        xaxis={"tickangle": -45},
    )

    return f"Plotly.newPlot('chart9', {fig.to_json()});"


def create_recent_executions_table(df: pl.DataFrame, n_executions: int = 10) -> str:
    """
    Create an HTML table showing the last N executions with details.

    Args:
        df: Preprocessed dataframe
        n_executions: Number of recent executions to display

    Returns:
        str: HTML table string
    """
    # Sort by end_date (most recent first) and take the last N executions
    recent_df = df.sort("end_date", descending=True).head(n_executions)

    # Build table rows
    rows = []
    for row in recent_df.iter_rows(named=True):
        start_str = row["start_date"].strftime("%Y-%m-%d %H:%M:%S")
        end_str = row["end_date"].strftime("%Y-%m-%d %H:%M:%S")
        duration = row["duration_hours"]

        rows.append(
            f"""
            <tr>
                <td>{start_str}</td>
                <td>{end_str}</td>
                <td>{row['productcut']}</td>
                <td>{row['flow']}</td>
                <td>{row['type']}</td>
                <td>{duration:.2f}h</td>
            </tr>
            """
        )

    table_html = f"""
    <table class="executions-table">
        <thead>
            <tr>
                <th>Start Time</th>
                <th>End Time</th>
                <th>Product</th>
                <th>Flow</th>
                <th>Type</th>
                <th>Duration</th>
            </tr>
        </thead>
        <tbody>
            {''.join(rows)}
        </tbody>
    </table>
    """

    return table_html


def calculate_statistics(df: pl.DataFrame) -> Dict[str, str]:
    """
    Calculate summary statistics from the dataframe.

    Args:
        df: Preprocessed dataframe

    Returns:
        dict: Dictionary containing summary statistics
    """
    # Get the most recent execution
    latest_execution = df.sort("end_date", descending=True).head(1)
    last_execution_time = "N/A"
    if len(latest_execution) > 0:
        last_execution_time = latest_execution["end_date"][0].strftime(
            "%Y-%m-%d %H:%M:%S"
        )

    return {
        "total_executions": str(len(df)),
        "unique_products": str(df["productcut"].n_unique()),
        "unique_flows": str(df["flow"].n_unique()),
        "total_hours": f"{df['duration_hours'].sum():.1f}",
        "last_execution_time": last_execution_time,
    }


def generate_html_report(
    stats: Dict[str, str], scripts: List[str], recent_executions_html: str
) -> str:
    """
    Generate the complete HTML report with embedded charts.

    Args:
        stats: Dictionary of summary statistics
        scripts: List of JavaScript code snippets for charts
        recent_executions_html: HTML string for recent executions table

    Returns:
        str: Complete HTML document as string
    """
    html_template = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Resource Usage Analysis Report</title>
    <script src="https://cdn.plot.ly/plotly-2.26.0.min.js"></script>
    <style>
        :root {{
            --ui-font-color0: #03234b;
            --ui-font-color1: #ffd200;
            --ui-font-color2: #3cb4e6;
            --ui-font-color3: #e6007e;
            --ui-font-color4: #49b170;
            --layout-color0: #ffffff;
            --md-grey-100: #eeeff1;
        }}

        body {{
            font-family: Arial, Helvetica, sans-serif;
            margin: 0;
            padding: 20px;
            background: linear-gradient(135deg, var(--ui-font-color2) 0%,
                                        var(--ui-font-color3) 100%);
            color: var(--ui-font-color0);
        }}

        .container {{
            max-width: 1400px;
            margin: 0 auto;
            background: white;
            padding: 30px;
            border-radius: 15px;
            box-shadow: 0px 0px 12px 1px rgba(87, 87, 87, 0.2);
        }}

        h1 {{
            color: var(--ui-font-color0);
            text-align: center;
            margin-bottom: 10px;
            font-size: 2.5em;
        }}

        .subtitle {{
            text-align: center;
            color: #7f8c8d;
            margin-bottom: 30px;
            font-size: 1.2em;
        }}

        .last-execution {{
            text-align: center;
            background: linear-gradient(135deg, var(--ui-font-color2) 0%,
                                        var(--ui-font-color3) 100%);
            color: white;
            padding: 15px;
            border-radius: 10px;
            margin-bottom: 30px;
            font-size: 1.2em;
            box-shadow: 0 4px 15px rgba(0,0,0,0.2);
        }}

        .last-execution strong {{
            font-size: 1.3em;
        }}

        .stats-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 20px;
            margin-bottom: 40px;
        }}

        .stat-card {{
            background: linear-gradient(135deg, var(--ui-font-color1) 0%,
                                        #faa307 100%);
            color: var(--ui-font-color0);
            padding: 25px;
            border-radius: 10px;
            text-align: center;
            box-shadow: 0 4px 15px rgba(0,0,0,0.2);
        }}

        .stat-value {{
            font-size: 2.5em;
            font-weight: bold;
            margin-bottom: 10px;
        }}

        .stat-label {{
            font-size: 1em;
            opacity: 0.9;
        }}

        .chart-container {{
            margin-bottom: 40px;
            padding: 20px;
            background: var(--md-grey-100);
            border-radius: 10px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        }}

        .chart-title {{
            font-size: 1.5em;
            color: var(--ui-font-color0);
            margin-bottom: 15px;
            text-align: center;
            font-weight: bold;
        }}

        .executions-table {{
            width: 100%;
            border-collapse: collapse;
            margin-top: 10px;
        }}

        .executions-table th {{
            background-color: var(--ui-font-color0);
            color: white;
            padding: 12px;
            text-align: left;
            font-weight: bold;
        }}

        .executions-table td {{
            padding: 10px;
            border-bottom: 1px solid #ddd;
        }}

        .executions-table tr:nth-child(even) {{
            background-color: #f9f9f9;
        }}

        .executions-table tr:hover {{
            background-color: var(--md-grey-100);
        }}
    </style>
</head>
<body>
    <div class="container">
        <h1>📊 Resource Usage Analysis Report</h1>
        <p class="subtitle">Detailed analysis of resource usage and execution times</p>

        <div class="last-execution">
            🕐 Last Execution: <strong>{last_execution_time}</strong>
        </div>
        
        <div class="stats-grid">
            <div class="stat-card">
                <div class="stat-value">{total_executions}</div>
                <div class="stat-label">Total Executions</div>
            </div>
            <div class="stat-card">
                <div class="stat-value">{unique_products}</div>
                <div class="stat-label">Unique Products</div>
            </div>
            <div class="stat-card">
                <div class="stat-value">{unique_flows}</div>
                <div class="stat-label">Unique Flows</div>
            </div>
            <div class="stat-card">
                <div class="stat-value">{total_hours}h</div>
                <div class="stat-label">Total Time</div>
            </div>
        </div>

        <div class="chart-container">
            <div class="chart-title">📋 Recent Executions</div>
            {recent_executions_html}
        </div>

        <div class="chart-container">
            <div class="chart-title">Resource Usage by Product</div>
            <div id="chart1"></div>
        </div>
        
        <div class="chart-container">
            <div class="chart-title">Distribution by Flow</div>
            <div id="chart2"></div>
        </div>
        
        <div class="chart-container">
            <div class="chart-title">Resource Usage by Type</div>
            <div id="chart3"></div>
        </div>
        
        <div class="chart-container">
            <div class="chart-title">Product x Flow Matrix</div>
            <div id="chart4"></div>
        </div>
        
        <div class="chart-container">
            <div class="chart-title">Execution Time Distribution by Product</div>
            <div id="chart5"></div>
        </div>
        
        <div class="chart-container">
            <div class="chart-title">Average Execution Time by Flow</div>
            <div id="chart6"></div>
        </div>
        
        <div class="chart-container">
            <div class="chart-title">Average Execution Time by Type</div>
            <div id="chart7"></div>
        </div>
        
        <div class="chart-container">
            <div class="chart-title">Execution Timeline</div>
            <div id="chart8"></div>
        </div>
        
        <div class="chart-container">
            <div class="chart-title">Top 15 IDs by Average Execution Time</div>
            <div id="chart9"></div>
        </div>
    </div>
    
    <script>
        {scripts}
    </script>
</body>
</html>
"""

    return html_template.format(
        **stats, scripts="\n".join(scripts), recent_executions_html=recent_executions_html
    )


def generate_usage(
    input_file: str = "history.parquet",
    output_file: str = r"\\gpm-pe-data.gnb.st.com\ENGI_MCD_STDF\analysis_report.html",
    top_n_ids: int = 15,
    n_recent_executions: int = 10,
    verbose: bool = False,
) -> Dict[str, str]:
    """
    Generate a complete resource analysis HTML report from PARQUET data.

    This is the main function to call - it handles everything from data loading
    to report generation in a single call.

    Args:
        input_file: Path to the input PARQUET file (default: "history.parquet")
        output_file: Path where the HTML report will be saved
                    (default: "resource_analysis_report.html")
        top_n_ids: Number of top IDs to display in the IDs chart (default: 15)
        n_recent_executions: Number of recent executions to show in the table (default: 10)
        verbose: If True, prints progress messages (default: True)

    Returns:
        dict: Dictionary containing summary statistics:
            - total_executions: Total number of executions
            - unique_products: Number of unique products
            - unique_flows: Number of unique flows
            - total_hours: Total execution time in hours
            - last_execution_time: Timestamp of the last execution

    Example:
        >>> # Simple usage
        >>> stats = generate_usage()

        >>> # Custom files and settings
        >>> stats = generate_usage(
        ...     input_file="my_data.parquet",
        ...     output_file="my_report.html",
        ...     top_n_ids=20,
        ...     n_recent_executions=15,
        ...     verbose=False
        ... )
        >>> print(f"Generated report with {stats['total_executions']} executions")
        >>> print(f"Last execution: {stats['last_execution_time']}")
    """
    # Load and preprocess data
    if verbose:
        print(f"Loading and preprocessing data from '{input_file}'...")
    df = load_and_preprocess_data(input_file)

    # Generate all charts
    if verbose:
        print("Generating charts...")
    scripts = [
        create_product_usage_chart(df),
        create_flow_distribution_chart(df),
        create_type_usage_chart(df),
        create_product_flow_heatmap(df),
        create_duration_boxplot(df),
        create_avg_duration_by_flow_chart(df),
        create_avg_duration_by_type_chart(df),
        create_timeline_chart(df),
        create_top_ids_chart(df, top_n=top_n_ids),
    ]

    # Generate recent executions table
    if verbose:
        print("Generating recent executions table...")
    recent_executions_html = create_recent_executions_table(df, n_recent_executions)

    # Calculate statistics
    stats = calculate_statistics(df)

    # Generate HTML report
    if verbose:
        print("Generating HTML report...")
    html_content = generate_html_report(stats, scripts, recent_executions_html)

    # Save report
    with open(output_file, "w", encoding="utf-8") as f:
        f.write(html_content)

    # Print summary
    if verbose:
        print(f"\n✅ Report generated successfully: {output_file}")
        print(f"\n📊 Statistics:")
        print(f"   - Total executions: {stats['total_executions']}")
        print(f"   - Unique products: {stats['unique_products']}")
        print(f"   - Unique flows: {stats['unique_flows']}")
        print(f"   - Total time: {stats['total_hours']} hours")
        print(f"   - Last execution: {stats['last_execution_time']}")

    return stats


# Keep the main function for backwards compatibility
def main():
    """Main execution function for command-line usage."""
    generate_usage()


if __name__ == "__main__":
    main()
