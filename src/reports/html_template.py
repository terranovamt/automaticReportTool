"""
HTML Report Template Generator

Provides clean, professional HTML templates for STDF reports.

Author: Matteo Terranova (matteo.terranova@st.com)
Organization: STMicroelectronics - MDRF GPAM
"""

from typing import List, Dict, Optional
from datetime import datetime


class HTMLTemplate:
    """Generate professional HTML reports."""

    # ST Colors
    ST_BLUE = "#03234B"
    ST_CYAN = "#3CB4E6"
    ST_RED = "#E6007E"
    ST_YELLOW = "#FFD200"
    ST_GREEN = "#49B170"

    @staticmethod
    def get_base_css() -> str:
        """Get base CSS for reports."""
        return """
        <style>
            * {
                margin: 0;
                padding: 0;
                box-sizing: border-box;
            }

            body {
                font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                background-color: #f5f5f5;
                color: #333;
                line-height: 1.6;
            }

            .container {
                max-width: 1400px;
                margin: 0 auto;
                padding: 20px;
            }

            .header {
                background: linear-gradient(135deg, #03234B 0%, #3CB4E6 100%);
                color: white;
                padding: 30px;
                border-radius: 10px;
                margin-bottom: 30px;
                box-shadow: 0 4px 6px rgba(0,0,0,0.1);
            }

            .header h1 {
                margin-bottom: 10px;
                font-size: 2.5em;
            }

            .header-info {
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
                gap: 15px;
                margin-top: 20px;
            }

            .info-item {
                background: rgba(255,255,255,0.1);
                padding: 10px 15px;
                border-radius: 5px;
            }

            .info-label {
                font-size: 0.85em;
                opacity: 0.9;
            }

            .info-value {
                font-size: 1.1em;
                font-weight: bold;
                margin-top: 5px;
            }

            .section {
                background: white;
                padding: 25px;
                margin-bottom: 25px;
                border-radius: 10px;
                box-shadow: 0 2px 4px rgba(0,0,0,0.05);
            }

            .section-title {
                color: #03234B;
                font-size: 1.8em;
                margin-bottom: 20px;
                padding-bottom: 10px;
                border-bottom: 3px solid #3CB4E6;
            }

            .chart-container {
                margin: 20px 0;
                padding: 15px;
                background: #fafafa;
                border-radius: 8px;
            }

            .summary-grid {
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
                gap: 20px;
                margin: 20px 0;
            }

            .summary-card {
                background: white;
                padding: 20px;
                border-radius: 8px;
                border-left: 4px solid #3CB4E6;
                box-shadow: 0 2px 4px rgba(0,0,0,0.05);
            }

            .summary-card-title {
                font-size: 0.9em;
                color: #666;
                margin-bottom: 10px;
            }

            .summary-card-value {
                font-size: 2em;
                font-weight: bold;
                color: #03234B;
            }

            .summary-card-unit {
                font-size: 0.9em;
                color: #888;
                margin-left: 5px;
            }

            .table-container {
                overflow-x: auto;
                margin: 20px 0;
            }

            table {
                width: 100%;
                border-collapse: collapse;
                background: white;
            }

            thead {
                background: #03234B;
                color: white;
            }

            th, td {
                padding: 12px 15px;
                text-align: left;
                border-bottom: 1px solid #ddd;
            }

            tbody tr:hover {
                background-color: #f5f5f5;
            }

            .status-pass {
                color: #49B170;
                font-weight: bold;
            }

            .status-fail {
                color: #E6007E;
                font-weight: bold;
            }

            .footer {
                text-align: center;
                padding: 20px;
                color: #666;
                font-size: 0.9em;
                margin-top: 40px;
            }

            .badge {
                display: inline-block;
                padding: 5px 10px;
                border-radius: 4px;
                font-size: 0.85em;
                font-weight: bold;
            }

            .badge-success {
                background-color: #49B170;
                color: white;
            }

            .badge-warning {
                background-color: #FFD200;
                color: #333;
            }

            .badge-danger {
                background-color: #E6007E;
                color: white;
            }

            .progress-bar {
                width: 100%;
                height: 30px;
                background-color: #e0e0e0;
                border-radius: 15px;
                overflow: hidden;
                margin: 10px 0;
            }

            .progress-fill {
                height: 100%;
                background: linear-gradient(90deg, #49B170 0%, #3CB4E6 100%);
                display: flex;
                align-items: center;
                justify-content: center;
                color: white;
                font-weight: bold;
                transition: width 0.3s ease;
            }

            @media print {
                body {
                    background: white;
                }
                .section {
                    box-shadow: none;
                    border: 1px solid #ddd;
                    page-break-inside: avoid;
                }
            }
        </style>
        """

    @classmethod
    def create_report(
        cls,
        title: str,
        report_type: str,
        parameter: Dict,
        summary_data: Dict,
        charts_html: List[str],
        tables_html: Optional[List[str]] = None
    ) -> str:
        """
        Create complete HTML report.

        Args:
            title: Report title
            report_type: Type of report (CONDITION, CHAR, YIELD, etc.)
            parameter: Test parameters dict
            summary_data: Summary statistics dict
            charts_html: List of chart HTML divs
            tables_html: Optional list of table HTML

        Returns:
            Complete HTML string
        """
        # Header info
        header_info = cls._create_header_info(parameter)

        # Summary cards
        summary_cards = cls._create_summary_cards(summary_data)

        # Charts section
        charts_section = cls._create_charts_section(charts_html)

        # Tables section
        tables_section = ""
        if tables_html:
            tables_section = cls._create_tables_section(tables_html)

        # Complete HTML
        html = f"""
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>{title}</title>
            <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
            {cls.get_base_css()}
        </head>
        <body>
            <div class="container">
                <!-- Header -->
                <div class="header">
                    <h1>{title}</h1>
                    <div style="font-size: 1.2em; margin-top: 10px;">
                        Report Type: <strong>{report_type}</strong>
                    </div>
                    <div class="header-info">
                        {header_info}
                    </div>
                </div>

                <!-- Summary Section -->
                <div class="section">
                    <h2 class="section-title">Summary</h2>
                    <div class="summary-grid">
                        {summary_cards}
                    </div>
                </div>

                <!-- Charts Section -->
                {charts_section}

                <!-- Tables Section -->
                {tables_section}

                <!-- Footer -->
                <div class="footer">
                    <p>Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
                    <p>STMicroelectronics - MDRF GPAM - ART.stdf Report Generator</p>
                </div>
            </div>
        </body>
        </html>
        """

        return html

    @classmethod
    def _create_header_info(cls, parameter: Dict) -> str:
        """Create header info grid."""
        info_items = []

        info_map = {
            'CUT': 'Product',
            'FLOW': 'Flow',
            'LOT': 'Lot',
            'WAFER': 'Wafer',
            'temperature': 'Temperature',
            'PACKAGE': 'Package'
        }

        for key, label in info_map.items():
            if key in parameter and parameter[key]:
                value = parameter[key]
                if key == 'temperature':
                    value = f"{value}°C"
                info_items.append(f"""
                    <div class="info-item">
                        <div class="info-label">{label}</div>
                        <div class="info-value">{value}</div>
                    </div>
                """)

        return "\n".join(info_items)

    @classmethod
    def _create_summary_cards(cls, summary_data: Dict) -> str:
        """Create summary cards."""
        cards = []

        for key, value in summary_data.items():
            # Determine unit and formatting
            unit = ""
            formatted_value = value

            if isinstance(value, float):
                formatted_value = f"{value:.2f}"

            if "yield" in key.lower() or "percentage" in key.lower():
                unit = "%"
            elif "count" in key.lower():
                formatted_value = f"{int(value):,}"

            cards.append(f"""
                <div class="summary-card">
                    <div class="summary-card-title">{key.replace('_', ' ').title()}</div>
                    <div class="summary-card-value">
                        {formatted_value}
                        <span class="summary-card-unit">{unit}</span>
                    </div>
                </div>
            """)

        return "\n".join(cards)

    @classmethod
    def _create_charts_section(cls, charts_html: List[str]) -> str:
        """Create charts section."""
        if not charts_html:
            return ""

        charts_divs = "\n".join([
            f'<div class="chart-container">{chart}</div>'
            for chart in charts_html
        ])

        return f"""
        <div class="section">
            <h2 class="section-title">Charts</h2>
            {charts_divs}
        </div>
        """

    @classmethod
    def _create_tables_section(cls, tables_html: List[str]) -> str:
        """Create tables section."""
        if not tables_html:
            return ""

        tables_divs = "\n".join([
            f'<div class="table-container">{table}</div>'
            for table in tables_html
        ])

        return f"""
        <div class="section">
            <h2 class="section-title">Data Tables</h2>
            {tables_divs}
        </div>
        """

    @staticmethod
    def dataframe_to_html_table(df, max_rows: int = 100) -> str:
        """
        Convert Polars DataFrame to HTML table.

        Args:
            df: Polars DataFrame
            max_rows: Maximum rows to display

        Returns:
            HTML table string
        """
        import polars as pl

        # Limit rows if needed
        if len(df) > max_rows:
            df = df.head(max_rows)

        # Convert to pandas for easy HTML conversion
        df_pd = df.to_pandas()

        return df_pd.to_html(
            index=False,
            classes='',
            border=0,
            escape=False
        )
