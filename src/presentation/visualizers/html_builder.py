"""
ART.stdf - HTML Builder

This module provides HTML generation utilities for semiconductor test reports.

Key Features:
- Color-coded tables (PTR, FTR, limits)
- CSV export functionality
- Responsive table layouts
- Interactive filtering
- ST branding and styling

Author: Matteo Terranova (matteo.terranova@st.com)
Organization: STMicroelectronics - MDRF GPAM
"""

import os
from pathlib import Path
from typing import Dict, Optional
import polars as pl


def get_web_content(filename: str, web_dir: Optional[str] = None) -> str:
    """
    Read web template file content.

    Args:
        filename: Name of the template file
        web_dir: Directory containing web templates (default: src/presentation/templates)

    Returns:
        String content of the file
    """
    if web_dir is None:
        web_dir = Path(__file__).parent.parent / "templates"

    file_path = Path(web_dir) / filename

    if not file_path.exists():
        return f"<!-- Template {filename} not found -->"

    with open(file_path, "r", encoding="utf-8") as f:
        return f.read()


def get_product_image(product: str, main_path: str = ".\\STDF") -> str:
    """
    Get SVG content for product image.

    Args:
        product: Product code
        main_path: Base path to STDF data

    Returns:
        SVG content or empty string
    """
    folder_path = os.path.join(main_path, product)

    if not os.path.exists(folder_path):
        return ""

    for filename in os.listdir(folder_path):
        if filename.lower().endswith(".svg"):
            svg_path = os.path.join(folder_path, filename)
            with open(svg_path, "r", encoding="utf-8") as f:
                return f.read()

    return ""


def color_cpk(val: float) -> Optional[str]:
    """
    Color function for Cpk values.

    Args:
        val: Cpk value

    Returns:
        Color hex code or None
    """
    if val == "-" or val is None:
        return None

    try:
        val = float(val)
    except (ValueError, TypeError):
        return None

    if val < 1.2:
        return "#F23202"
    elif 1.2 <= val < 1.3:
        return "#E85D04"
    elif 1.3 <= val < 1.4:
        return "#F48C06"
    elif 1.4 <= val < 1.5:
        return "#FAA307"
    elif 1.5 <= val <= 1.6:
        return "#FFBA08"
    else:
        return None


def color_yield(val: float) -> Optional[str]:
    """
    Color function for Yield values with 20 color gradients.

    Args:
        val: Yield percentage value

    Returns:
        Color hex code or None
    """
    if val == "-" or val is None:
        return None

    try:
        # Handle both percentage strings and numeric values
        if isinstance(val, str) and val.strip().endswith("%"):
            val = float(val.strip().rstrip("%"))
        else:
            val = float(val)
    except (ValueError, TypeError):
        return None

    if val >= 97.5:
        return None
    elif val >= 95:
        return "#FFEB93"
    elif val >= 92.5:
        return "#FFE680"
    elif val >= 90:
        return "#FFE06D"
    elif val >= 87.5:
        return "#FFDB5A"
    elif val >= 85:
        return "#FFD647"
    elif val >= 82.5:
        return "#FFD134"
    elif val >= 80:
        return "#FFCC21"
    elif val >= 77.5:
        return "#FFC70E"
    elif val >= 75:
        return "#FFC200"
    elif val >= 72.5:
        return "#FFBA00"
    elif val >= 70:
        return "#FFB200"
    elif val >= 67.5:
        return "#FFAA00"
    elif val >= 65:
        return "#FFA200"
    elif val >= 62.5:
        return "#FF9A00"
    elif val >= 60:
        return "#FF9200"
    elif val >= 57.5:
        return "#FF8A00"
    elif val >= 55:
        return "#FF8200"
    elif val >= 52.5:
        return "#FF7A00"
    elif val >= 50:
        return "#FF7200"
    else:
        return "#F23202"


def generate_colored_ptrtable_html(df: pl.DataFrame) -> str:
    """
    Generate HTML table for PTR (Parametric Test Record) data with color coding.

    Features:
    - Hides index column
    - Multi-column sorting (temperature, corner, split)
    - Color coding for Cpk and Yield
    - CSV export button

    Args:
        df: Polars DataFrame with PTR statistics

    Returns:
        HTML string with embedded JavaScript
    """
    if df.is_empty():
        return "<div>Il DataFrame è vuoto.</div>"

    df_pandas = df.to_pandas()

    # Sort by temperature, corner, split
    df_sorted = df_pandas.reset_index(drop=True).sort_values(
        by=["°C", "Corner", "Split"]
    )

    # Generate base HTML table
    html_table = df_sorted.to_html(
        classes="display compact",
        table_id="colored-table",
        border=0,
        index=False,
    )

    # Container with export button
    html_content = f"""
    <div class="table-container">
        <div class="filter-container">
            <button class="btncsv btn-success" onclick="exportToCSV()">to CSV</button>
        </div>
        <div class="table-wrapper">
            {html_table}
        </div>
    </div>
    """

    # JavaScript for coloring and CSV export
    js_script = f"""
    <script src="https://code.jquery.com/jquery-3.7.1.min.js"></script>

    <script>
    // Table data for export
    const tableData = {df_sorted.to_json(orient='records')};
    const columnNames = {df_sorted.columns.tolist()};

    // Coloring functions
    function colorCpk(value) {{
        if (value === null || value === undefined || isNaN(value)) return null;
        if (value < 1.2) return "cpk-red";
        if (value >= 1.2 && value < 1.3) return "cpk-orange-dark";
        if (value >= 1.3 && value < 1.4) return "cpk-orange";
        if (value >= 1.4 && value < 1.5) return "cpk-yellow-dark";
        if (value >= 1.5 && value <= 1.6) return "cpk-yellow";
        return null;
    }}

    function colorYield(value) {{
        if (value === null || value === undefined || isNaN(value)) return null;
        if (value >= 97.5) return null;
        if (value >= 95) return "yield-1";
        if (value >= 92.5) return "yield-2";
        if (value >= 90) return "yield-3";
        if (value >= 87.5) return "yield-4";
        if (value >= 85) return "yield-5";
        if (value >= 82.5) return "yield-6";
        if (value >= 80) return "yield-7";
        if (value >= 77.5) return "yield-8";
        if (value >= 75) return "yield-9";
        if (value >= 72.5) return "yield-10";
        if (value >= 70) return "yield-11";
        if (value >= 67.5) return "yield-12";
        if (value >= 65) return "yield-13";
        if (value >= 62.5) return "yield-14";
        if (value >= 60) return "yield-15";
        if (value >= 57.5) return "yield-16";
        if (value >= 55) return "yield-17";
        if (value >= 52.5) return "yield-18";
        if (value >= 50) return "yield-19";
        return "yield-critical";
    }}

    // Apply coloring to specific cells
    function applySpecificColoring() {{
        $('#colored-table tbody tr').each(function(rowIndex) {{
            $(this).find('td').each(function(colIndex) {{
                const columnName = columnNames[colIndex];
                const cellValue = parseFloat($(this).text());
                let colorClass = null;

                if (columnName === 'Cpk') {{
                    colorClass = colorCpk(cellValue);
                }} else if (columnName === 'Yield') {{
                    colorClass = colorYield(cellValue);
                }}

                if (colorClass) {{
                    $(this).addClass(colorClass);
                }}
            }});
        }});
    }}

    function exportToCSV() {{
        let csvContent = columnNames.join(',') + '\\n';
        tableData.forEach(row => {{
            const rowArray = columnNames.map(col => {{
                const value = row[col];
                if (typeof value === 'string' && (value.includes(',') || value.includes('"'))) {{
                    return '"' + value.replace(/"/g, '""') + '"';
                }}
                return value !== null && value !== undefined ? value : '';
            }});
            csvContent += rowArray.join(',') + '\\n';
        }});

        const titleElement = document.querySelector('.gtitle');
        const fileName = titleElement ? titleElement.textContent.trim() : 'ptr_table';

        const blob = new Blob([csvContent], {{ type: 'text/csv;charset=utf-8;' }});
        const link = document.createElement('a');
        const url = URL.createObjectURL(blob);
        link.setAttribute('href', url);
        link.setAttribute('download', `${{fileName}}.csv`);
        link.style.visibility = 'hidden';
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
        URL.revokeObjectURL(url);
    }}

    $(document).ready(function() {{
        applySpecificColoring();
    }});
    </script>
    """

    return html_content + js_script


def generate_colored_ftrtable_html(df: pl.DataFrame) -> str:
    """
    Generate HTML table for FTR (Functional Test Record) data.

    Features:
    - Reorganizes columns (°C, Corner, Metric first)
    - CSV export button
    - No index column

    Args:
        df: Polars DataFrame with FTR statistics

    Returns:
        HTML string with embedded JavaScript
    """
    if df.is_empty():
        return "<div>Il DataFrame è vuoto.</div>"

    df_pandas = df.to_pandas()
    priority_cols = ["°C", "Corner", "Metric"]
    remaining_cols = sorted([col for col in df.columns if col not in priority_cols])
    new_order = priority_cols + remaining_cols
    df_pandas = df_pandas[new_order]

    # Generate HTML table
    html_table = df_pandas.to_html(
        classes="display compact",
        table_id="colored-table",
        border=0,
        index=False,
    )

    html_content = f"""
    <div class="table-container">
        <div class="filter-container">
            <button class="btncsv btn-success" onclick="exportToCSV()">to CSV</button>
        </div>
        <div class="table-wrapper">
            {html_table}
        </div>
    </div>
    """

    js_script = f"""
    <script src="https://code.jquery.com/jquery-3.7.1.min.js"></script>

    <script>
    const tableData = {df_pandas.to_json(orient='records')};
    const columnNames = {df_pandas.columns.tolist()};

    function exportToCSV() {{
        let csvContent = columnNames.join(',') + '\\n';
        tableData.forEach(row => {{
            const rowArray = columnNames.map(col => {{
                const value = row[col];
                if (typeof value === 'string' && (value.includes(',') || value.includes('"'))) {{
                    return '"' + value.replace(/"/g, '""') + '"';
                }}
                return value;
            }});
            csvContent += rowArray.join(',') + '\\n';
        }});

        const titleElement = document.querySelector('.gtitle');
        const fileName = titleElement ? titleElement.textContent.trim() : 'ftr_table';

        const blob = new Blob([csvContent], {{ type: 'text/csv;charset=utf-8;' }});
        const link = document.createElement('a');
        const url = URL.createObjectURL(blob);
        link.setAttribute('href', url);
        link.setAttribute('download', `${{fileName}}.csv`);
        link.style.visibility = 'hidden';
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
        URL.revokeObjectURL(url);
    }}
    </script>
    """

    return html_content + js_script


def generate_colored_limittable_html(df: pl.DataFrame, tname: str) -> str:
    """
    Generate HTML table for limits data with link to chart.

    Args:
        df: Polars DataFrame with limit statistics
        tname: Test name

    Returns:
        HTML string with embedded JavaScript
    """
    if df.is_empty():
        return "<div>Il DataFrame è vuoto.</div>"

    df_pandas = df.to_pandas()

    html_table = df_pandas.to_html(
        classes="display compact",
        table_id="colored-table",
        border=0,
        index=False,
    )

    html_content = f"""
    <div class="table-container">
        <div class="filter-container">
            <a href="./{tname.upper().replace(":","_")}.html" class="btncsv btn-success">to Chart</a>
            <button class="btncsv btn-success" onclick="exportToCSV()">to CSV</button>
        </div>
        <div class="table-wrapper">
            {html_table}
        </div>
    </div>
    """

    js_script = f"""
    <script src="https://code.jquery.com/jquery-3.7.1.min.js"></script>

    <script>
    const tableData = {df_pandas.to_json(orient='records')};
    const columnNames = {df_pandas.columns.tolist()};

    function colorCpk(value) {{
        if (value === null || value === undefined || isNaN(value)) return null;
        if (value < 1.2) return "cpk-red";
        if (value >= 1.2 && value < 1.3) return "cpk-orange-dark";
        if (value >= 1.3 && value < 1.4) return "cpk-orange";
        if (value >= 1.4 && value < 1.5) return "cpk-yellow-dark";
        if (value >= 1.5 && value <= 1.6) return "cpk-yellow";
        return null;
    }}

    function colorYield(value) {{
        if (value === null || value === undefined || isNaN(value)) return null;
        if (value >= 97.5) return null;
        if (value >= 95) return "yield-1";
        if (value >= 92.5) return "yield-2";
        if (value >= 90) return "yield-3";
        if (value >= 87.5) return "yield-4";
        if (value >= 85) return "yield-5";
        if (value >= 82.5) return "yield-6";
        if (value >= 80) return "yield-7";
        if (value >= 77.5) return "yield-8";
        if (value >= 75) return "yield-9";
        if (value >= 72.5) return "yield-10";
        if (value >= 70) return "yield-11";
        if (value >= 67.5) return "yield-12";
        if (value >= 65) return "yield-13";
        if (value >= 62.5) return "yield-14";
        if (value >= 60) return "yield-15";
        if (value >= 57.5) return "yield-16";
        if (value >= 55) return "yield-17";
        if (value >= 52.5) return "yield-18";
        if (value >= 50) return "yield-19";
        return "yield-critical";
    }}

    function applySpecificColoring() {{
        $('#colored-table tbody tr').each(function(rowIndex) {{
            $(this).find('td').each(function(colIndex) {{
                const columnName = columnNames[colIndex];
                const cellValue = parseFloat($(this).text());
                let colorClass = null;

                if (columnName === 'Cpk') {{
                    colorClass = colorCpk(cellValue);
                }} else if (columnName === 'Yield') {{
                    colorClass = colorYield(cellValue);
                }}

                if (colorClass) {{
                    $(this).addClass(colorClass);
                }}
            }});
        }});
    }}

    function exportToCSV() {{
        let csvContent = columnNames.join(',') + '\\n';
        tableData.forEach(row => {{
            const rowArray = columnNames.map(col => {{
                const value = row[col];
                if (typeof value === 'string' && (value.includes(',') || value.includes('"'))) {{
                    return '"' + value.replace(/"/g, '""') + '"';
                }}
                return value !== null && value !== undefined ? value : '';
            }});
            csvContent += rowArray.join(',') + '\\n';
        }});

        const titleElement = document.querySelector('.gtitle');
        const fileName = titleElement ? titleElement.textContent.trim() : 'limit_table';

        const blob = new Blob([csvContent], {{ type: 'text/csv;charset=utf-8;' }});
        const link = document.createElement('a');
        const url = URL.createObjectURL(blob);
        link.setAttribute('href', url);
        link.setAttribute('download', `${{fileName}}.csv`);
        link.style.visibility = 'hidden';
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
        URL.revokeObjectURL(url);
    }}

    $(document).ready(function() {{
        applySpecificColoring();
    }});
    </script>
    """

    return html_content + js_script
