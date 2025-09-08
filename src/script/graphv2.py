import numpy as np
import polars as pl
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from plotly.offline import plot
from itertools import product
import json
import itables


STblue = "#03234B"
STcyan = "#3CB4E6"
STred = "#E6007E"
STyellow = "#FFD200"
STgreen = "#49B170"
STViolet = "#8C0078"
STdarkgreen = "#04572F"
color_light = "#FFFFFF"
color_dark = "#EEEFF1"


def freedman_diaconis_rule(data):
    """
    Calculate optimal number of bins using Freedman-Diaconis rule
    Input: Polars Series or expression
    """
    if isinstance(data, pl.Series):
        # Remove nulls and infinite values
        clean_data = data.drop_nulls().filter(pl.col("").is_finite())
    else:
        # If it's a DataFrame column
        clean_data = data.drop_nulls().filter(pl.col(data.name).is_finite())

    if len(clean_data) == 0:
        return 10

    q25, q75 = clean_data.quantile(0.01), clean_data.quantile(0.99)
    iqr = q75 - q25

    if iqr == 0:
        return 10

    n = len(clean_data)
    bin_width = 2 * iqr / np.cbrt(n)
    data_range = clean_data.max() - clean_data.min()

    return int(np.ceil(data_range / bin_width)) * 5


def std_hist(td, ll, ul, units, STPalette, limit_color="#E6007E"):
    # Convert to numpy for histogram calculation if needed
    nbins_fd = freedman_diaconis_rule(td.select("Value").to_series())
    corners = td.select("Corner").unique().sort("Corner").to_series().to_list()

    # Calculate global limits for X axis
    global_min = td.select("Value").min().item()
    global_max = td.select("Value").max().item()

    # Create initial empty figure
    fig = go.Figure()

    # Get all unique temperatures
    all_temps = td.select("°C").unique().sort("°C").to_series().to_list()
    added_to_legend = set()

    # For each corner, add histograms
    for i, corner in enumerate(corners):
        corner_data = td.filter(pl.col("Corner") == corner)

        # For each global temperature, create histogram (even if empty)
        for temp in all_temps:
            temp_data = corner_data.filter(pl.col("°C") == temp)

            # Check if show this temperature in legend
            show_in_legend = temp not in added_to_legend
            if show_in_legend:
                added_to_legend.add(temp)

            # Add histogram
            values = (
                temp_data.select("Value").to_series().to_list()
                if len(temp_data) > 0
                else []
            )

            fig.add_trace(
                go.Histogram(
                    x=values,
                    name=f"{temp}°C",
                    marker_color=STPalette.get(
                        temp, STPalette.get(str(temp), "#1f77b4")
                    ),
                    opacity=0.7,
                    visible=(
                        True if i == 0 else False
                    ),  # Only first corner visible initially
                    legendgroup=f"temp_{temp}",  # Group by temperature
                )
            )

    # Calculate how many traces per corner
    temps_per_corner = len(all_temps)

    # Create steps for slider
    steps = []
    for i, corner in enumerate(corners):
        # Calculate which traces to make visible for this corner
        visible = [False] * len(fig.data)
        start_idx = i * temps_per_corner
        end_idx = start_idx + temps_per_corner

        for j in range(start_idx, min(end_idx, len(visible))):
            visible[j] = True

        step = dict(
            method="update",
            args=[{"visible": visible}, {"title": f"Histogram - Corner: {corner}"}],
            label=str(corner),
        )
        steps.append(step)

    # Add vertical lines if needed
    if ul != 0 and ll != 0:
        fig.add_vline(
            ul,
            line_color=limit_color,
            line_dash="dash",
            annotation_text="Upper Limit",
            annotation_position="top",
        )
        fig.add_vline(
            ll,
            line_color=limit_color,
            line_dash="dash",
            annotation_text="Lower Limit",
            annotation_position="top",
        )

    # Configure layout with slider
    fig.update_layout(
        sliders=[
            dict(
                active=0,
                currentvalue={"prefix": "Corner: "},
                pad={"t": 50},
                steps=steps,
                x=0.1,
                len=0.8,
                xanchor="left",
                y=0,
                yanchor="top",
            )
        ],
        xaxis=dict(
            title=f"Value ({units})",
            range=[global_min * 0.95, global_max * 1.05],
        ),
        yaxis=dict(title="Count"),
        title=f"Histogram - Corner: {corners[0]}",
        template="plotly_white",
        showlegend=True,
        barmode="overlay",
    )

    return fig


def combined_hist_heatmap(
    td,
    ll,
    ul,
    units,
    STPalette,
    xwafer,
    ywafer,
    gradientcolor=["#03234B", "#3CB4E6", "#FFD200", "#E6007E"],
    limit_color="#E6007E",
):
    """
    Creates dashboard with organized and responsive layout using Polars DataFrame
    """
    # Initial parameters
    nbins_fd = freedman_diaconis_rule(td.select("Value").to_series())
    corners = td.select("Corner").unique().sort("Corner").to_series().to_list()
    all_temps = td.select("°C").unique().sort("°C").to_series().to_list()

    # Default values
    default_corner = "TTTT" if "TTTT" in corners else corners[0]
    default_temp = "30" if "30" in all_temps else all_temps[0]

    default_corner_idx = corners.index(default_corner)
    default_temp_idx = all_temps.index(default_temp)

    # Global limits for histogram
    global_min = td.select("Value").min().item()
    global_max = td.select("Value").max().item()

    # Layout: 2x1 grid
    fig = make_subplots(
        rows=2,
        row_heights=[0.2, 0.6],
        horizontal_spacing=0.05,
    )

    # =========================
    # PART 1: HISTOGRAM (Row 1, Col 1)
    # =========================

    for i, corner in enumerate(corners):
        corner_data = td.filter(pl.col("Corner") == corner)

        for temp in all_temps:
            temp_data = corner_data.filter(pl.col("°C") == temp)
            values = (
                temp_data.select("Value").to_series().to_list()
                if len(temp_data) > 0
                else []
            )

            fig.add_trace(
                go.Histogram(
                    x=values,
                    name=f"{temp}°C",
                    marker_color=STPalette.get(
                        temp, STPalette.get(str(temp), "#1f77b4")
                    ),
                    opacity=0.6,
                    visible=True if i == default_corner_idx else False,
                    legendgroup=f"temp_{temp}",
                ),
                row=1,
                col=1,
            )

    # =========================
    # PART 2: HEATMAP (Row 2, Col 1)
    # =========================

    # Prepare heatmap data with padding
    std_dev = td.select("Value").std().item()
    step = std_dev / 10 if std_dev >= 1e-5 else std_dev

    # Create ranges for X and Y coordinates
    x_range = np.arange(
        td.select("X_COORD").min().item() - 1, td.select("X_COORD").max().item() + 2
    )
    y_range = np.arange(
        td.select("Y_COORD").min().item() - 1, td.select("Y_COORD").max().item() + 2
    )

    # Create meshgrid for all combinations
    x_mesh, y_mesh = np.meshgrid(x_range, y_range)

    # Create additional data with all combinations
    additional_data = pl.DataFrame(
        {
            "X_COORD": x_mesh.flatten(),
            "Y_COORD": y_mesh.flatten(),
            "Value": [None] * len(x_mesh.flatten()),
            "Corner": [corners[0]] * len(x_mesh.flatten()),
            "°C": [all_temps[0]] * len(x_mesh.flatten()),
        }
    )

    # Concatenate with original data
    td_extended = pl.concat([td, additional_data])

    # For each corner-temperature combination, create heatmap
    for i, corner in enumerate(corners):
        for j, temp in enumerate(all_temps):
            filtered_data = td_extended.filter(
                (pl.col("Corner") == corner) & (pl.col("°C") == temp)
            )

            if len(filtered_data) == 0:
                filtered_data = additional_data.clone()

            visible = i == default_corner_idx and j == default_temp_idx

            # Calculate min/max for dynamic gradient
            actual_data = td.filter(
                (pl.col("Corner") == corner) & (pl.col("°C") == temp)
            )
            if len(actual_data) > 0:
                zmin_val = actual_data.select("Value").min().item()
                zmax_val = actual_data.select("Value").max().item()
            else:
                zmin_val = td.select("Value").min().item()
                zmax_val = td.select("Value").max().item()

            fig.add_trace(
                go.Heatmap(
                    z=filtered_data.select("Value").to_series().to_list(),
                    x=filtered_data.select("X_COORD").to_series().to_list(),
                    y=filtered_data.select("Y_COORD").to_series().to_list(),
                    colorscale=gradientcolor,
                    colorbar=dict(
                        title=f"Value ({units})",
                        x=0.9,
                        y=0.25,
                        len=0.5,
                        yanchor="middle",
                    ),
                    hoverongaps=False,
                    hovertemplate="x: %{x}<br>y: %{y}<br>Value: %{z:.2f}<br>",
                    name="",
                    zmin=zmin_val,
                    zmax=zmax_val,
                    visible=visible,
                    showlegend=False,
                ),
                row=2,
                col=1,
            )

    # Rest of the function remains similar but with Polars-specific operations
    # [Button creation and layout configuration code would continue here...]

    # Add limit lines to histogram
    if ul != 0 and ll != 0:
        fig.add_vline(
            ul,
            line_color=limit_color,
            line_dash="dash",
            row=1,
            col=1,
        )
        fig.add_vline(
            ll,
            line_color=limit_color,
            line_dash="dash",
            row=1,
            col=1,
        )

    fig.update_layout(
        legend=dict(
            x=1,
            y=1,
            traceorder="reversed",
            font=dict(size=12, color=STblue),
        )
    )

    # Configure main layout
    fig.update_layout(
        autosize=True,
        template="plotly_white",
        showlegend=True,
        barmode="overlay",
        margin=dict(l=50, r=120, t=120, b=50),
        title_text=str(td.select("TestName").item(0, 0)),
        title_x=0.5,
        title_font=dict(size=48),
        title_pad=dict(t=10, r=0, b=15, l=0),
        height=800,
    )

    return fig


def boxploth(
    td,
    ll,
    ul,
    units,
    STPalette,
    limit_color="#E6007E",
):
    # Initial parameters - sort temperatures numerically
    all_temps = td.select("°C").unique().sort("°C").to_series().to_list()
    all_temps = sorted(all_temps, key=lambda x: float(x))

    all_split = (
        td.select("Split").unique().sort("Split").to_series().to_list()
        if "Split" in td.columns
        else ["Default"]
    )

    # Calculate number of splits and required rows
    n_splits = len(all_split)
    total_rows = n_splits if n_splits > 0 else 1

    # Subplot specification for single column
    subplot_specs = [[{"type": "xy"}] for _ in range(total_rows)]

    # Create figure with single column
    fig = make_subplots(
        rows=total_rows,
        cols=1,
        vertical_spacing=0.15/total_rows,
        specs=subplot_specs,
    )

    # =========================
    # CALCULATE GLOBAL X-AXIS LIMITS
    # =========================
    Q1 = td.select("Value").quantile(0.25)["Value"][0]
    Q3 = td.select("Value").quantile(0.75)["Value"][0]
    IQR = Q3 - Q1

    global_min = Q1 - 1.5 * IQR
    global_max = Q3 + 1.5 * IQR

    # Add small margin (3% of range)
    value_range = global_max - global_min
    margin = value_range * 0.03
    x_min = global_min - margin
    x_max = global_max + margin

    corner_order = [
        "SSTT",
        "SSXX",
        "S1TT",
        "SFTT",
        "TTTT",
        "FSTT",
        "F1TT",
        "FFMM",
        "FFTT",
    ]

    corners = td.select("Corner").unique().sort("Corner").to_series().to_list()
    corners = sorted(
        corners,
        key=lambda c: (
            corner_order.index(c) if c in corner_order else len(corner_order)
        ),
    )

    # =========================
    # PART 1: BOX PLOT with facet for Split
    # =========================
    for split_idx, split in enumerate(all_split):
        split_data = (
            td.filter(pl.col("Split") == split) if "Split" in td.columns else td
        )

        for j, temp in enumerate(all_temps):
            temp_split_data = split_data.filter(pl.col("°C") == temp)

            if len(temp_split_data) > 0:
                fig.add_trace(
                    go.Box(
                        x=temp_split_data.select("Value").to_series().to_list(),
                        y=temp_split_data.select("Corner").to_series().to_list(),
                        name=f"{temp}°C",
                        marker_color=STPalette.get(
                            str(temp), STPalette.get(temp, "#1f77b4")
                        ),
                        orientation="h",
                        boxpoints="outliers",
                        visible=True,
                        legendgroup=f"temp_{temp}",
                        showlegend=(split_idx == 0),
                        offsetgroup=f"split_{split}_temp_{temp}",
                        notched=True,
                        boxmean="sd",
                    ),
                    row=split_idx + 1,
                    col=1,
                )

    # =========================
    # LIMIT LINES
    # =========================
    if ul != 0 and ll != 0:
        for split_idx in range(n_splits):
            fig.add_vline(
                ul, line_color=limit_color, line_dash="dash", row=split_idx + 1, col=1
            )
            fig.add_vline(
                ll, line_color=limit_color, line_dash="dash", row=split_idx + 1, col=1
            )
            # Add margin
            value_range = global_max - global_min
            margin = value_range * 0.03
            x_min = min(ll, global_min) - margin
            x_max = max(ul, global_max) + margin

    # =========================
    # FINAL LAYOUT
    # =========================
    base_height = 1200 + max(0, (n_splits - 2) * 1200)

    fig.update_layout(
        autosize=True,
        template="plotly_white",
        showlegend=True,
        barmode="overlay",
        margin=dict(l=50, r=120, t=120, b=50),
        height=base_height,
        title_text=str(td.select("TestName").item(0, 0)),
        title_x=0.5,
        title_font=dict(size=24),
        title_pad=dict(t=10, r=0, b=15, l=0),
        boxmode="group",
        violinmode="group",
    )

    # =========================
    # CONFIGURE AXES WITH UNIFIED LIMITS
    # =========================

    for split_idx in range(n_splits):
        fig.update_xaxes(
            title=(f"Value ({units}) - Split: {all_split[split_idx]}"
        if units != ""
        else f"Result at Split: {all_split[split_idx]}"),
            range=[x_min, x_max],  # Apply unified limits
            row=split_idx + 1,
            col=1,
        )
        fig.update_yaxes(
            title="Corner",
            row=split_idx + 1,
            col=1,
            categoryorder="array",
            categoryarray=corners,
        )

    return fig


def scatter(
    td,
    STPalette,
):
    # Calculate total tests for each Split/°C/Corner combination
    total_counts = (
        td.group_by(["Split", "°C", "Corner"])
        .agg(pl.count("RESULT").alias("Total"))
        .sort(["Split", "°C", "Corner"])
    )

    # Calculate passed tests (RESULT = 1) for each combination
    pass_counts = (
        td.filter(pl.col("RESULT") == 1)
        .group_by(["Split", "°C", "Corner"])
        .agg(pl.count("RESULT").alias("Pass"))
        .sort(["Split", "°C", "Corner"])
    )

    # Merge data to calculate Yield
    df = (
        total_counts.join(pass_counts, on=["Split", "°C", "Corner"], how="left")
        .with_columns(pl.col("Pass").fill_null(0))  # Fill NaN with 0 if no passed tests
        .with_columns(
            ((pl.col("Pass") / pl.col("Total")) * 100).alias(
                "Yield"
            )  # Calculate Yield as percentage
        )
    )

    # Custom template
    STtemplate = go.layout.Template()
    STtemplate.layout = go.Layout(
        plot_bgcolor="white",
        xaxis=dict(gridcolor="#ebf0f8", zerolinecolor="#dee3ea"),
        yaxis=dict(gridcolor="#ebf0f8", zerolinecolor="#dee3ea"),
    )

    # Get unique values
    corner_order = [
        "SSTT",
        "SSXX",
        "S1TT",
        "SFTT",
        "TTTT",
        "FSTT",
        "F1TT",
        "FFMM",
        "FFTT",
    ]

    corners = df.select("Corner").unique().sort("Corner").to_series().to_list()
    corners = sorted(
        corners,
        key=lambda c: corner_order.index(c) if c in corner_order else len(corner_order),
    )

    temperatures = [
        str(temp)
        for temp in sorted(
            [int(temp) for temp in df.select("°C").unique().to_series().to_list()]
        )
    ]
    n_corners = len(corners)

    # Create subplot with facet for Corner
    fig = make_subplots(
        rows=n_corners,
        cols=1,
        subplot_titles=[f"Corner: {corner}" for corner in corners],
    )

    # Use STPalette for temperature colors
    color_map = {
        temp: STPalette.get(temp, list(STPalette.values())[i % len(STPalette)])
        for i, temp in enumerate(temperatures)
    }

    # Add traces for each Corner/Temperature combination
    for i, corner in enumerate(corners):
        corner_data = df.filter(pl.col("Corner") == corner)

        for temp in temperatures:
            temp_data = corner_data.filter(pl.col("°C") == temp)

            if len(temp_data) > 0:
                fig.add_trace(
                    go.Scatter(
                        x=temp_data.select("Split").to_series().to_list(),
                        y=temp_data.select("Yield").to_series().to_list(),
                        mode="markers+lines",
                        name=f"{temp}°C",
                        line=dict(color=color_map[temp]),
                        marker=dict(color=color_map[temp]),
                        showlegend=(i == 0),  # Show legend only for first subplot
                        legendgroup=f"{temp}°C",  # Group legends
                        hovertemplate="Split: %{x}<br>Yield: %{y:.1f}%<br>Temp: "
                        + f"{temp}°C",
                    ),
                    row=i + 1,
                    col=1,
                )

    # Update layout
    fig.update_layout(
        height=300 * n_corners,
        template=STtemplate,
        hovermode="x unified",
        showlegend=True,
        barmode="overlay",
        margin=dict(l=50, r=120, t=120, b=50),
        title_text=str(td.select("TestName").item(0, 0)),
        title_x=0.5,
        title_font=dict(size=24),
        title_pad=dict(t=10, r=0, b=15, l=0),
    )

    # Update Y axes with 0-100% range for Yield
    fig.update_yaxes(title_text="Yield (%)", range=[-5, 105])

    # Update X axis only for last subplot
    fig.update_xaxes(title_text="Split", row=n_corners, col=1)

    return fig


# Color functions remain the same as they work with individual values
def color_cpk(val):
    """Color function for Cpk values"""
    if val == "-":
        return
    try:
        val = float(val)
    except (ValueError, TypeError):
        return ""

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
        return


def color_yield(val):
    """Color function for Yield values with 20 color gradients from 99 to 50"""
    if val == "-":
        return None
    try:
        # Handle both percentage strings and numeric values
        if isinstance(val, str) and val.strip().endswith("%"):
            val = float(val.strip().rstrip("%"))
        else:
            val = float(val)
    except (ValueError, TypeError):
        return ""

    if val >= 97.5:
        return None
    elif val >= 95:
        return "#FFEB93"
    # ... (rest of the color mappings remain the same)
    else:
        return "#F23202"


def generate_colored_ptrtable_html(df: pl.DataFrame):
    """
    Generate HTML table with colored cells for Polars DataFrame
    """
    if df.is_empty():
        return "<div>Il DataFrame è vuoto.</div>"

    # Convert to pandas for HTML generation (Polars doesn't have direct HTML output)
    df_pandas = df.to_pandas()

    # 1. Multi-column sorting (first by temperature, then alphabetic)
    df_sorted = df_pandas.reset_index().sort_values(by=["°C", "Corner", "Split"])

    # Rest of the HTML generation remains the same...
    # [HTML generation code continues as before]

    return "HTML content would be generated here using the pandas version"


def generate_colored_ptrtable_html(
    df: pl.DataFrame,
):
    """
    Versione migliorata che restituisce l'HTML della tabella PTR con:
    - Nasconde la colonna indice
    - Mostra sempre tutti i dati
    - Ordinamento multi-colonna (prima per temperatura, poi alfabetico)
    - Colorazione specifica per Cpk, Yield, Cp
    - Pulsante per esportare in CSV
    """
    if df.is_empty():
        return "<div>Il DataFrame è vuoto.</div>"

    df_pandas = df.to_pandas()

    # 1. Ordinamento multi-colonna (prima per temperatura, poi alfabetico)
    df_sorted = df_pandas.reset_index(drop=True).sort_values(
        by=["°C", "Corner", "Split"]
    )
    # Genera HTML base della tabella (senza indice)
    html_table = df_sorted.to_html(
        classes="display compact cell-border",
        table_id="colored-table",
        # escape=False,
        border=0,
        index=False,  # Nasconde la colonna indice
    )

    # CSS integrato con colorazione specifica per PTR
    css_styles = f"""
    <style>
    .filter-container {{
        margin: 2px 0;
        padding: 2px;
        background-color: #ffffff;
        border-radius: 5px;
        text-align: end;
    }}
    
    .btn {{
        padding: 5px 10px;
        margin: 0px;
        border: none;
        border-radius: 4px;
        cursor: pointer;
        font-size: 12px;
    }}
    
    .btn-success {{
        background-color: #28a745;
        color: white;
    }}
    
    .btn:hover {{
        opacity: 0.8;
    }}
    
    #colored-table {{
        width: 100%;
        table-layout: auto;
        border-collapse: collapse;
        font-family: Arial, sans-serif;
        margin: 0px;
    }}
    
    #colored-table thead th {{
        background-color: {STyellow};
        color: {STblue};
        font-weight: bold;
        text-align: center;
        font-size: 12px;
        padding: 8px;
        border: 1px solid #ddd;
    }}
    
    /* Colorazione per le colonne indice (°C, Corner, Split, index) */
    #colored-table tbody tr td:nth-child(1),
    #colored-table tbody tr td:nth-child(2), 
    #colored-table tbody tr td:nth-child(3),
    #colored-table tbody tr td:nth-child(4) {{
        background-color: {STyellow};
        color: {STblue};
        font-weight: bold;
    }}
    
    /* Colorazione alternata per le altre colonne */
    #colored-table tbody tr:nth-child(even) td:nth-child(n+5) {{
        background-color: {color_light};
    }}
    
    #colored-table tbody tr:nth-child(odd) td:nth-child(n+5) {{
        background-color: {color_dark};
    }}
    
    #colored-table tbody tr td {{
        text-align: center;
        font-size: 11px;
        height: 30px;
        vertical-align: middle;
        padding: 8px;
        border: 1px solid #ddd;
        white-space: nowrap;
    }}
    
    .hidden-row {{
        display: none !important;
    }}
    
    /* Classi per colorazione specifica delle celle */
    .cpk-red {{ background-color: #F23202 !important; color: white; }}
    .cpk-orange-dark {{ background-color: #E85D04 !important; color: white; }}
    .cpk-orange {{ background-color: #F48C06 !important; color: white; }}
    .cpk-yellow-dark {{ background-color: #FAA307 !important; color: black; }}
    .cpk-yellow {{ background-color: #FFBA08 !important; color: black; }}

    .yield-1 {{ background-color: #FFEB93 !important; color: black; }}
    .yield-2 {{ background-color: #FFE678 !important; color: black; }}
    .yield-3 {{ background-color: #FFE15D !important; color: black; }}
    .yield-4 {{ background-color: #FFDF50 !important; color: black; }}
    .yield-5 {{ background-color: #FFDC42 !important; color: black; }}
    .yield-6 {{ background-color: #FFD626 !important; color: black; }}
    .yield-7 {{ background-color: #FFBA08 !important; color: black; }}
    .yield-8 {{ background-color: #FCB007 !important; color: black; }}
    .yield-9 {{ background-color: #FBA607 !important; color: black; }}
    .yield-10 {{ background-color: #FB9C07 !important; color: black; }}
    .yield-11 {{ background-color: #FAA307 !important; color: black; }}
    .yield-12 {{ background-color: #F99806 !important; color: black; }}
    .yield-13 {{ background-color: #F78E06 !important; color: black; }}
    .yield-14 {{ background-color: #F68406 !important; color: black; }}
    .yield-15 {{ background-color: #F48C06 !important; color: white; }}
    .yield-16 {{ background-color: #F17505 !important; color: white; }}
    .yield-17 {{ background-color: #EE6905 !important; color: white; }}
    .yield-18 {{ background-color: #EA6104 !important; color: white; }}
    .yield-19 {{ background-color: #E85D04 !important; color: white; }}
    .yield-critical {{ background-color: #F23202 !important; color: white; }}
    </style>
    """

    # Pulsante per export CSV
    html_content = f"""
    <div class="table">
    <div class="filter-container">
        <button class="btn btn-success" onclick="exportToCSV()">to CSV</button>
    </div>
        {html_table}
    </div>
    """

    # JavaScript per colorazione dinamica e export CSV
    js_script = f"""
    <script src="https://code.jquery.com/jquery-3.7.1.min.js"></script>

    <script>
    // Dati della tabella per l'export
    const tableData = {df_sorted.to_json(orient='records')};
    const columnNames = {df_sorted.columns.tolist()};

    // Funzioni di colorazione
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

    // Applica colorazione alle celle specifiche
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
        // Converti in CSV
        let csvContent = columnNames.join(',') + '\\n';
        tableData.forEach(row => {{
            const rowArray = columnNames.map(col => {{
                const value = row[col];
                // Gestisci valori con virgole o caratteri speciali
                if (typeof value === 'string' && (value.includes(',') || value.includes('"'))) {{
                    return '"' + value.replace(/"/g, '""') + '"';
                }}
                return value !== null && value !== undefined ? value : '';
            }});
            csvContent += rowArray.join(',') + '\\n';
        }});
        
        // Trova l'elemento con la classe 'gtitle'
        const titleElement = document.querySelector('.gtitle');
        const fileName = titleElement ? titleElement.textContent.trim() : 'ptr_table';

        // Scarica il file
        const blob = new Blob([csvContent], {{ type: 'text/csv;charset=utf-8;' }});
        const link = document.createElement('a');
        const url = URL.createObjectURL(blob);
        link.setAttribute('href', url);
        link.setAttribute('download', `${{fileName}}.csv`);
        link.style.visibility = 'hidden';
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
        URL.revokeObjectURL(url); // Pulisci l'URL per evitare memory leak
    }}

    // Applica la colorazione specifica quando la pagina è caricata
    $(document).ready(function() {{
        applySpecificColoring();
    }});
    </script>
    """

    return css_styles + html_content + js_script


def generate_colored_ftrtable_html(
    df: pl.DataFrame,
):
    """
    Versione migliorata che restituisce l'HTML della tabella con:
    - Nasconde la colonna indice
    - Mostra sempre tutti i dati
    - Filtri checkbox per le prime 4 colonne
    - Pulsante per esportare in CSV
    """
    if df.is_empty():
        return "<div>Il DataFrame è vuoto.</div>"
    df_pandas = df.to_pandas()
    priority_cols = ["°C", "Corner", "Metric"]
    remaining_cols = sorted([col for col in df.columns if col not in priority_cols])
    new_order = priority_cols + remaining_cols
    df_pandas = df_pandas[new_order]

    # Genera HTML base della tabella (senza indice)
    html_table = df_pandas.to_html(
        classes="display compact cell-border",
        table_id="colored-table",
        # escape=False,
        border=0,
        index=False,  # Nasconde la colonna indice
    )

    # CSS integrato
    css_styles = f"""
    <style>
    .filter-container {{
        margin: 2px 0;
        padding: 2px;
        background-color: #ffffff;
        border-radius: 5px;
        text-align: end;
    }}
    
    .btn {{
        padding: 5px 10px;
        margin: 0px;
        border: none;
        border-radius: 4px;
        cursor: pointer;
        font-size: 12px;
    }}
    
    .btn-success {{
        background-color: #28a745;
        color: white;
    }}
    
    .btn:hover {{
        opacity: 0.8;
    }}
    
    #colored-table {{
        width: 100%;
        table-layout: auto;
        border-collapse: collapse;
        font-family: Arial, sans-serif;
        margin: 0px;
    }}
    
    #colored-table thead th {{
        background-color: {STyellow};
        color: {STblue};
        font-weight: bold;
        text-align: center;
        font-size: 12px;
        padding: 8px;
        border: 1px solid #ddd;
    }}
    
    #colored-table tbody tr td:nth-child(1),
    #colored-table tbody tr td:nth-child(2), 
    #colored-table tbody tr td:nth-child(3) {{
        background-color: {STyellow};
        color: {STblue};
        font-weight: bold;
    }}
    
    #colored-table tbody tr:nth-child(even) td:nth-child(n+4) {{
        background-color: {color_light};
    }}
    
    #colored-table tbody tr:nth-child(odd) td:nth-child(n+4) {{
        background-color: {color_dark};
    }}
    
    #colored-table tbody tr td {{
        text-align: center;
        font-size: 11px;
        height: 30px;
        vertical-align: middle;
        padding: 8px;
        border: 1px solid #ddd;
    }}
    
    .hidden-row {{
        display: none !important;
    }}
    </style>
    """

    # Pulsante per export CSV
    html_content = f"""
    
    <div class="table">
    <div class="filter-container">
        <button class="btn btn-success" onclick="exportToCSV()">to CSV</button>
    </div>
        {html_table}
    </div>
    """

    # JavaScript per i filtri e export CSV
    js_script = f"""
    <script src="https://code.jquery.com/jquery-3.7.1.min.js"></script>
    
    <script>
    // Dati della tabella per l'export
    const tableData = {df_pandas.to_json(orient='records')};
    const columnNames = {df_pandas.columns.tolist()};
    
    function exportToCSV() {{
        // Converti in CSV
        let csvContent = columnNames.join(',') + '\\n';
        tableData.forEach(row => {{
            const rowArray = columnNames.map(col => {{
                const value = row[col];
                // Gestisci valori con virgole o caratteri speciali
                if (typeof value === 'string' && (value.includes(',') || value.includes('"'))) {{
                    return '"' + value.replace(/"/g, '""') + '"';
                }}
                return value;
            }});
            csvContent += rowArray.join(',') + '\\n';
        }});
        
        // Trova l'elemento con la classe 'gtitle'
        const titleElement = document.querySelector('.gtitle');
        const fileName = titleElement ? titleElement.textContent.trim() : 'row_table';
    
        // Scarica il file
        const blob = new Blob([csvContent], {{ type: 'text/csv;charset=utf-8;' }});
        const link = document.createElement('a');
        const url = URL.createObjectURL(blob);
        link.setAttribute('href', url);
        link.setAttribute('download', `${{fileName}}.csv`);
        link.style.visibility = 'hidden';
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
    }}
    </script>
    """

    return css_styles + html_content + js_script
