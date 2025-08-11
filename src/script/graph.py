import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from plotly.offline import plot

STblue = "#03234B"
STcyan = "#3CB4E6"
STred = "#E6007E"
STyellow = "#FFD200"
STgreen = "#49B170"
STViolet = "#8C0078"
STdarkgreen = "#04572F"


def freedman_diaconis_rule(data):
    import numpy as np

    data = data.dropna()  # Rimuove NaN
    data = data[np.isfinite(data)]  # Rimuove inf e -inf

    q25, q75 = np.percentile(data, [1, 99])  # Calcolo IQR
    iqr = q75 - q25
    if iqr == 0:
        return 10
    n = len(data)
    bin_width = 2 * iqr / np.cbrt(n)
    data_range = data.max() - data.min()
    return int(np.ceil(data_range / bin_width)) * 5


def std_hist(td, ll, ul, units, STPalette, limit_color="#E6007E"):
    nbins_fd = freedman_diaconis_rule(td["Value"])
    corners = sorted(td["Corner"].unique())

    # Calcola i limiti globali dell'asse X per tutti i corner
    global_min = td["Value"].min()
    global_max = td["Value"].max()

    # Crea il grafico iniziale vuoto
    fig = go.Figure()

    # Ottieni tutte le temperature uniche nel dataset
    all_temps = sorted(td["°C"].unique())
    added_to_legend = set()  # Traccia quali temperature sono già nella legenda

    # Per ogni corner, aggiungi gli istogrammi
    for i, corner in enumerate(corners):
        corner_data = td[td["Corner"] == corner]

        # Per ogni temperatura globale, crea un istogramma (anche se vuoto)
        for temp in all_temps:
            temp_data = corner_data[corner_data["°C"] == temp]

            # Controlla se mostrare questa temperatura nella legenda
            show_in_legend = temp not in added_to_legend
            if show_in_legend:
                added_to_legend.add(temp)

            # Aggiungi l'istogramma
            fig.add_trace(
                go.Histogram(
                    x=temp_data["Value"] if len(temp_data) > 0 else [],
                    name=f"{temp}°C",
                    # nbinsx=nbins_fd,
                    marker_color=STPalette.get(
                        temp, STPalette.get(str(temp), "#1f77b4")
                    ),
                    opacity=0.7,
                    visible=(
                        True if i == 0 else False
                    ),  # Solo il primo corner visibile inizialmente
                    legendgroup=f"temp_{temp}",  # Raggruppa per temperatura
                )
            )

    # Calcola quanti trace ci sono per corner
    temps_per_corner = len(
        all_temps
    )  # Usa tutte le temperature, non solo quelle del corner

    # Crea gli step per lo slider
    steps = []
    for i, corner in enumerate(corners):
        # Calcola quali trace rendere visibili per questo corner
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

    # Aggiungi le linee verticali se necessario
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

    # Configura il layout con slider
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
            range=[global_min * 0.95, global_max * 1.05],  # Asse fisso con margine
        ),
        yaxis=dict(title="Count"),
        title=f"Histogram - Corner: {corners[0]}",
        template="plotly_white",
        # height=600,
        showlegend=True,
        barmode="overlay",  # Sovrappone gli istogrammi
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
    Crea una dashboard con layout organizzato e responsive:
    - Metà superiore: istogramma (3/4) + bottoni corner (1/4)
    - Metà inferiore: heatmap quadrata (3/4) + bottoni temperatura verticali (1/4)

    Parameters:
    - td: DataFrame con colonne 'Value', 'Corner', '°C', 'X_COORD', 'Y_COORD'
    - ll, ul: limiti inferiore e superiore
    - units: unità di misura
    - STPalette: dizionario colori per temperature
    - gradientcolor: scala colori per heatmap
    - xwafer, ywafer: range assi per heatmap
    - limit_color: colore delle linee limite
    """

    # Parametri iniziali
    nbins_fd = freedman_diaconis_rule(td["Value"])
    corners = sorted(td["Corner"].unique())
    all_temps = sorted(td["°C"].unique())

    # Default values
    default_corner = "TTTT" if "TTTT" in corners else corners[0]
    default_temp = "30" if "30" in all_temps else all_temps[0]

    default_corner_idx = corners.index(default_corner)
    default_temp_idx = all_temps.index(default_temp)

    # Limiti globali per l'istogramma
    global_min = td["Value"].min()
    global_max = td["Value"].max()

    # Layout: 2x1 grid (rimuoviamo la row intermedia per i bottoni)
    fig = make_subplots(
        rows=2,
        row_heights=[0.2, 0.6],
        horizontal_spacing=0.05,
    )

    # =========================
    # PARTE 1: ISTOGRAMMA (Row 1, Col 1)
    # =========================

    for i, corner in enumerate(corners):
        corner_data = td[td["Corner"] == corner]

        for temp in all_temps:
            temp_data = corner_data[corner_data["°C"] == temp]

            fig.add_trace(
                go.Histogram(
                    x=temp_data["Value"] if len(temp_data) > 0 else [],
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
    # PARTE 2: HEATMAP (Row 2, Col 1)
    # =========================

    # Prepara dati heatmap con padding
    std_dev = np.std(td["Value"])
    step = std_dev / 10 if std_dev >= 1e-5 else std_dev

    # Create ranges for X and Y coordinates
    x_range = np.arange(td["X_COORD"].min() - 1, td["X_COORD"].max() + 2)
    y_range = np.arange(td["Y_COORD"].min() - 1, td["Y_COORD"].max() + 2)

    # Create meshgrid for all combinations
    x_mesh, y_mesh = np.meshgrid(x_range, y_range)

    # Create additional data with all combinations
    additional_data = pd.DataFrame(
        {
            "X_COORD": x_mesh.flatten(),
            "Y_COORD": y_mesh.flatten(),
            "Value": np.nan,
            "Corner": corners[0],
            "°C": all_temps[0],
        }
    )

    td_extended = pd.concat([td, additional_data], ignore_index=True)

    # Per ogni combinazione corner-temperatura, crea una heatmap
    for i, corner in enumerate(corners):
        for j, temp in enumerate(all_temps):
            filtered_data = td_extended[
                (td_extended["Corner"] == corner) & (td_extended["°C"] == temp)
            ]

            if len(filtered_data) == 0:
                filtered_data = additional_data.copy()

            visible = i == default_corner_idx and j == default_temp_idx

            # Calcola min/max per gradiente dinamico
            current_data = td[(td["Corner"] == corner) & (td["°C"] == temp)]
            if len(current_data) > 0:
                zmin_val = current_data["Value"].min()
                zmax_val = current_data["Value"].max()
            else:
                zmin_val = td["Value"].min()
                zmax_val = td["Value"].max()

            fig.add_trace(
                go.Heatmap(
                    z=filtered_data["Value"],
                    x=filtered_data["X_COORD"],
                    y=filtered_data["Y_COORD"],
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

    # =========================
    # PREPARAZIONE BOTTONI
    # =========================

    temps_per_corner = len(all_temps)
    total_hist_traces = len(corners) * temps_per_corner
    total_heatmap_traces = len(corners) * len(all_temps)

    # Bottoni per Corner
    corner_buttons = []
    for i, corner in enumerate(corners):
        # Visibilità istogrammi
        hist_visible = [False] * total_hist_traces
        hist_start = i * temps_per_corner
        hist_end = hist_start + temps_per_corner
        for j in range(hist_start, min(hist_end, len(hist_visible))):
            hist_visible[j] = True

        # Visibilità heatmap - mantieni temperatura corrente
        heatmap_visible = [False] * total_heatmap_traces
        current_temp_idx = default_temp_idx
        heatmap_idx = i * len(all_temps) + current_temp_idx
        if heatmap_idx < len(heatmap_visible):
            heatmap_visible[heatmap_idx] = True

        all_visible = hist_visible + heatmap_visible

        # Aggiorna gradiente
        current_data = td[
            (td["Corner"] == corner) & (td["°C"] == all_temps[current_temp_idx])
        ]
        if len(current_data) > 0:
            zmin_update = current_data["Value"].min()
            zmax_update = current_data["Value"].max()
        else:
            zmin_update = td["Value"].min()
            zmax_update = td["Value"].max()

        button = dict(
            method="update",
            args=[
                {
                    "visible": all_visible,
                    "zmin": [None] * total_hist_traces
                    + [zmin_update if vis else None for vis in heatmap_visible],
                    "zmax": [None] * total_hist_traces
                    + [zmax_update if vis else None for vis in heatmap_visible],
                }
            ],
            label=str(corner),
        )
        corner_buttons.append(button)

    # Bottoni per Temperature
    temp_buttons = []
    for j, temp in enumerate(all_temps):
        # Mantieni istogrammi corner corrente
        hist_visible = [False] * total_hist_traces
        current_corner_idx = default_corner_idx
        hist_start = current_corner_idx * temps_per_corner
        hist_end = hist_start + temps_per_corner
        for k in range(hist_start, min(hist_end, len(hist_visible))):
            hist_visible[k] = True

        # Cambia heatmap per nuova temperatura
        heatmap_visible = [False] * total_heatmap_traces
        heatmap_idx = current_corner_idx * len(all_temps) + j
        if heatmap_idx < len(heatmap_visible):
            heatmap_visible[heatmap_idx] = True

        all_visible = hist_visible + heatmap_visible

        # Aggiorna gradiente
        current_data = td[
            (td["Corner"] == corners[current_corner_idx]) & (td["°C"] == temp)
        ]
        if len(current_data) > 0:
            zmin_update = current_data["Value"].min()
            zmax_update = current_data["Value"].max()
        else:
            zmin_update = td["Value"].min()
            zmax_update = td["Value"].max()

        button = dict(
            method="update",
            args=[
                {
                    "visible": all_visible,
                    "zmin": [None] * total_hist_traces
                    + [
                        zmin_update if vis else None
                        for vis in all_visible[total_hist_traces:]
                    ],
                    "zmax": [None] * total_hist_traces
                    + [
                        zmax_update if vis else None
                        for vis in all_visible[total_hist_traces:]
                    ],
                }
            ],
            label=f"{temp}°C",
        )
        temp_buttons.append(button)

    # =========================
    # LAYOUT E CONFIGURAZIONE
    # =========================

    # Aggiungi linee limite all'istogramma
    if ul != 0 and ll != 0:
        fig.add_vline(
            ul,
            line_color=limit_color,
            line_dash="dash",
            # annotation_text="Upper Limit",
            # annotation_position="top",
            row=1,
            col=1,
        )
        fig.add_vline(
            ll,
            line_color=limit_color,
            line_dash="dash",
            # annotation_text="Lower Limit",
            # annotation_position="top",
            row=1,
            col=1,
        )
    fig.update_layout(
        legend=dict(
            x=1,
            y=1,
            traceorder="reversed",
            font=dict(size=12, color=STblue),
            # bgcolor="LightSteelBlue",
            # bordercolor="Black",
            # borderwidth=2,
        )
    )
    # Configura layout principale con bottoni
    fig.update_layout(
        # Menu a tendina per Corner (orizzontale in alto)
        updatemenus=[
            dict(
                type="buttons",
                direction="right",
                active=default_corner_idx,
                x=0.5,
                y=1,
                xanchor="center",
                yanchor="bottom",
                buttons=corner_buttons,
                bgcolor=STyellow,
                bordercolor=STblue,
                borderwidth=0,
                font=dict(size=15, color=STblue),
                pad=dict(r=5, t=5),
                showactive=True,
            ),
            # Menu a tendina per Temperature (verticale a destra)
            dict(
                type="buttons",
                direction="down",
                active=default_temp_idx,
                x=1,
                y=0.25,
                xanchor="left",
                yanchor="middle",
                buttons=temp_buttons,
                bgcolor=STyellow,
                bordercolor=STblue,
                borderwidth=0,
                font=dict(size=15, color=STblue),
                pad=dict(r=5, t=5),
                showactive=True,
            ),
        ],
        autosize=True,
        template="plotly_white",
        showlegend=True,
        barmode="overlay",
        margin=dict(l=50, r=120, t=120, b=50),  # Margini aggiustati per i bottoni
        # annotations=[
        #     # Etichetta per i bottoni Corner
        #     dict(
        #         text="Corner:",
        #         x=0.35,
        #         y=0.51,
        #         xref="paper",
        #         yref="paper",
        #         xanchor="center",
        #         yanchor="bottom",
        #         showarrow=False,
        #         font=dict(size=12, color="black"),
        #     ),
        #     # Etichetta per i bottoni Temperature
        #     dict(
        #         text="Temperature:",
        #         x=1.05,
        #         y=0.35,
        #         xref="paper",
        #         yref="paper",
        #         xanchor="left",
        #         yanchor="bottom",
        #         showarrow=False,
        #         font=dict(size=12, color="black"),
        #     ),
        # ],
    )

    # Configura assi istogramma
    fig.update_xaxes(
        title=f"Value ({units})",
        row=1,
        col=1,
    )
    fig.update_yaxes(title="Count", row=1, col=1)

    # Configura assi heatmap - FORZARE ASPETTO QUADRATO
    fig.update_xaxes(
        title="X Coordinate",
        showgrid=False,
        zeroline=False,
        range=xwafer,
        scaleanchor="y2",  # Lega l'asse X all'asse Y per mantenere proporzioni
        scaleratio=1,  # Ratio 1:1 per aspetto quadrato
        constrain="domain",  # Vincola al dominio disponibile
        row=2,
        col=1,
    )
    fig.update_yaxes(
        title="Y Coordinate",
        showgrid=False,
        zeroline=False,
        range=ywafer[::-1],
        constrain="domain",  # Vincola al dominio disponibile
        row=2,
        col=1,
    )
    fig.update_layout(
        title_text=str(td["TestName"].iloc[0]),
        title_x=0.5,  # Centra il titolo
        title_font=dict(size=48),  # Imposta la dimensione del font
        title_pad=dict(t=10, r=0, b=15, l=0),
        height=800,  # o l'altezza desiderata
        autosize=True,
    )

    return fig


def color_cpk(val):
    """Color function for Cpk values"""
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
        return "#FFFFFF"


def color_yield(val):
    """Color function for Yield values"""
    try:
        # Handle both percentage strings and numeric values
        if isinstance(val, str) and val.strip().endswith("%"):
            val = float(val.strip().rstrip("%"))
        else:
            val = float(val)
    except (ValueError, TypeError):
        return ""

    if val < 50:
        return "#F23202"
    elif 50 <= val < 60:
        return "#E85D04"
    elif 60 <= val < 70:
        return "#F48C06"
    elif 70 <= val < 80:
        return "#FAA307"
    elif 80 <= val <= 99:
        return "#FFBA08"
    else:
        return "#FFFFFF"


def color_kurtosis(val):
    """Color function for Kurtosis values"""
    try:
        val = float(val)
    except (ValueError, TypeError):
        return ""

    if val > -0.2:
        return "#F23202"
    elif -0.2 >= val > -0.4:
        return "#E85D04"
    elif -0.4 >= val > -0.6:
        return "#F48C06"
    elif -0.6 >= val > -0.8:
        return "#FAA307"
    elif -0.8 >= val >= -1.0:
        return "#FFBA08"
    else:
        return "#FFFFFF"


def color_cp(val):
    """Color function for Cp values"""
    try:
        val = float(val)
    except (ValueError, TypeError):
        return ""

    if val < 6:
        return "#F23202"
    elif 6 <= val < 7:
        return "#E85D04"
    elif 7 <= val < 8:
        return "#F48C06"
    elif 8 <= val < 9:
        return "#FAA307"
    elif 9 <= val <= 10:
        return "#FFBA08"
    else:
        return "#FFFFFF"


def generate_colored_table(df):
    """Generate HTML table with colored cells based on value ranges and grouped by temperature"""

    # Create a copy of the dataframe with index as a column
    df_with_index = df.reset_index()

    # Sort by °C column to ensure proper grouping
    df_with_index = df_with_index.sort_values("°C")

    # Create grouped version where °C values are shown only once per group
    df_grouped = df_with_index.copy()

    # Replace duplicate °C values with empty strings (keep only first occurrence)
    prev_temp = None
    for i, temp in enumerate(df_grouped["°C"]):
        if temp == prev_temp:
            df_grouped.loc[df_grouped.index[i], "°C"] = ""
        else:
            prev_temp = temp

    # Get all columns
    columns = df_grouped.columns.tolist()

    # Create cell colors matrix
    cell_colors = []

    for _, row in df_grouped.iterrows():
        row_colors = []
        for col in columns:
            val = row[col]

            if col == "Cpk":
                color = color_cpk(val)
            elif col == "Yield":
                color = color_yield(val)
            # elif col == "Kurtosis":
            #     color = color_kurtosis(val)
            elif col == "Cp":
                color = color_cp(val)
            elif col in ["°C", "Corner", "index"]:
                color = STyellow  # Dark color for index columns
            else:
                color = "#FFFFFF"  # Default white background

            row_colors.append(color)
        cell_colors.append(row_colors)

    # Convert data to list format for Plotly
    cell_values = []
    for col in columns:
        cell_values.append(df_grouped[col].tolist())

    # Create the table
    fig = go.Figure(
        data=[
            go.Table(
                header=dict(
                    values=columns,
                    fill_color=STyellow,
                    font=dict(color=STblue, size=12),
                    align="center",
                    # height=40,
                ),
                cells=dict(
                    values=cell_values,
                    fill_color=np.array(cell_colors).T.tolist(),
                    align="center",
                    font=dict(
                        color=[
                            STblue if col in ["°C", "Corner", "index"] else "black"
                            for col in columns
                        ],
                        size=11,
                    ),
                    # height=30,
                ),
            )
        ]
    )

    # Update layout
    fig.update_layout(
        title_x=0.5,
        height=900,
        margin=dict(l=20, r=20, t=50, b=20),
    )

    # Generate HTML string
    html_string = plot(fig, output_type="div", include_plotlyjs=True)

    return html_string


def combined_hist_heatmap_box(
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
    Crea una dashboard con layout 2x2:
    - Row 1 Col 1: Box plot
    - Row 1 Col 2: Istogramma
    - Row 2 Col 1: Box plot (continuazione se necessario)
    - Row 2 Col 2: Heatmap

    Parameters:
    - td: DataFrame con colonne 'Value', 'Corner', '°C', 'X_COORD', 'Y_COORD', 'Split'
    - ll, ul: limiti inferiore e superiore
    - units: unità di misura
    - STPalette: dizionario colori per temperature
    - tempSTcolort: lista colori per box plot
    - gradientcolor: scala colori per heatmap
    - xwafer, ywafer: range assi per heatmap
    - limit_color: colore delle linee limite
    """

    # Parametri iniziali
    nbins_fd = freedman_diaconis_rule(td["Value"])
    corners = sorted(td["Corner"].unique())
    all_temps = sorted(td["°C"].unique())
    all_split = sorted(td["Split"].unique()) if "Split" in td.columns else ["Default"]

    # Default values
    default_corner = "TTTT" if "TTTT" in corners else corners[0]
    default_temp = "30" if "30" in all_temps else all_temps[0]

    default_corner_idx = corners.index(default_corner)
    default_temp_idx = all_temps.index(default_temp)

    # Layout: 2x2 grid
    fig = make_subplots(
        rows=2,
        cols=2,
        row_heights=[0.2, 0.6],
        column_widths=[0.5, 0.5],
        # subplot_titles=("Box Plot", "Histogram", "", "Heatmap"),
        vertical_spacing=0.1,
        horizontal_spacing=0.08,
        specs=[[{"rowspan": 2}, {"type": "xy"}], [None, {"type": "xy"}]],
    )

    # =========================
    # PARTE 1: BOX PLOT (Row 1-2, Col 1)
    # =========================
    tempSTcolort = list(STPalette.values())
    # Crea box plot per ogni combinazione di temperatura
    for i, split in enumerate(all_split):
        split_data = td[td["Split"] == split] if "Split" in td.columns else td

        for j, temp in enumerate(all_temps):
            temp_split_data = split_data[split_data["°C"] == temp]

            if len(temp_split_data) > 0:
                fig.add_trace(
                    go.Box(
                        x=temp_split_data["Value"],
                        y=temp_split_data["Corner"],
                        name=f"{temp}°C",
                        marker_color=(
                            tempSTcolort[j % len(tempSTcolort)]
                            if j < len(tempSTcolort)
                            else tempSTcolort[0]
                        ),
                        orientation="h",
                        boxpoints="outliers",
                        visible=True,
                        legendgroup=f"temp_{temp}",  # Usa stesso gruppo dell'istogramma
                        showlegend=True,  # Mostra solo per il primo trace di ogni temperatura
                        offsetgroup=f"split_{split}_temp_{temp}",
                        yaxis=f"y{i+1}" if i > 0 else "y",
                    ),
                    row=1,
                    col=1,
                )

    # =========================
    # PARTE 2: ISTOGRAMMA (Row 1, Col 2)
    # =========================

    for i, corner in enumerate(corners):
        corner_data = td[td["Corner"] == corner]

        for j, temp in enumerate(all_temps):
            temp_data = corner_data[corner_data["°C"] == temp]

            fig.add_trace(
                go.Histogram(
                    x=temp_data["Value"] if len(temp_data) > 0 else [],
                    name=f"{temp}°C",
                    marker_color=STPalette.get(
                        temp, STPalette.get(str(temp), "#1f77b4")
                    ),
                    opacity=0.6,
                    visible=True if i == default_corner_idx else False,
                    legendgroup=f"temp_{temp}",  # Stesso gruppo del box plot
                    showlegend=False,  # Non mostrare nella legenda (già mostrato dal box plot)
                ),
                row=1,
                col=2,
            )

    # =========================
    # PARTE 3: HEATMAP (Row 2, Col 2)
    # =========================

    # Prepara dati heatmap con padding
    std_dev = np.std(td["Value"])
    step = std_dev / 10 if std_dev >= 1e-5 else std_dev

    # Create ranges for X and Y coordinates
    x_range = np.arange(td["X_COORD"].min() - 1, td["X_COORD"].max() + 2)
    y_range = np.arange(td["Y_COORD"].min() - 1, td["Y_COORD"].max() + 2)

    # Create meshgrid for all combinations
    x_mesh, y_mesh = np.meshgrid(x_range, y_range)

    # Create additional data with all combinations
    additional_data = pd.DataFrame(
        {
            "X_COORD": x_mesh.flatten(),
            "Y_COORD": y_mesh.flatten(),
            "Value": np.nan,
            "Corner": corners[0],
            "°C": all_temps[0],
        }
    )

    td_extended = pd.concat([td, additional_data], ignore_index=True)

    # Per ogni combinazione corner-temperatura, crea una heatmap
    for i, corner in enumerate(corners):
        for j, temp in enumerate(all_temps):
            filtered_data = td_extended[
                (td_extended["Corner"] == corner) & (td_extended["°C"] == temp)
            ]

            if len(filtered_data) == 0:
                filtered_data = additional_data.copy()

            visible = i == default_corner_idx and j == default_temp_idx

            # Calcola min/max per gradiente dinamico
            current_data = td[(td["Corner"] == corner) & (td["°C"] == temp)]
            if len(current_data) > 0:
                zmin_val = current_data["Value"].min()
                zmax_val = current_data["Value"].max()
            else:
                zmin_val = td["Value"].min()
                zmax_val = td["Value"].max()

            fig.add_trace(
                go.Heatmap(
                    z=filtered_data["Value"],
                    x=filtered_data["X_COORD"],
                    y=filtered_data["Y_COORD"],
                    colorscale=gradientcolor,
                    colorbar=dict(
                        title=f"Value ({units})",
                        x=1.02,
                        y=0.25,
                        len=0.4,
                        yanchor="middle",
                    ),
                    hoverongaps=False,
                    hovertemplate="x: %{x}<br>y: %{y}<br>Value: %{z:.2f}<br>",
                    name="",
                    zmin=zmin_val,
                    zmax=zmax_val,
                    visible=visible,
                    showlegend=False,  # Heatmap non deve apparire nella legenda
                ),
                row=2,
                col=2,
            )

    # =========================
    # PREPARAZIONE BOTTONI
    # =========================

    temps_per_corner = len(all_temps)
    total_box_traces = len(all_split) * len(all_temps)
    total_hist_traces = len(corners) * temps_per_corner
    total_heatmap_traces = len(corners) * len(all_temps)

    # Bottoni per Corner
    corner_buttons = []
    for i, corner in enumerate(corners):
        # Box plot sempre visibile
        box_visible = [True] * total_box_traces

        # Visibilità istogrammi
        hist_visible = [False] * total_hist_traces
        hist_start = i * temps_per_corner
        hist_end = hist_start + temps_per_corner
        for j in range(hist_start, min(hist_end, len(hist_visible))):
            hist_visible[j] = True

        # Visibilità heatmap - mantieni temperatura corrente
        heatmap_visible = [False] * total_heatmap_traces
        current_temp_idx = default_temp_idx
        heatmap_idx = i * len(all_temps) + current_temp_idx
        if heatmap_idx < len(heatmap_visible):
            heatmap_visible[heatmap_idx] = True

        all_visible = box_visible + hist_visible + heatmap_visible

        # Aggiorna gradiente
        current_data = td[
            (td["Corner"] == corner) & (td["°C"] == all_temps[current_temp_idx])
        ]
        if len(current_data) > 0:
            zmin_update = current_data["Value"].min()
            zmax_update = current_data["Value"].max()
        else:
            zmin_update = td["Value"].min()
            zmax_update = td["Value"].max()

        button = dict(
            method="update",
            args=[
                {
                    "visible": all_visible,
                    "zmin": [None] * (total_box_traces + total_hist_traces)
                    + [zmin_update if vis else None for vis in heatmap_visible],
                    "zmax": [None] * (total_box_traces + total_hist_traces)
                    + [zmax_update if vis else None for vis in heatmap_visible],
                }
            ],
            label=str(corner),
        )
        corner_buttons.append(button)

    # Bottoni per Temperature
    temp_buttons = []
    for j, temp in enumerate(all_temps):
        # Box plot sempre visibile
        box_visible = [True] * total_box_traces

        # Mantieni istogrammi corner corrente
        hist_visible = [False] * total_hist_traces
        current_corner_idx = default_corner_idx
        hist_start = current_corner_idx * temps_per_corner
        hist_end = hist_start + temps_per_corner
        for k in range(hist_start, min(hist_end, len(hist_visible))):
            hist_visible[k] = True

        # Cambia heatmap per nuova temperatura
        heatmap_visible = [False] * total_heatmap_traces
        heatmap_idx = current_corner_idx * len(all_temps) + j
        if heatmap_idx < len(heatmap_visible):
            heatmap_visible[heatmap_idx] = True

        all_visible = box_visible + hist_visible + heatmap_visible

        # Aggiorna gradiente
        current_data = td[
            (td["Corner"] == corners[current_corner_idx]) & (td["°C"] == temp)
        ]
        if len(current_data) > 0:
            zmin_update = current_data["Value"].min()
            zmax_update = current_data["Value"].max()
        else:
            zmin_update = td["Value"].min()
            zmax_update = td["Value"].max()

        button = dict(
            method="update",
            args=[
                {
                    "visible": all_visible,
                    "zmin": [None] * (total_box_traces + total_hist_traces)
                    + [zmin_update if vis else None for vis in heatmap_visible],
                    "zmax": [None] * (total_box_traces + total_hist_traces)
                    + [zmax_update if vis else None for vis in heatmap_visible],
                }
            ],
            label=f"{temp}°C",
        )
        temp_buttons.append(button)

    # =========================
    # AGGIUNGI LINEE LIMITE
    # =========================

    # Linee limite per box plot
    if ul != 0 and ll != 0:
        fig.add_vline(ul, line_color=limit_color, line_dash="dash", row=1, col=1)
        fig.add_vline(ll, line_color=limit_color, line_dash="dash", row=1, col=1)

        # Linee limite per istogramma
        fig.add_vline(ul, line_color=limit_color, line_dash="dash", row=1, col=2)
        fig.add_vline(ll, line_color=limit_color, line_dash="dash", row=1, col=2)

    # =========================
    # LAYOUT E CONFIGURAZIONE
    # =========================

    fig.update_layout(
        # Menu a tendina per Corner
        updatemenus=[
            dict(
                type="buttons",
                direction="right",
                active=default_corner_idx,
                x=0.75,
                y=1.02,
                xanchor="center",
                yanchor="bottom",
                buttons=corner_buttons,
                bgcolor=STyellow,
                bordercolor=STblue,
                borderwidth=0,
                font=dict(size=15, color=STblue),
                pad=dict(r=5, t=5),
                showactive=True,
            ),
            # Menu a tendina per Temperature
            dict(
                type="buttons",
                direction="down",
                active=default_temp_idx,
                x=1.1,
                y=0.25,
                xanchor="left",
                yanchor="middle",
                buttons=temp_buttons,
                bgcolor=STyellow,
                bordercolor=STblue,
                borderwidth=0,
                font=dict(size=15, color=STblue),
                pad=dict(r=5, t=5),
                showactive=True,
            ),
        ],
        autosize=True,
        template="plotly_white",
        showlegend=True,
        barmode="overlay",
        margin=dict(l=50, r=120, t=120, b=50),
        height=1000,
        title_text=str(td["TestName"].iloc[0]),
        title_x=0.5,
        title_font=dict(size=24),
        title_pad=dict(t=10, r=0, b=15, l=0),
        boxmode="group",
    )

    # Configura assi box plot
    fig.update_xaxes(title=f"Value ({units})", row=1, col=1)
    fig.update_yaxes(title="Corner", row=1, col=1)

    # Configura assi istogramma
    fig.update_xaxes(title=f"Value ({units})", row=1, col=2)
    fig.update_yaxes(title="Count", row=1, col=2)

    # Configura assi heatmap
    fig.update_xaxes(
        title="X Coordinate",
        showgrid=False,
        zeroline=False,
        range=xwafer,
        scaleanchor="y4",
        scaleratio=1,
        constrain="domain",
        row=2,
        col=2,
    )
    fig.update_yaxes(
        title="Y Coordinate",
        showgrid=False,
        zeroline=False,
        range=ywafer[::-1],
        constrain="domain",
        row=2,
        col=2,
    )

    return fig
