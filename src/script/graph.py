import numpy as np
import pandas as pd
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
            filtered_data = td[(td["Corner"] == corner) & (td["°C"] == temp)]
            if len(filtered_data) > 0:
                zmin_val = filtered_data["Value"].min()
                zmax_val = filtered_data["Value"].max()
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
    elif val >= 92.5:
        return "#FFE678"
    elif val >= 90:
        return "#FFE15D"
    elif val >= 87.5:
        return "#FFDF50"
    elif val >= 85:
        return "#FFDC42"
    elif val >= 82.5:
        return "#FFD626"
    elif val >= 80:
        return "#FFBA08"
    elif val >= 77.5:
        return "#FCB007"
    elif val >= 75:
        return "#FBA607"
    elif val >= 72.5:
        return "#FB9C07"
    elif val >= 70:
        return "#FAA307"
    elif val >= 67.5:
        return "#F99806"
    elif val >= 65:
        return "#F78E06"
    elif val >= 62.5:
        return "#F68406"
    elif val >= 60:
        return "#F48C06"
    elif val >= 57.5:
        return "#F17505"
    elif val >= 55:
        return "#EE6905"
    elif val >= 52.5:
        return "#EA6104"
    elif val >= 50:
        return "#E85D04"
    else:
        return "#F23202"


def color_kurtosis(val):
    """Color function for Kurtosis values"""
    if val == "-":
        return
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
        return None


def color_cp(val):
    """Color function for Cp values"""
    if val == "-":
        return
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
        return None


def js():
    #     return """
    # // CONFIGURAZIONE GLOBALE - Inizializzata dal Python
    # window.dashboard_state = {
    #     currentSplit: null,
    #     currentCorner: null,
    #     currentTemp: null,

    #     allSplits: [],
    #     allCorners: [],
    #     allTemps: [],

    #     boxTraceCount: 0,
    #     totalTraces: 0,

    #     // Indici precalcolati per ogni sezione
    #     histStartIdx: 0,
    #     heatmapStartIdx: 0,

    #     // Inizializza la configurazione (chiamata dal Python)
    #     init: function(config) {
    #         this.currentSplit = config.currentSplit;
    #         this.currentCorner = config.currentCorner;
    #         this.currentTemp = config.currentTemp;

    #         this.allSplits = config.allSplits;
    #         this.allCorners = config.allCorners;
    #         this.allTemps = config.allTemps;

    #         this.boxTraceCount = config.boxTraceCount;
    #         this.totalTraces = config.totalTraces;

    #         this.histStartIdx = config.histStartIdx;
    #         this.heatmapStartIdx = config.heatmapStartIdx;

    #         console.log('Dashboard state initialized:', this);
    #     }
    # };

    # // Funzione per calcolare la visibilità delle tracce
    # window.updateVisibility = function() {
    #     var state = window.dashboard_state;
    #     var visibility = [];

    #     console.log('Updating visibility with state:', state);

    #     // 1. Box plots: sempre visibili (primi boxTraceCount)
    #     for (var i = 0; i < state.boxTraceCount; i++) {
    #         visibility.push(true);
    #     }

    #     // 2. Histogram traces: visibili solo per currentSplit + currentCorner
    #     // Ordine: split -> corner -> temp
    #     for (var s = 0; s < state.allSplits.length; s++) {
    #         for (var c = 0; c < state.allCorners.length; c++) {
    #             for (var t = 0; t < state.allTemps.length; t++) {
    #                 var split = state.allSplits[s];
    #                 var corner = state.allCorners[c];
    #                 var showHist = (split === state.currentSplit && corner === state.currentCorner);
    #                 visibility.push(showHist);
    #             }
    #         }
    #     }

    #     // 3. Heatmap traces: visibili solo per currentSplit + currentCorner + currentTemp
    #     // Ordine: split -> corner -> temp
    #     for (var s = 0; s < state.allSplits.length; s++) {
    #         for (var c = 0; c < state.allCorners.length; c++) {
    #             for (var t = 0; t < state.allTemps.length; t++) {
    #                 var split = state.allSplits[s];
    #                 var corner = state.allCorners[c];
    #                 var temp = state.allTemps[t];
    #                 var showHeat = (split === state.currentSplit &&
    #                               corner === state.currentCorner &&
    #                               temp === state.currentTemp);
    #                 visibility.push(showHeat);
    #             }
    #         }
    #     }

    #     console.log('Calculated visibility array length:', visibility.length);
    #     console.log('Expected total traces:', state.totalTraces);

    #     return visibility;
    # };

    # // Funzione sicura per applicare l'update
    # window.safeUpdate = function(plotDiv, visibility) {
    #     try {
    #         if (!plotDiv || !plotDiv.data || !Array.isArray(plotDiv.data)) {
    #             console.error('PlotDiv o data non validi');
    #             return false;
    #         }

    #         if (!plotDiv._fullData || !Array.isArray(plotDiv._fullData)) {
    #             console.log('PlotDiv non completamente renderizzato, riprovo...');
    #             return false;
    #         }

    #         if (!Array.isArray(visibility) || visibility.length !== plotDiv.data.length) {
    #             console.error('Visibility array length mismatch:', visibility.length, 'vs', plotDiv.data.length);
    #             return false;
    #         }

    #         // Applica l'update
    #         Plotly.update(plotDiv, {visible: visibility});
    #         console.log('Update applicato con successo');
    #         return true;

    #     } catch (error) {
    #         console.error('Errore durante update:', error);
    #         return false;
    #     }
    # };

    # // Funzione per trovare il plotDiv
    # window.findPlotDiv = function() {
    #     var selectors = [
    #         '.plotly-graph-div',
    #         '[id*="plotly"]',
    #         'div[class*="plotly"]'
    #     ];

    #     for (var i = 0; i < selectors.length; i++) {
    #         var div = document.querySelector(selectors[i]);
    #         if (div && div.data && Array.isArray(div.data)) {
    #             return div;
    #         }
    #     }
    #     return null;
    # };

    # // Gestore eventi Plotly per i pulsanti
    # window.setupPlotlyEventHandlers = function(plotDiv) {
    #     if (typeof plotDiv.on !== 'function') {
    #         console.log('plotDiv.on not available');
    #         return false;
    #     }

    #     plotDiv.on('plotly_buttonclicked', function(eventData) {
    #         console.log('Button clicked via Plotly event:', eventData);

    #         var state = window.dashboard_state;
    #         var menuIndex = eventData.menu._index;
    #         var buttonIndex = eventData.active;

    #         console.log('Menu index:', menuIndex, 'Button index:', buttonIndex);

    #         // Menu 0: Split buttons
    #         if (menuIndex === 0 && buttonIndex < state.allSplits.length) {
    #             var newSplit = state.allSplits[buttonIndex];
    #             console.log('Split changed to:', newSplit);
    #             state.currentSplit = newSplit;
    #         }
    #         // Menu 1: Corner buttons
    #         else if (menuIndex === 1 && buttonIndex < state.allCorners.length) {
    #             var newCorner = state.allCorners[buttonIndex];
    #             console.log('Corner changed to:', newCorner);
    #             state.currentCorner = newCorner;
    #         }
    #         // Menu 2: Temperature buttons
    #         else if (menuIndex === 2 && buttonIndex < state.allTemps.length) {
    #             var newTemp = state.allTemps[buttonIndex];
    #             console.log('Temperature changed to:', newTemp);
    #             state.currentTemp = newTemp;
    #         }

    #         // Aggiorna la visibilità
    #         setTimeout(function() {
    #             var visibility = window.updateVisibility();
    #             window.safeUpdate(plotDiv, visibility);
    #         }, 50);
    #     });

    #     console.log('Plotly event handlers configurati');
    #     return true;
    # };

    # // Gestore eventi DOM come fallback
    # window.setupDOMEventHandlers = function() {
    #     setTimeout(function() {
    #         var state = window.dashboard_state;
    #         var buttons = document.querySelectorAll('.updatemenu-button');

    #         console.log('Setting up DOM handlers for', buttons.length, 'buttons');

    #         buttons.forEach(function(button, index) {
    #             // Rimuovi listener esistenti
    #             var newButton = button.cloneNode(true);
    #             button.parentNode.replaceChild(newButton, button);

    #             newButton.addEventListener('click', function() {
    #                 var buttonText = this.textContent.trim();
    #                 console.log('DOM Button clicked:', buttonText);

    #                 // Determina il tipo di pulsante dal testo
    #                 if (state.allSplits.indexOf(buttonText) !== -1) {
    #                     state.currentSplit = buttonText;
    #                     console.log('Split set to:', buttonText);
    #                 }
    #                 else if (state.allCorners.indexOf(buttonText) !== -1) {
    #                     state.currentCorner = buttonText;
    #                     console.log('Corner set to:', buttonText);
    #                 }
    #                 else if (buttonText.includes('°C')) {
    #                     var temp = buttonText.replace('°C', '');
    #                     if (state.allTemps.indexOf(temp) !== -1) {
    #                         state.currentTemp = temp;
    #                         console.log('Temperature set to:', temp);
    #                     }
    #                 }

    #                 // Aggiorna la visibilità
    #                 setTimeout(function() {
    #                     var plotDiv = window.findPlotDiv();
    #                     if (plotDiv) {
    #                         var visibility = window.updateVisibility();
    #                         window.safeUpdate(plotDiv, visibility);
    #                     }
    #                 }, 100);
    #             });
    #         });

    #     }, 1500);
    # };

    # // Funzione di debug completa
    # window.debugDashboard = function() {
    #     console.log('=== DEBUG DASHBOARD ===');
    #     var state = window.dashboard_state;

    #     console.log('Current state:');
    #     console.log('- currentSplit:', state.currentSplit);
    #     console.log('- currentCorner:', state.currentCorner);
    #     console.log('- currentTemp:', state.currentTemp);
    #     console.log('- allSplits:', state.allSplits);
    #     console.log('- allCorners:', state.allCorners);
    #     console.log('- allTemps:', state.allTemps);

    #     var plotDiv = window.findPlotDiv();
    #     if (plotDiv && plotDiv.data) {
    #         console.log('Plot info:');
    #         console.log('- Total traces:', plotDiv.data.length);
    #         console.log('- Expected total:', state.totalTraces);

    #         console.log('Trace breakdown:');
    #         console.log('- Box traces:', state.boxTraceCount);
    #         console.log('- Hist start idx:', state.histStartIdx);
    #         console.log('- Heatmap start idx:', state.heatmapStartIdx);

    #         var visibility = window.updateVisibility();
    #         console.log('Current visibility:', visibility);

    #         var visibleCount = visibility.filter(function(v) { return v; }).length;
    #         console.log('Visible traces count:', visibleCount);
    #     }

    #     var buttons = document.querySelectorAll('.updatemenu-button');
    #     console.log('Buttons found:', buttons.length);
    #     buttons.forEach(function(btn, i) {
    #         console.log('Button', i + ':', btn.textContent.trim());
    #     });
    # };

    # // Inizializzazione principale
    # window.initializeDashboard = function(config) {
    #     console.log('Initializing dashboard with config:', config);

    #     // Inizializza lo stato
    #     window.dashboard_state.init(config);

    #     // Trova il plot
    #     var plotDiv = window.findPlotDiv();
    #     if (!plotDiv) {
    #         console.error('PlotDiv non trovato');
    #         return false;
    #     }

    #     console.log('PlotDiv trovato, setting up event handlers...');

    #     // Setup event handlers
    #     var plotlySuccess = window.setupPlotlyEventHandlers(plotDiv);
    #     window.setupDOMEventHandlers(); // Sempre come fallback

    #     // Imposta visibilità iniziale
    #     setTimeout(function() {
    #         var visibility = window.updateVisibility();
    #         var success = window.safeUpdate(plotDiv, visibility);

    #         if (success) {
    #             console.log('Dashboard inizializzato con successo');
    #         } else {
    #             console.error('Errore nell\'impostazione della visibilità iniziale');
    #         }
    #     }, 200);

    #     return true;
    # };

    # // Auto-inizializzazione quando il DOM è pronto
    # document.addEventListener('DOMContentLoaded', function() {
    #     console.log('Dashboard JavaScript loaded, waiting for initialization...');
    # });

    # // Funzioni di utilità per controllo manuale
    # window.dashboardFunctions = {
    #     setSplit: function(split) {
    #         var state = window.dashboard_state;
    #         if (state.allSplits.indexOf(split) !== -1) {
    #             state.currentSplit = split;
    #             var plotDiv = window.findPlotDiv();
    #             if (plotDiv) {
    #                 var visibility = window.updateVisibility();
    #                 window.safeUpdate(plotDiv, visibility);
    #             }
    #             console.log('Split set to:', split);
    #         } else {
    #             console.error('Invalid split:', split);
    #         }
    #     },

    #     setCorner: function(corner) {
    #         var state = window.dashboard_state;
    #         if (state.allCorners.indexOf(corner) !== -1) {
    #             state.currentCorner = corner;
    #             var plotDiv = window.findPlotDiv();
    #             if (plotDiv) {
    #                 var visibility = window.updateVisibility();
    #                 window.safeUpdate(plotDiv, visibility);
    #             }
    #             console.log('Corner set to:', corner);
    #         } else {
    #             console.error('Invalid corner:', corner);
    #         }
    #     },

    #     setTemp: function(temp) {
    #         var state = window.dashboard_state;
    #         if (state.allTemps.indexOf(temp) !== -1) {
    #             state.currentTemp = temp;
    #             var plotDiv = window.findPlotDiv();
    #             if (plotDiv) {
    #                 var visibility = window.updateVisibility();
    #                 window.safeUpdate(plotDiv, visibility);
    #             }
    #             console.log('Temperature set to:', temp);
    #         } else {
    #             console.error('Invalid temperature:', temp);
    #         }
    #     },

    #     getCurrentState: function() {
    #         return {
    #             split: window.dashboard_state.currentSplit,
    #             corner: window.dashboard_state.currentCorner,
    #             temp: window.dashboard_state.currentTemp
    #         };
    #     },

    #     debug: window.debugDashboard
    # };
    # """
    return ""


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
    Crea una dashboard con layout dinamico e selezione a cascata semplificata:
    - Colonna 1: Box plot con facet per Split (n righe dinamiche)
    - Colonna 2: Sempre divisa a metà - Istogramma sopra, Heatmap sotto
    - Selezione: Split → cambia tutto, Corner → cambia istogramma+heatmap, Temp → cambia solo heatmap
    """

    # Parametri iniziali
    nbins_fd = freedman_diaconis_rule(td["Value"])
    corners = sorted(td["Corner"].unique())
    all_temps = sorted(td["°C"].unique())
    all_split = sorted(td["Split"].unique()) if "Split" in td.columns else ["Default"]

    # Default values
    default_corner = "TTTT" if "TTTT" in corners else corners[0]
    default_temp = "30" if "30" in all_temps else all_temps[0]
    default_split = "3v3" if "3v3" in all_split else all_split[0]

    # Calculate number of splits and rows needed
    n_splits = len(all_split)
    total_rows = max(n_splits, 2)

    # Create subplot specifications
    subplot_specs = []
    row_heights = []

    if n_splits >= 2:
        box_height_per_row = 1.0 / n_splits
        for i in range(n_splits):
            row_heights.append(box_height_per_row)

        for i in range(n_splits):
            if i < n_splits // 2:
                subplot_specs.append(
                    [{"type": "xy"}, {"type": "xy", "rowspan": n_splits // 2}]
                )
            elif i == n_splits // 2:
                subplot_specs.append(
                    [
                        {"type": "xy"},
                        {"type": "xy", "rowspan": n_splits - n_splits // 2},
                    ]
                )
            else:
                subplot_specs.append([{"type": "xy"}, None])
    else:
        row_heights = [0.5, 0.5]
        subplot_specs = [
            [{"type": "xy", "rowspan": 2}, {"type": "xy"}],
            [None, {"type": "xy"}],
        ]
        total_rows = 2

    # Create the subplot figure
    fig = make_subplots(
        rows=total_rows,
        cols=2,
        row_heights=row_heights,
        column_widths=[0.5, 0.5],
        vertical_spacing=0.05,
        horizontal_spacing=0.08,
        specs=subplot_specs,
    )

    # =========================
    # PARTE 1: BOX PLOT con facet per Split
    # =========================
    for split_idx, split in enumerate(all_split):
        split_data = td[td["Split"] == split] if "Split" in td.columns else td

        for j, temp in enumerate(all_temps):
            temp_split_data = split_data[split_data["°C"] == temp]

            if len(temp_split_data) > 0:
                fig.add_trace(
                    go.Box(
                        x=temp_split_data["Value"],
                        y=temp_split_data["Corner"],
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
                    ),
                    row=split_idx + 1,
                    col=1,
                )

    # =========================
    # PARTE 2: ISTOGRAMMA
    # =========================
    hist_row = 1

    # Crea SOLO le tracce per la combinazione di default inizialmente
    current_split_data = (
        td[td["Split"] == default_split] if "Split" in td.columns else td
    )
    current_corner_data = current_split_data[
        current_split_data["Corner"] == default_corner
    ]

    for temp_idx, temp in enumerate(all_temps):
        temp_data = current_corner_data[current_corner_data["°C"] == temp]

        fig.add_trace(
            go.Histogram(
                x=temp_data["Value"] if len(temp_data) > 0 else [],
                name=f"{temp}°C",
                marker_color=STPalette.get(temp, STPalette.get(str(temp), "#1f77b4")),
                opacity=0.6,
                visible=True,
                legendgroup=f"temp_{temp}",
                showlegend=False,
                nbinsx=nbins_fd,
            ),
            row=hist_row,
            col=2,
        )

    # =========================
    # PARTE 3: HEATMAP
    # =========================
    heatmap_row = 2 if n_splits == 1 else (n_splits // 2 + 1)

    # Prepara dati heatmap con padding
    std_dev = np.std(td["Value"])
    x_min, x_max = xwafer
    y_min, y_max = ywafer
    x_range = np.arange(x_min, x_max + 1)
    y_range = np.arange(y_min, y_max + 1)

    # Crea SOLO la traccia per la combinazione di default inizialmente
    current_temp_data = current_corner_data[current_corner_data["°C"] == default_temp]

    # Create base dataframe with all coordinate combinations
    x_mesh, y_mesh = np.meshgrid(x_range, y_range)
    base_df = pd.DataFrame(
        {
            "X_COORD": x_mesh.flatten(),
            "Y_COORD": y_mesh.flatten(),
            "Value": np.nan,
        }
    )

    # Merge with actual data
    if len(current_temp_data) > 0:
        merged_data = pd.concat([current_temp_data, base_df]).drop_duplicates(
            subset=["X_COORD", "Y_COORD"], keep="first"
        )
        zmin_val = current_temp_data["Value"].min()
        zmax_val = current_temp_data["Value"].max()
    else:
        merged_data = base_df
        zmin_val = td["Value"].min()
        zmax_val = td["Value"].max()

    fig.add_trace(
        go.Heatmap(
            z=merged_data["Value"],
            x=merged_data["X_COORD"],
            y=merged_data["Y_COORD"],
            colorscale=gradientcolor,
            colorbar=dict(
                title=f"Value ({units})",
                x=1.02,
                y=0.3,
                len=0.4,
                yanchor="middle",
            ),
            hoverongaps=False,
            hovertemplate="x: %{x}<br>y: %{y}<br>Value: %{z:.2f}<br>",
            name="",
            zmin=zmin_val,
            zmax=zmax_val,
            visible=True,
            showlegend=False,
        ),
        row=heatmap_row,
        col=2,
    )

    # =========================
    # BOTTONI CON LOGICA SEMPLIFICATA
    # =========================

    # Conteggio tracce
    n_box_traces = len(all_split) * len(all_temps)
    n_hist_traces = len(all_temps)  # Solo per combinazione corrente
    n_heatmap_traces = 1  # Solo una heatmap

    # BOTTONI SPLIT - Cambiano tutto (regen delle tracce histogramma e heatmap)
    split_buttons = []
    for split_idx, split in enumerate(all_split):
        # Per ogni split, prepara i nuovi dati per istogramma e heatmap
        split_data = td[td["Split"] == split] if "Split" in td.columns else td
        corner_data = split_data[split_data["Corner"] == default_corner]

        # Dati istogramma per tutte le temperature
        new_hist_data = {}
        for temp in all_temps:
            temp_data = corner_data[corner_data["°C"] == temp]
            new_hist_data[temp] = (
                temp_data["Value"].tolist() if len(temp_data) > 0 else []
            )

        # Dati heatmap per temperatura di default
        temp_data = corner_data[corner_data["°C"] == default_temp]
        if len(temp_data) > 0:
            heatmap_merged = pd.concat([temp_data, base_df]).drop_duplicates(
                subset=["X_COORD", "Y_COORD"], keep="first"
            )
            new_heatmap_z = heatmap_merged["Value"].tolist()
            new_heatmap_x = heatmap_merged["X_COORD"].tolist()
            new_heatmap_y = heatmap_merged["Y_COORD"].tolist()
            new_zmin = temp_data["Value"].min()
            new_zmax = temp_data["Value"].max()
        else:
            new_heatmap_z = base_df["Value"].tolist()
            new_heatmap_x = base_df["X_COORD"].tolist()
            new_heatmap_y = base_df["Y_COORD"].tolist()
            new_zmin = td["Value"].min()
            new_zmax = td["Value"].max()

        # Costruisci gli aggiornamenti per le tracce
        hist_updates = []
        for temp_idx, temp in enumerate(all_temps):
            hist_updates.append({"x": [new_hist_data[temp]]})

        heatmap_update = {
            "z": [new_heatmap_z],
            "x": [new_heatmap_x],
            "y": [new_heatmap_y],
            "zmin": [new_zmin],
            "zmax": [new_zmax],
        }

        # Indici delle tracce da aggiornare
        hist_trace_indices = list(range(n_box_traces, n_box_traces + n_hist_traces))
        heatmap_trace_index = n_box_traces + n_hist_traces

        split_buttons.append(
            dict(
                method="restyle",
                args=[
                    {
                        **{
                            f"x[{i-n_box_traces}]": new_hist_data[
                                all_temps[i - n_box_traces]
                            ]
                            for i in hist_trace_indices
                        },
                        "z": [new_heatmap_z],
                        "x": [new_heatmap_x] if len(new_heatmap_x) > 0 else [[]],
                        "y": [new_heatmap_y] if len(new_heatmap_y) > 0 else [[]],
                        "zmin": [new_zmin],
                        "zmax": [new_zmax],
                    },
                    hist_trace_indices + [heatmap_trace_index],
                ],
                label=str(split),
            )
        )

    # BOTTONI CORNER - Cambiano istogramma e heatmap
    corner_buttons = []
    for corner_idx, corner in enumerate(corners):
        # Usa split di default
        split_data = td[td["Split"] == default_split] if "Split" in td.columns else td
        corner_data = split_data[split_data["Corner"] == corner]

        # Nuovi dati istogramma
        new_hist_data = {}
        for temp in all_temps:
            temp_data = corner_data[corner_data["°C"] == temp]
            new_hist_data[temp] = (
                temp_data["Value"].tolist() if len(temp_data) > 0 else []
            )

        # Nuovi dati heatmap
        temp_data = corner_data[corner_data["°C"] == default_temp]
        if len(temp_data) > 0:
            heatmap_merged = pd.concat([temp_data, base_df]).drop_duplicates(
                subset=["X_COORD", "Y_COORD"], keep="first"
            )
            new_heatmap_z = heatmap_merged["Value"].tolist()
            new_heatmap_x = heatmap_merged["X_COORD"].tolist()
            new_heatmap_y = heatmap_merged["Y_COORD"].tolist()
            new_zmin = temp_data["Value"].min()
            new_zmax = temp_data["Value"].max()
        else:
            new_heatmap_z = base_df["Value"].tolist()
            new_heatmap_x = base_df["X_COORD"].tolist()
            new_heatmap_y = base_df["Y_COORD"].tolist()
            new_zmin = td["Value"].min()
            new_zmax = td["Value"].max()

        hist_trace_indices = list(range(n_box_traces, n_box_traces + n_hist_traces))
        heatmap_trace_index = n_box_traces + n_hist_traces

        corner_buttons.append(
            dict(
                method="update",
                args=[
                    {
                        **{
                            f"x[{i-n_box_traces}]": new_hist_data[
                                all_temps[i - n_box_traces]
                            ]
                            for i in hist_trace_indices
                        },
                        "z": [new_heatmap_z],
                        "x": [new_heatmap_x] if len(new_heatmap_x) > 0 else [[]],
                        "y": [new_heatmap_y] if len(new_heatmap_y) > 0 else [[]],
                        "zmin": [new_zmin],
                        "zmax": [new_zmax],
                    },
                    hist_trace_indices + [heatmap_trace_index],
                ],
                label=str(corner),
            )
        )

    # BOTTONI TEMPERATURA - Cambiano solo heatmap
    temp_buttons = []
    for temp_idx, temp in enumerate(all_temps):
        # Usa split e corner di default
        split_data = td[td["Split"] == default_split] if "Split" in td.columns else td
        corner_data = split_data[split_data["Corner"] == default_corner]
        temp_data = corner_data[corner_data["°C"] == temp]

        if len(temp_data) > 0:
            heatmap_merged = pd.concat([temp_data, base_df]).drop_duplicates(
                subset=["X_COORD", "Y_COORD"], keep="first"
            )
            new_heatmap_z = heatmap_merged["Value"].tolist()
            new_heatmap_x = heatmap_merged["X_COORD"].tolist()
            new_heatmap_y = heatmap_merged["Y_COORD"].tolist()
            new_zmin = temp_data["Value"].min()
            new_zmax = temp_data["Value"].max()
        else:
            new_heatmap_z = base_df["Value"].tolist()
            new_heatmap_x = base_df["X_COORD"].tolist()
            new_heatmap_y = base_df["Y_COORD"].tolist()
            new_zmin = td["Value"].min()
            new_zmax = td["Value"].max()

        heatmap_trace_index = n_box_traces + n_hist_traces

        temp_buttons.append(
            dict(
                method="update",
                args=[
                    {
                        "z": [new_heatmap_z],
                        "x": [new_heatmap_x] if len(new_heatmap_x) > 0 else [[]],
                        "y": [new_heatmap_y] if len(new_heatmap_y) > 0 else [[]],
                        "zmin": [new_zmin],
                        "zmax": [new_zmax],
                    },
                    [heatmap_trace_index],
                ],
                label=f"{temp}°C",
            )
        )

    # =========================
    # LINEE LIMITE
    # =========================
    if ul != 0 and ll != 0:
        for split_idx in range(n_splits):
            fig.add_vline(
                ul, line_color=limit_color, line_dash="dash", row=split_idx + 1, col=1
            )
            fig.add_vline(
                ll, line_color=limit_color, line_dash="dash", row=split_idx + 1, col=1
            )
        fig.add_vline(ul, line_color=limit_color, line_dash="dash", row=hist_row, col=2)
        fig.add_vline(ll, line_color=limit_color, line_dash="dash", row=hist_row, col=2)

    # =========================
    # LAYOUT FINALE
    # =========================
    wafer_width = x_max - x_min + 1
    wafer_height = y_max - y_min + 1
    wafer_aspect_ratio = wafer_width / wafer_height

    if n_splits == 1:
        base_height = 1200
        if wafer_aspect_ratio != 1.0:
            if wafer_aspect_ratio > 1:
                base_height = int(base_height * (1 + 0.3 * (wafer_aspect_ratio - 1)))
            else:
                base_height = int(
                    base_height * (1 + 0.3 * (1 / wafer_aspect_ratio - 1))
                )
    else:
        base_height = 600 + max(0, (n_splits - 2) * 300)

    fig.update_layout(
        updatemenus=[
            # Menu Split
            dict(
                type="buttons",
                direction="right",
                active=all_split.index(default_split),
                x=0.25,
                y=1.02,
                xanchor="center",
                yanchor="bottom",
                buttons=split_buttons,
                bgcolor="#FFD200",
                bordercolor="#003366",
                borderwidth=0,
                font=dict(size=15, color="#003366"),
                pad=dict(r=5, t=5),
                showactive=True,
            ),
            # Menu Corner
            dict(
                type="buttons",
                direction="right",
                active=corners.index(default_corner),
                x=0.75,
                y=1.02,
                xanchor="center",
                yanchor="bottom",
                buttons=corner_buttons,
                bgcolor="#FFD200",
                bordercolor="#003366",
                borderwidth=0,
                font=dict(size=15, color="#003366"),
                pad=dict(r=5, t=5),
                showactive=True,
            ),
            # Menu Temperature
            dict(
                type="buttons",
                direction="down",
                active=all_temps.index(default_temp),
                x=1.1,
                y=0.5,
                xanchor="left",
                yanchor="middle",
                buttons=temp_buttons,
                bgcolor="#FFD200",
                bordercolor="#003366",
                borderwidth=0,
                font=dict(size=15, color="#003366"),
                pad=dict(r=5, t=5),
                showactive=True,
            ),
        ],
        autosize=True,
        template="plotly_white",
        showlegend=True,
        barmode="overlay",
        margin=dict(l=50, r=120, t=120, b=50),
        height=base_height,
        title_text=str(td["TestName"].iloc[0]),
        title_x=0.5,
        title_font=dict(size=24),
        title_pad=dict(t=10, r=0, b=15, l=0),
        boxmode="group",
    )

    # Configura assi
    for split_idx in range(n_splits):
        fig.update_xaxes(
            title=f"Value ({units}) - Split: {all_split[split_idx]}",
            row=split_idx + 1,
            col=1,
        )
        fig.update_yaxes(title="Corner", row=split_idx + 1, col=1)

    fig.update_xaxes(title=f"Value ({units})", row=hist_row, col=2)
    fig.update_yaxes(title="Count", row=hist_row, col=2)

    fig.update_xaxes(
        title="X Coordinate",
        showgrid=False,
        zeroline=False,
        range=[x_min - 0.5, x_max + 0.5],
        scaleanchor=f"y{total_rows + 2}",
        scaleratio=1,
        constrain="domain",
        row=heatmap_row,
        col=2,
    )
    fig.update_yaxes(
        title="Y Coordinate",
        showgrid=False,
        zeroline=False,
        range=[y_max + 0.5, y_min - 0.5],
        constrain="domain",
        row=heatmap_row,
        col=2,
    )

    return fig


def boxploth(
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

    # Parametri iniziali
    # nbins_fd = freedman_diaconis_rule(td["Value"])
    # corners = sorted(td["Corner"].unique())

    # CORREZIONE: Ordina le temperature numericamente (non come stringhe)
    all_temps = sorted(td["°C"].unique(), key=lambda x: float(x))

    all_split = sorted(td["Split"].unique()) if "Split" in td.columns else ["Default"]

    # Calcola il numero di split e il numero di righe necessarie
    n_splits = len(all_split)
    total_rows = n_splits if n_splits > 0 else 1

    # Specifica per il subplot a singola colonna
    subplot_specs = [[{"type": "xy"}] for _ in range(total_rows)]

    # Crea la figura con un'unica colonna
    fig = make_subplots(
        rows=total_rows,
        cols=1,
        horizontal_spacing=0.08,
        specs=subplot_specs,
    )

    # =========================
    # CALCOLA I LIMITI GLOBALI PER L'ASSE X
    # =========================
    global_min = td["Value"].min()
    global_max = td["Value"].max()

    # Aggiungi un piccolo margine (3% del range)
    value_range = global_max - global_min
    margin = value_range * 0.03
    x_min = global_min - margin
    x_max = global_max + margin

    # =========================
    # PARTE 1: BOX PLOT con facet per Split
    # =========================
    for split_idx, split in enumerate(all_split):
        split_data = td[td["Split"] == split] if "Split" in td.columns else td

        for j, temp in enumerate(all_temps):
            temp_split_data = split_data[split_data["°C"] == temp]

            if len(temp_split_data) > 0:
                fig.add_trace(
                    go.Box(
                        x=temp_split_data["Value"],
                        y=temp_split_data["Corner"],
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
                # fig.add_trace(
                #     go.Violin(
                #         x=temp_split_data["Value"],
                #         y=temp_split_data["Corner"],
                #         name=f"{temp}°C",
                #         fillcolor=STPalette.get(
                #             str(temp), STPalette.get(temp, "#1f77b4")
                #         ),
                #         line_color=STPalette.get(
                #             str(temp), STPalette.get(temp, "#1f77b4")
                #         ),
                #         orientation="h",
                #         points="outliers",  # equivalente a boxpoints="outliers"
                #         spanmode="hard",
                #         visible=True,
                #         legendgroup=f"temp_{temp}",
                #         showlegend=(split_idx == 0),
                #         offsetgroup=f"split_{split}_temp_{temp}",
                #         meanline_visible=True,  # equivalente a boxmean
                #         box_visible=True,  # mostra il box interno
                #         scalemode="width",  # scala la larghezza del violin
                #     ),
                #     row=split_idx + 1,
                #     col=1,
                # )

    # =========================
    # LINEE LIMITE
    # =========================
    if ul != 0 and ll != 0:
        for split_idx in range(n_splits):
            fig.add_vline(
                ul, line_color=limit_color, line_dash="dash", row=split_idx + 1, col=1
            )
            fig.add_vline(
                ll, line_color=limit_color, line_dash="dash", row=split_idx + 1, col=1
            )
            # Aggiungi un piccolo margine (3% del range)
            value_range = global_max - global_min
            margin = value_range * 0.03
            x_min = min(ll, global_min) - margin
            x_max = max(ul, global_max) + margin

    # =========================
    # LAYOUT FINALE
    # =========================
    base_height = 1200 + max(0, (n_splits - 2) * 600)

    fig.update_layout(
        autosize=True,
        template="plotly_white",
        showlegend=True,
        barmode="overlay",
        margin=dict(l=50, r=120, t=120, b=50),
        height=base_height,
        title_text=str(td["TestName"].iloc[0]),
        title_x=0.5,
        title_font=dict(size=24),
        title_pad=dict(t=10, r=0, b=15, l=0),
        boxmode="group",
        violinmode="group",
    )

    # =========================
    # CONFIGURA ASSI CON LIMITI UNIFICATI
    # =========================
    for split_idx in range(n_splits):
        fig.update_xaxes(
            title=f"Value ({units}) - Split: {all_split[split_idx]}",
            range=[x_min, x_max],  # Applica i limiti unificati
            row=split_idx + 1,
            col=1,
        )
        fig.update_yaxes(title="Corner", row=split_idx + 1, col=1)

    return fig


def boxplotv(
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

    # Parametri iniziali
    nbins_fd = freedman_diaconis_rule(td["Value"])
    corners = sorted(td["Corner"].unique())
    all_temps = sorted(td["°C"].unique(), key=lambda x: float(x))
    all_split = sorted(td["Split"].unique()) if "Split" in td.columns else ["Default"]

    # Calcola il numero di split e il numero di colonne necessarie
    n_splits = len(all_split)
    total_cols = n_splits if n_splits > 0 else 1

    # Specifica per il subplot a singola riga
    subplot_specs = [[{"type": "xy"} for _ in range(total_cols)]]

    # Crea la figura con un'unica riga
    fig = make_subplots(
        rows=1,
        cols=total_cols,
        vertical_spacing=0.08,
        specs=subplot_specs,
        subplot_titles=[f"Split: {split}" for split in all_split],
    )

    # =========================
    # PARTE 1: BOX PLOT con facet per Split
    # =========================
    for split_idx, split in enumerate(all_split):
        split_data = td[td["Split"] == split] if "Split" in td.columns else td

        for j, temp in enumerate(all_temps):
            temp_split_data = split_data[split_data["°C"] == temp]

            if len(temp_split_data) > 0:
                fig.add_trace(
                    go.Box(
                        x=temp_split_data["Corner"],
                        y=temp_split_data["Value"],
                        name=f"{temp}°C",
                        marker_color=STPalette.get(
                            str(temp), STPalette.get(temp, "#1f77b4")
                        ),
                        orientation="v",  # Cambiato da "h" a "v" per orientazione verticale
                        boxpoints="outliers",
                        visible=True,
                        legendgroup=f"temp_{temp}",
                        showlegend=(split_idx == 0),
                        offsetgroup=f"split_{split}_temp_{temp}",
                    ),
                    row=1,
                    col=split_idx + 1,
                )

    # =========================
    # LINEE LIMITE
    # =========================
    if ul != 0 and ll != 0:
        for split_idx in range(n_splits):
            # Cambiato da add_vline a add_hline per linee orizzontali
            fig.add_hline(
                ul, line_color=limit_color, line_dash="dash", row=1, col=split_idx + 1
            )
            fig.add_hline(
                ll, line_color=limit_color, line_dash="dash", row=1, col=split_idx + 1
            )

    # =========================
    # LAYOUT FINALE
    # =========================
    base_width = 1200 + max(0, (n_splits - 2) * 600)

    fig.update_layout(
        autosize=True,
        template="plotly_white",
        showlegend=True,
        barmode="overlay",
        margin=dict(
            l=50, r=50, t=120, b=120
        ),  # Aumentato margine bottom per i titoli degli assi
        width=base_width,  # Cambiato da height a width
        title_text=str(td["TestName"].iloc[0]),
        title_x=0.5,
        title_font=dict(size=24),
        title_pad=dict(t=10, r=0, b=15, l=0),
        boxmode="group",
    )

    # Configura assi - invertiti per layout orizzontale
    for split_idx in range(n_splits):
        fig.update_xaxes(
            title="Corner",  # Cambiato: ora Corner è sull'asse X
            row=1,
            col=split_idx + 1,
        )
        fig.update_yaxes(
            title=f"Value ({units})",  # Cambiato: ora Value è sull'asse Y
            row=1,
            col=split_idx + 1,
        )

    return fig


def scatter(
    td,
    STPalette,
    xwafer,
    ywafer,
    gradientcolor=["#03234B", "#3CB4E6", "#FFD200", "#E6007E"],
    limit_color="#E6007E",
):
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots
    import pandas as pd

    # Calcola il totale dei test per ogni combinazione Split/°C/Corner
    total_counts = (
        td.groupby(["Split", "°C", "Corner"]).size().reset_index(name="Total")
    )

    # Calcola i test passati (RESULT = 1) per ogni combinazione
    pass_counts = (
        pd.crosstab([td["Split"], td["°C"], td["Corner"]], td["RESULT"], margins=False)[
            1
        ]
        .reset_index()
        .rename(columns={1: "Pass"})
    )

    # Merge dei dati per calcolare lo Yield
    df = pd.merge(total_counts, pass_counts, on=["Split", "°C", "Corner"], how="left")
    df["Pass"] = df["Pass"].fillna(0)  # Riempie NaN con 0 se non ci sono test passati

    # Calcola lo Yield come percentuale
    df["Yield"] = (df["Pass"] / df["Total"]) * 100

    # Template personalizzato
    STtemplate = go.layout.Template()
    STtemplate.layout = go.Layout(
        plot_bgcolor="white",
        xaxis=dict(gridcolor="#ebf0f8", zerolinecolor="#dee3ea"),
        yaxis=dict(gridcolor="#ebf0f8", zerolinecolor="#dee3ea"),
    )

    # Ottieni valori unici
    corner_orner = [
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
    corners = sorted(
        df["Corner"].unique(),
        key=lambda c: corner_orner.index(c) if c in corner_orner else len(corner_orner),
    )
    temperatures = [
        str(temp) for temp in sorted(int(temp) for temp in df["°C"].unique())
    ]
    n_corners = len(corners)

    # Crea subplot con facet per Corner
    fig = make_subplots(
        rows=n_corners,
        cols=1,
        subplot_titles=[f"Corner: {corner}" for corner in corners],
        # vertical_spacing=0.18,
    )

    # Usa STPalette per i colori delle temperature
    color_map = {
        temp: STPalette.get(temp, list(STPalette.values())[i % len(STPalette)])
        for i, temp in enumerate(temperatures)
    }

    # Aggiungi le tracce per ogni combinazione Corner/Temperatura
    for i, corner in enumerate(corners):
        corner_data = df[df["Corner"] == corner]

        for temp in temperatures:
            temp_data = corner_data[corner_data["°C"] == temp]

            if not temp_data.empty:
                fig.add_trace(
                    go.Scatter(
                        x=temp_data["Split"],
                        y=temp_data["Yield"],
                        mode="markers+lines",
                        name=f"{temp}°C",
                        line=dict(color=color_map[temp]),
                        marker=dict(color=color_map[temp]),
                        showlegend=(
                            i == 0
                        ),  # Mostra la legenda solo per il primo subplot
                        legendgroup=f"{temp}°C",  # Raggruppa le leggende
                        hovertemplate="Split: %{x}<br>Yield: %{y:.1f}%<br>Temp: "
                        + f"{temp}°C",
                    ),
                    row=i + 1,
                    col=1,
                )

    # Aggiorna il layout
    fig.update_layout(
        height=300 * n_corners,
        template=STtemplate,
        hovermode="x unified",
        # autosize=True,
        showlegend=True,
        barmode="overlay",
        margin=dict(l=50, r=120, t=120, b=50),
        title_text=str(td["TestName"].iloc[0]),
        title_x=0.5,
        title_font=dict(size=24),
        title_pad=dict(t=10, r=0, b=15, l=0),
    )

    # Aggiorna gli assi Y con range 0-100% per lo Yield
    fig.update_yaxes(title_text="Yield (%)", range=[-5, 105])

    # Aggiorna l'asse X solo per l'ultimo subplot
    fig.update_xaxes(title_text="Split", row=n_corners, col=1)

    return fig


def generate_standalone_html_dashboard(
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
    Genera un HTML standalone con dashboard interattivo usando Plotly.js
    Ritorna una stringa HTML completa.
    """

    def freedman_diaconis_rule(data):
        if len(data) == 0:
            return 10
        q25, q75 = np.percentile(data, [25, 75])
        iqr = q75 - q25
        if iqr == 0:
            return 10
        bin_width = 2 * iqr / (len(data) ** (1 / 3))
        if bin_width == 0:
            return 10
        nbins = int((data.max() - data.min()) / bin_width)
        return max(10, min(nbins, 100))

    # Parametri - converti numpy types in Python types
    nbins_fd = int(freedman_diaconis_rule(td["Value"]))
    corners = [str(x) for x in sorted(td["Corner"].unique())]
    all_temps = [
        int(x) if isinstance(x, (np.integer, int)) else float(x)
        for x in sorted(td["°C"].unique())
    ]
    all_split = (
        [str(x) for x in sorted(td["Split"].unique())]
        if "Split" in td.columns
        else ["Default"]
    )

    default_corner = "TTTT" if "TTTT" in corners else corners[0]
    default_temp = 30 if 30 in all_temps else all_temps[0]
    default_split = "3v3" if "3v3" in all_split else all_split[0]

    # Prepara dati base per heatmap
    x_min, x_max = xwafer
    y_min, y_max = ywafer
    x_range = np.arange(x_min, x_max + 1)
    y_range = np.arange(y_min, y_max + 1)
    x_mesh, y_mesh = np.meshgrid(x_range, y_range)
    base_df = pd.DataFrame(
        {
            "X_COORD": x_mesh.flatten(),
            "Y_COORD": y_mesh.flatten(),
            "Value": [None] * len(x_mesh.flatten()),
        }
    )

    # Converti dati in formato JSON per JavaScript - gestisci numpy types
    def convert_numpy_types(obj):
        """Converte numpy types in Python types per JSON serialization"""
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        return obj

    # Converti DataFrame in dict e gestisci numpy types
    td_records = td.to_dict("records")
    td_json = []
    for record in td_records:
        converted_record = {}
        for key, value in record.items():
            converted_record[key] = convert_numpy_types(value)
        td_json.append(converted_record)

    base_df_json = []
    for record in base_df.to_dict("records"):
        converted_record = {}
        for key, value in record.items():
            converted_record[key] = (
                convert_numpy_types(value) if value is not None else None
            )
        base_df_json.append(converted_record)

    # Genera figura iniziale
    def create_initial_figure():
        n_splits = len(all_split)
        total_rows = max(n_splits, 2)

        # Create subplot layout
        subplot_specs = []
        row_heights = []

        if n_splits >= 2:
            box_height_per_row = 1.0 / n_splits
            for i in range(n_splits):
                row_heights.append(box_height_per_row)

            for i in range(n_splits):
                if i < n_splits // 2:
                    specs_row = [
                        {"type": "xy"},
                        {"type": "xy", "rowspan": n_splits // 2},
                    ]
                elif i == n_splits // 2:
                    specs_row = [
                        {"type": "xy"},
                        {"type": "xy", "rowspan": n_splits - n_splits // 2},
                    ]
                else:
                    specs_row = [{"type": "xy"}, None]
                subplot_specs.append(specs_row)
        else:
            row_heights = [0.5, 0.5]
            subplot_specs = [
                [{"type": "xy", "rowspan": 2}, {"type": "xy"}],
                [None, {"type": "xy"}],
            ]
            total_rows = 2

        fig = make_subplots(
            rows=total_rows,
            cols=2,
            row_heights=row_heights,
            column_widths=[0.5, 0.5],
            vertical_spacing=0.05,
            horizontal_spacing=0.08,
            specs=subplot_specs,
        )

        hist_row = 1
        heatmap_row = 2 if n_splits == 1 else (n_splits // 2 + 1)

        # Add initial traces (vuoti, verranno aggiornati con JS)
        fig.add_trace(go.Box(x=[], y=[], name="placeholder"), row=1, col=1)
        fig.add_trace(go.Histogram(x=[], name="placeholder"), row=hist_row, col=2)
        fig.add_trace(
            go.Heatmap(z=[], x=[], y=[], name="placeholder"), row=heatmap_row, col=2
        )

        # Layout configuration
        wafer_width = x_max - x_min + 1
        wafer_height = y_max - y_min + 1
        wafer_aspect_ratio = wafer_width / wafer_height

        if n_splits == 1:
            base_height = 1200
        else:
            base_height = 900 + max(0, (n_splits - 2) * 200)

        fig.update_layout(
            autosize=True,
            template="plotly_white",
            showlegend=True,
            barmode="overlay",
            margin=dict(l=50, r=120, t=100, b=50),
            height=base_height,
            title_text=f"{td['TestName'].iloc[0]} | Interactive Dashboard",
            title_x=0.5,
            title_y=0.95,
            title_font=dict(size=20),
            boxmode="group",
        )

        return fig

    initial_fig = create_initial_figure()
    fig_json = initial_fig.to_json()

    # Template HTML
    html_template = f"""
<div>
    <script src="https://cdn.plot.ly/plotly-2.26.0.min.js"></script>
    <style>
        body {{
            margin: 0;
            padding: 0;
            font-family: Arial, sans-serif;
            background-color: #f5f5f5;
        }}
        
        .controls-container {{
            position: fixed;
            top: 10px;
            left: 10px;
            z-index: 1000;
            background: white;
            padding: 15px;
            border-radius: 8px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
            border: 2px solid #003366;
            display: flex;
            flex-wrap: wrap;
            gap: 15px;
            align-items: flex-start;
            max-width: 800px;
        }}
        
        .control-group {{
            min-width: 120px;
        }}
        
        .control-label {{
            display: block;
            font-weight: bold;
            margin-bottom: 5px;
            color: #003366;
            font-size: 12px;
        }}
        
        .control-select {{
            width: 100%;
            padding: 5px;
            border-radius: 4px;
            border: 1px solid #ccc;
            font-size: 12px;
        }}
        
        .split-select {{
            background-color: #FFD200;
        }}
        
        .corner-select {{
            background-color: #3CB4E6;
        }}
        
        .temp-select {{
            background-color: #E6007E;
            color: white;
        }}
        
        .current-state {{
            font-size: 12px;
            color: #666;
            align-self: center;
            margin-top: 20px;
            font-weight: bold;
        }}
        
        #main-graph {{
            height: 800px;
            width: 100%;
            margin-top: 80px;
        }}
    </style>

    <div class="controls-container">
        
        <div class="control-group">
            <label class="control-label">Corner</label>
            <select id="corner-dropdown" class="control-select corner-select">
                {' '.join([f'<option value="{corner}" {"selected" if corner == default_corner else ""}>{corner}</option>' for corner in corners])}
            </select>
        </div>
        
        <div class="control-group">
            <label class="control-label">Temperature</label>
            <select id="temp-dropdown" class="control-select temp-select">
                {' '.join([f'<option value="{temp}" {"selected" if temp == default_temp else ""}>{temp}°C</option>' for temp in all_temps])}
            </select>
        </div>
        
        <div class="current-state">
            <strong>Current: </strong>
            <span id="current-state-text">{default_split}+{default_corner}+{default_temp}°C</span>
        </div>
    </div>
    
    <div id="main-graph"></div>
    
    <script>
        // Dati globali
        const tdData = {json.dumps(td_json)};
        const baseDfData = {json.dumps(base_df_json)};
        const allSplit = {json.dumps(all_split)};
        const allTemps = {json.dumps(all_temps)};
        const corners = {json.dumps(corners)};
        const STPalette = {json.dumps(STPalette)};
        const units = "{units}";
        const testName = "{td['TestName'].iloc[0]}";
        const xMin = {int(x_min)};
        const xMax = {int(x_max)};
        const yMin = {int(y_min)};
        const yMax = {int(y_max)};
        const nbinsFd = {nbins_fd};
        const gradientColor = {json.dumps(gradientcolor)};
        
        // Funzione per filtrare i dati
        function filterData(data, filters) {{
            return data.filter(row => {{
                let match = true;
                if (filters.split && row.Split !== filters.split) match = false;
                if (filters.corner && row.Corner !== filters.corner) match = false;
                if (filters.temp && row['°C'] !== filters.temp) match = false;
                return match;
            }});
        }}
        
        // Funzione per creare le tracce box plot
        function createBoxTraces(selectedSplit) {{
            const traces = [];
            const splitData = filterData(tdData, {{ split: selectedSplit }});
            
            allSplit.forEach((split, splitIdx) => {{
                const currentSplitData = filterData(tdData, {{ split: split }});
                
                allTemps.forEach(temp => {{
                    const tempSplitData = filterData(currentSplitData, {{ temp: temp }});
                    
                    // Raggruppa per corner
                    const cornerGroups = {{}};
                    tempSplitData.forEach(row => {{
                        if (!cornerGroups[row.Corner]) cornerGroups[row.Corner] = [];
                        cornerGroups[row.Corner].push(row.Value);
                    }});
                    
                    Object.keys(cornerGroups).forEach(corner => {{
                        traces.push({{
                            type: 'box',
                            x: cornerGroups[corner],
                            y: Array(cornerGroups[corner].length).fill(corner),
                            name: `${{temp}}°C`,
                            marker: {{ color: STPalette[temp.toString()] || '#1f77b4' }},
                            orientation: 'h',
                            boxpoints: 'outliers',
                            visible: true,
                            legendgroup: `temp_${{temp}}`,
                            showlegend: splitIdx === 0,
                            offsetgroup: `split_${{split}}_temp_${{temp}}`,
                            xaxis: `x${{splitIdx + 1}}`,
                            yaxis: `y${{splitIdx + 1}}`
                        }});
                    }});
                }});
            }});
            
            return traces;
        }}
        
        // Funzione per creare le tracce histogram
        function createHistTraces(selectedSplit, selectedCorner) {{
            const traces = [];
            const filteredData = filterData(tdData, {{ split: selectedSplit, corner: selectedCorner }});
            
            allTemps.forEach(temp => {{
                const tempData = filterData(filteredData, {{ temp: temp }});
                const values = tempData.map(row => row.Value);
                
                traces.push({{
                    type: 'histogram',
                    x: values,
                    name: `${{temp}}°C`,
                    marker: {{ color: STPalette[temp.toString()] || '#1f77b4' }},
                    opacity: 0.6,
                    visible: true,
                    legendgroup: `temp_${{temp}}`,
                    showlegend: false,
                    nbinsx: nbinsFd,
                    xaxis: 'x{len(all_split) + 1}',
                    yaxis: 'y{len(all_split) + 1}'
                }});
            }});
            
            return traces;
        }}
        
        // Funzione per creare la traccia heatmap
        function createHeatmapTrace(selectedSplit, selectedCorner, selectedTemp) {{
            const filteredData = filterData(tdData, {{ 
                split: selectedSplit, 
                corner: selectedCorner, 
                temp: selectedTemp 
            }});
            
            // Crea una mappa delle coordinate
            const coordMap = new Map();
            filteredData.forEach(row => {{
                const key = `${{row.X_COORD}}_${{row.Y_COORD}}`;
                coordMap.set(key, row.Value);
            }});
            
            // Crea griglia completa
            const xRange = [];
            const yRange = [];
            const zValues = [];
            
            for (let x = xMin; x <= xMax; x++) {{
                xRange.push(x);
            }}
            
            for (let y = yMax; y >= yMin; y--) {{
                yRange.push(y);
                const row = [];
                for (let x = xMin; x <= xMax; x++) {{
                    const key = `${{x}}_${{y}}`;
                    row.push(coordMap.get(key) || null);
                }}
                zValues.push(row);
            }}
            
            // Calcola min/max per la scala colore
            let zmin = null, zmax = null;
            if (filteredData.length > 0) {{
                const values = filteredData.map(row => row.Value);
                zmin = Math.min(...values);
                zmax = Math.max(...values);
            }} else {{
                const allValues = tdData.map(row => row.Value);
                zmin = Math.min(...allValues);
                zmax = Math.max(...allValues);
            }}
            
            return {{
                type: 'heatmap',
                z: zValues,
                x: xRange,
                y: yRange,
                colorscale: gradientColor.map((color, idx) => [idx / (gradientColor.length - 1), color]),
                colorbar: {{
                    title: `Value (${{units}})`,
                    x: 1.02,
                    y: 0.3,
                    len: 0.4,
                    yanchor: 'middle'
                }},
                hoverongaps: false,
                hovertemplate: 'x: %{{x}}<br>y: %{{y}}<br>Value: %{{z:.2f}}<br><extra></extra>',
                zmin: zmin,
                zmax: zmax,
                visible: true,
                showlegend: false,
                xaxis: 'x{len(all_split) + 2}',
                yaxis: 'y{len(all_split) + 2}'
            }};
        }}
        
        // Funzione per aggiornare il grafico
        function updateGraph() {{
            const selectedSplit = document.getElementById('split-dropdown').value;
            const selectedCorner = document.getElementById('corner-dropdown').value;
            const selectedTemp = parseInt(document.getElementById('temp-dropdown').value);
            
            // Aggiorna testo stato corrente
            document.getElementById('current-state-text').textContent = 
                `${{selectedSplit}}+${{selectedCorner}}+${{selectedTemp}}°C`;
            
            // Crea tutte le tracce
            const boxTraces = createBoxTraces(selectedSplit);
            const histTraces = createHistTraces(selectedSplit, selectedCorner);
            const heatmapTrace = createHeatmapTrace(selectedSplit, selectedCorner, selectedTemp);
            
            // Combina tutte le tracce
            const allTraces = [...boxTraces, ...histTraces, heatmapTrace];
            
            // Layout
            const nSplits = allSplit.length;
            const totalRows = Math.max(nSplits, 2);
            const baseHeight = nSplits === 1 ? 800 : 700 + Math.max(0, (nSplits - 2) * 200);
            
            const layout = {{
                autosize: true,
                template: 'plotly_white',
                showlegend: true,
                barmode: 'overlay',
                margin: {{ l: 50, r: 120, t: 100, b: 50 }},
                height: baseHeight,
                title: {{
                    text: `${{testName}} | Current: ${{selectedSplit}}+${{selectedCorner}}+${{selectedTemp}}°C`,
                    x: 0.5,
                    y: 0.95,
                    font: {{ size: 20 }}
                }},
                boxmode: 'group',
                grid: {{
                    rows: totalRows,
                    columns: 2,
                    pattern: 'independent'
                }}
            }};
            
            // Aggiorna assi
            for (let i = 0; i < nSplits; i++) {{
                layout[`xaxis${{i === 0 ? '' : i + 1}}`] = {{
                    title: `Value (${{units}}) - Split: ${{allSplit[i]}}`,
                    domain: [0, 0.45]
                }};
                layout[`yaxis${{i === 0 ? '' : i + 1}}`] = {{
                    title: 'Corner'
                }};
            }}
            
            // Asse histogram
            const histAxisNum = nSplits + 1;
            layout[`xaxis${{histAxisNum}}`] = {{
                title: `Value (${{units}})`,
                domain: [0.55, 1.0]
            }};
            layout[`yaxis${{histAxisNum}}`] = {{
                title: 'Count'
            }};
            
            // Asse heatmap
            const heatmapAxisNum = nSplits + 2;
            layout[`xaxis${{heatmapAxisNum}}`] = {{
                title: 'X Coordinate',
                showgrid: false,
                zeroline: false,
                range: [xMin - 0.5, xMax + 0.5],
                scaleanchor: `y${{heatmapAxisNum}}`,
                scaleratio: 1,
                constrain: 'domain',
                domain: [0.55, 1.0]
            }};
            layout[`yaxis${{heatmapAxisNum}}`] = {{
                title: 'Y Coordinate',
                showgrid: false,
                zeroline: false,
                range: [yMax + 0.5, yMin - 0.5],
                constrain: 'domain'
            }};
            
            // Plotta il grafico
            Plotly.newPlot('main-graph', allTraces, layout, {{responsive: true}});
        }}
        
        // Event listeners
        document.getElementById('split-dropdown').addEventListener('change', updateGraph);
        document.getElementById('corner-dropdown').addEventListener('change', updateGraph);
        document.getElementById('temp-dropdown').addEventListener('change', updateGraph);
        
        // Inizializza il grafico
        document.addEventListener('DOMContentLoaded', function() {{
            updateGraph();
        }});
    </script>
</div>
"""

    return html_template


def generate_colored_ptrtable_html(
    df: pd.DataFrame,
):
    """
    Versione migliorata che restituisce l'HTML della tabella PTR con:
    - Nasconde la colonna indice
    - Mostra sempre tutti i dati
    - Ordinamento multi-colonna (prima per temperatura, poi alfabetico)
    - Colorazione specifica per Cpk, Yield, Cp
    - Pulsante per esportare in CSV
    """
    if df.empty:
        return "<div>Il DataFrame è vuoto.</div>"

    # 1. Ordinamento multi-colonna (prima per temperatura, poi alfabetico)
    df_sorted = df.reset_index().sort_values(by=["°C", "Corner", "Split"])

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
    df: pd.DataFrame,
):
    """
    Versione migliorata che restituisce l'HTML della tabella con:
    - Nasconde la colonna indice
    - Mostra sempre tutti i dati
    - Filtri checkbox per le prime 4 colonne
    - Pulsante per esportare in CSV
    """
    if df.empty:
        return "<div>Il DataFrame è vuoto.</div>"

    # Genera HTML base della tabella (senza indice)
    html_table = df.to_html(
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
    #colored-table tbody tr td:nth-child(3),
    #colored-table tbody tr td:nth-child(4) {{
        background-color: {STyellow};
        color: {STblue};
        font-weight: bold;
    }}
    
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
    const tableData = {df.to_json(orient='records')};
    const columnNames = {df.columns.tolist()};
    
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


def generate_colored_ftrtable(
    df: pd.DataFrame,
):
    """
    Genera una tabella HTML con celle colorate, raggruppamento visivo,
    filtri interattivi e gestione duplicati intelligente.

    Caratteristiche:
    - Header e colonne indice in giallo
    - Righe alternate con colori diversi
    - Raggruppamento per evitare duplicati nelle metriche
    - Filtri dropdown sulle prime tre colonne
    """
    if df.empty:
        return "<div>Il DataFrame è vuoto.</div>"

    # 1. Preparazione dati e ordinamento
    # df["°C"] = pd.to_numeric(df["°C"], errors="coerce")
    df_sorted = df.reset_index().sort_values(by=["°C", "Corner", "Metric"])

    # 3. Creazione matrice colori
    num_rows = len(df_sorted)
    fill_colors = []

    for col in df_sorted.columns:
        col_colors = []
        for row_idx in range(num_rows):
            # Colonne indice in giallo
            if col in ["°C", "Corner", "Metric"] or col == df_sorted.columns[0]:
                col_colors.append(STyellow)
            else:
                # Righe alternate per le altre colonne
                if row_idx % 2 == 0:
                    col_colors.append(color_light)
                else:
                    col_colors.append(color_dark)
        fill_colors.append(col_colors)

    # 4. Creazione tabella base
    fig = go.Figure(
        data=[
            go.Table(
                header=dict(
                    values=list(df_sorted.columns),
                    fill_color=STyellow,
                    font=dict(color=STblue, size=12, family="Arial"),
                    align="center",
                    height=30,
                ),
                cells=dict(
                    values=[df_sorted[col] for col in df_sorted.columns],
                    fill_color=fill_colors,
                    align="center",
                    font=dict(
                        color=[
                            STblue if col in ["°C", "Corner", "Metric"] else "black"
                            for col in df_sorted.columns
                        ],
                        size=11,
                        family="Arial",
                    ),
                    height=30,
                ),
            )
        ]
    )

    # 5. Layout con controlli interattivi
    fig.update_layout(
        height=min(900, 40 + 32 * len(df_sorted) + 150),
        margin=dict(l=20, r=20, t=100, b=20),
        paper_bgcolor="white",
        plot_bgcolor="white",
    )

    return plot(fig, output_type="div", include_plotlyjs=True)


def generate_colored_ptrtable(df: pd.DataFrame):
    """
    Genera una tabella HTML con celle colorate, raggruppamento visivo
    e ordinamento alfabetico per Corner e Split.
    """
    if df.empty:
        return "<div>Il DataFrame è vuoto.</div>"

    # 1. Ordinamento multi-colonna (prima per temperatura, poi alfabetico)
    df_sorted = df.reset_index().sort_values(by=["°C", "Corner", "Split"])

    # 2. Mappatura delle colonne alle funzioni di colorazione
    color_functions = {
        "Cpk": color_cpk,
        "Yield": color_yield,
        "Cp": color_cp,
    }

    # 3. Creazione della matrice dei colori
    num_rows = len(df_sorted)
    fill_colors = []

    # Per ogni colonna, crea la lista dei colori per tutte le righe
    for col in df_sorted.columns:
        col_colors = []
        for row_idx in range(num_rows):
            # Prima determina il colore di base (alternato o giallo per indici)
            if col in ["°C", "Corner", "Split", "index"]:
                base_color = STyellow
            else:
                # Colori alternati per tutte le altre colonne
                if row_idx % 2 == 0:
                    base_color = color_light
                else:
                    base_color = color_dark

            # Se la colonna ha una funzione di colorazione specifica,
            # sovrascrive il colore di base solo se necessario
            if col in color_functions:
                specific_color = color_functions[col](df_sorted.iloc[row_idx][col])
                # Usa il colore specifico se non è None, altrimenti mantieni quello alternato
                if specific_color is not None:
                    col_colors.append(specific_color)
                else:
                    col_colors.append(base_color)
            else:
                col_colors.append(base_color)
        fill_colors.append(col_colors)

    # 4. Creazione della tabella Plotly
    fig = go.Figure(
        data=[
            go.Table(
                header=dict(
                    values=list(df_sorted.columns),
                    fill_color=STyellow,
                    font=dict(color=STblue, size=12, family="Arial"),
                    align="center",
                    height=35,
                ),
                cells=dict(
                    values=[df_sorted[col] for col in df_sorted.columns],
                    fill_color=fill_colors,  # Usa la matrice dei colori creata
                    align="center",
                    font=dict(
                        color=[
                            (
                                STblue
                                if col in ["°C", "Corner", "Split", "index"]
                                else "black"
                            )
                            for col in df_sorted.columns
                        ],
                        size=11,
                        family="Arial",
                    ),
                    height=30,
                ),
            )
        ]
    )

    fig.update_layout(
        height=min(800, 35 * len(df_sorted) + 100),  # Altezza dinamica
        margin=dict(l=20, r=20, t=60, b=20),
    )

    return plot(fig, output_type="div", include_plotlyjs=True)
