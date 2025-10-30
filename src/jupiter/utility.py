import numpy


def read_with_fallback(path):
    import polars as pl
    from pandas.errors import EmptyDataError, ParserError
    import os

    try:
        return pl.read_parquet(path)
    except (EmptyDataError, FileNotFoundError, ParserError) as e:
        # print("ERROR:", e)
        return pl.DataFrame()


# Customize cell colors
def color_cpk(val):
    try:
        val = float(val)
    except ValueError:
        return ""
    if isinstance(val, (int, float)):
        if val < 1.2:
            return "background-color: #F23202"
        elif 1.2 <= val < 1.3:
            return "background-color: #E85D04"
        elif 1.3 <= val < 1.4:
            return "background-color: #F48C06"
        elif 1.4 <= val < 1.5:
            return "background-color: #FAA307F0"
        elif 1.5 <= val <= 1.6:
            return "background-color: #FFBA08F1"
        else:
            return ""
    return ""


def color_yield(val):
    try:
        val = float(val.strip("%"))
    except ValueError:
        return ""
    if isinstance(val, (int, float)):
        if val < 50:
            return "background-color: #F23202"
        elif 50 <= val < 60:
            return "background-color: #E85D04"
        elif 60 <= val < 70:
            return "background-color: #F48C06"
        elif 70 <= val < 80:
            return "background-color: #FAA307F0"
        elif 80 <= val <= 99:
            return "background-color: #FFBA08F1"
        else:
            return ""
    return ""


def color_kurtosis(val):
    try:
        val = float(val)
    except ValueError:
        return ""
    if isinstance(val, (int, float)):
        if val > -0.2:
            return "background-color: #F23202"
        elif -0.2 >= val > -0.4:
            return "background-color: #E85D04"
        elif -0.4 >= val > -0.6:
            return "background-color: #F48C06"
        elif -0.6 >= val > -0.8:
            return "background-color: #FAA307F0"
        elif -0.8 >= val >= -1.0:
            return "background-color: #FFBA08F1"
        else:
            return ""
    return ""


# Customize cell colors
def color_cp(val):
    try:
        val = float(val)
    except ValueError:
        return ""
    if isinstance(val, (int, float)):
        if val < 6:
            return "background-color: #F23202"
        elif 6 <= val < 7:
            return "background-color: #E85D04"
        elif 7 <= val < 8:
            return "background-color: #F48C06"
        elif 8 <= val < 9:
            return "background-color: #FAA307F0"
        elif 9 <= val <= 10:
            return "background-color: #FFBA08F1"
        else:
            return ""
    return ""


def power_of_10(value):
    if value >= 0:
        return 10**value
    else:
        return 1 / (10 ** abs(value))


def find_value(value, calc_type):
    if value == 0:
        if calc_type == "min":
            min_value = -0.01
            # print(f"Valore attuale: {value} Minimo: {min_value}")
            return 0.01
        elif calc_type == "max":
            max_value = 0.01
            # print(f"Valore attuale: {value} Massimo: {max_value}")
            return -0.01
    elif value < 0:
        if calc_type == "min":
            min_value = value - (value * 0.001)
            # print(f"Valore attuale: {value} Minimo: {min_value}")
            return value - (value * 0.001)
        elif calc_type == "max":
            max_value = value + (value * 0.001)
            # print(f"Valore attuale: {value} Massimo: {max_value}")
            return value + (value * 0.001)
    else:
        if calc_type == "min":
            min_value = value + (value * 0.001)
            # print(f"Valore attuale: {value} Minimo: {min_value}")
            return value + (value * 0.001)
        elif calc_type == "max":
            max_value = value - (value * 0.001)
            # print(f"Valore attuale: {value} Massimo: {max_value}")
            return value - (value * 0.001)


def write_log(message, filename="../run.log"):
    import os
    import datetime

    now = datetime.datetime.now()
    timestamp = now.strftime("[%Y-%m-%d %H:%M:%S]")

    try:
        with open(filename, "r+") as file:
            content = file.readlines()
            # Limit the content to the last 499 lines
            if len(content) >= 500:
                content = content[:499]

            # Calculate time difference if there is a previous log entry
            if content:
                last_timestamp_str = content[0].split("] ")[0].strip("[ ]")
                last_timestamp = datetime.datetime.strptime(
                    last_timestamp_str, "%Y-%m-%d %H:%M:%S"
                )
                time_diff = now - last_timestamp
                diff_str = " (+{})".format(str(time_diff).split(".")[0])
            else:
                diff_str = ""

            # Insert the new log entry at the beginning
            content.insert(0, timestamp + diff_str + " |--> " + message + "\n")
            # Write back the content to the file
            file.seek(0)
            file.writelines(content)
            file.truncate()
    except FileNotFoundError:
        with open(filename, "w") as file:
            file.write(timestamp + " |--> " + message + "\n")
            file.write(timestamp + " Log file created.\n")
    except OSError as e:
        print(f"An error occurred: {e}")


def interpolate_color(color1, color2, factor: float):
    """Interpolates between two colors."""
    return [color1[i] + (color2[i] - color1[i]) * factor for i in range(3)]


def hex_to_rgb(hex_color: str):
    """Converts a hex color to an RGB tuple."""
    hex_color = hex_color.lstrip("#")
    return tuple(int(hex_color[i : i + 2], 16) for i in (0, 2, 4))


def rgb_to_hex(rgb_color):
    """Converts an RGB tuple to a hex color."""
    return "#{:02x}{:02x}{:02x}".format(
        int(rgb_color[0]), int(rgb_color[1]), int(rgb_color[2])
    )


def create_gradient(colors, num_colors):
    """Creates a gradient of colors."""
    gradient = []
    num_segments = len(colors) - 1
    colors_per_segment = num_colors // num_segments

    for i in range(num_segments):
        color1 = hex_to_rgb(colors[i])
        color2 = hex_to_rgb(colors[i + 1])

        for j in range(colors_per_segment):
            factor = j / float(colors_per_segment)
            interpolated_color = interpolate_color(color1, color2, factor)
            gradient.append(rgb_to_hex(interpolated_color))

    # Add the last color
    gradient.append(colors[-1])

    return gradient


def create_heatmap(td, gradientcolor, xwafer, ywafer):
    import numpy as np
    import pandas as pd
    import plotly.graph_objects as go

    std_dev = np.std(td["Value"])
    step = std_dev / 10
    if step < 1e-5:
        step = std_dev

    fig = go.Figure(
        data=go.Heatmap(
            z=td["Value"].astype(float),
            x=td["XId"].astype(int),
            y=td["YId"].astype(int),
            colorscale=gradientcolor,
            colorbar=dict(title="Value"),
            hoverongaps=False,
            hovertemplate="x: %{x}<br>y: %{y}<br>Value: %{z:.2f}<br>",
            name="",
            zmin=td["Value"].min(),
            zmax=td["Value"].max(),
        )
    )

    fig.update_layout(
        xaxis=dict(
            showgrid=False,
            zeroline=False,
            showticklabels=False,
            range=xwafer,
            scaleanchor="x",
            scaleratio=1,
        ),
        yaxis=dict(
            showgrid=False,
            zeroline=False,
            showticklabels=False,
            range=ywafer[::-1],
            scaleanchor="y",
            scaleratio=1,
        ),
        showlegend=False,
        autosize=True,
        margin=dict(l=0, r=0, t=0, b=0),
        height=1000,
        plot_bgcolor="#FFFFFF",
    )

    if step >= 1e-5:
        z_values = np.arange(td["Value"].min(), td["Value"].max() + step, step)

        steps_zmin = [
            {
                "label": f"{i:.2f}",
                "method": "restyle",
                "args": [{"zmin": [i], "zauto": False}],
            }
            for i in z_values
        ]

        steps_zmax = [
            {
                "label": f"{i:.2f}",
                "method": "restyle",
                "args": [{"zmax": [i], "zauto": False}],
            }
            for i in z_values
        ]

        fig.update_layout(
            sliders=[
                {
                    "active": 0,
                    "steps": steps_zmin,
                    "currentvalue": {"prefix": "Min Z: "},
                    "pad": {"b": 10},
                    "x": 0.1,
                    "len": 0.35,
                    "tickcolor": "white",
                    "minorticklen": 0,
                    "ticklen": 0,
                    "tickwidth": 0,
                },
                {
                    "active": len(steps_zmax) - 1,
                    "steps": steps_zmax,
                    "currentvalue": {"prefix": "Max Z: "},
                    "pad": {"b": 10},
                    "x": 0.55,
                    "len": 0.35,
                    "tickcolor": "white",
                    "minorticklen": 0,
                    "ticklen": 0,
                    "tickwidth": 0,
                },
            ]
        )

    fig.show()


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
    return int(np.ceil(data_range / bin_width))


def create_histogram(td, units, ul, ll, maxvalue, minvalue, STPalette, STred):
    import plotly_express as px

    nbins_fd = freedman_diaconis_rule(td["Value"])
    fig = px.histogram(
        td[["Value", "XId", "YId"]],
        x="Value",
        nbins=nbins_fd,
        marginal="box",
        hover_data=td[["Value", "XId", "YId"]],
        barmode="overlay",
        template="plotly_white",
        color_discrete_sequence=STPalette,
    )

    if ul != 0 and ll != 0:
        fig.add_vline(ul, line_color=STred, line_dash="dash")
        fig.add_vline(ll, line_color=STred, line_dash="dash")

    if abs(maxvalue - minvalue) < 1 and ul == 0 and ll == 0:
        difference = abs(maxvalue - minvalue) * 2
        range_min = minvalue - difference
        range_max = maxvalue + difference
        fig.update_layout(xaxis=dict(range=[range_min, range_max]))

    if units and "nan" not in units:
        fig.update_layout(
            xaxis=dict(title="Value ({})".format(units)),
            height=300,
        )
    else:
        fig.update_layout(
            xaxis=dict(title="Value (uint)"),
            height=300,
        )

    fig.show()


def create_histogram_with_color(
    td, units, ul, ll, maxvalue, minvalue, tempSTcolort, STred
):
    import plotly_express as px

    fig = px.histogram(
        td[["Value", "XId", "YId", "Volt"]],
        x="Value",
        color="Volt",
        marginal="box",
        hover_data=td[["Value", "XId", "YId"]],
        barmode="overlay",
        template="plotly_white",
        color_discrete_sequence=tempSTcolort,
    )

    if ul != 0 and ll != 0:
        fig.add_vline(ul, line_color=STred, line_dash="dash")
        fig.add_vline(ll, line_color=STred, line_dash="dash")

    if abs(maxvalue - minvalue) < 1 and ul == 0 and ll == 0:
        difference = abs(maxvalue - minvalue) * 2
        range_min = minvalue - difference
        range_max = maxvalue + difference
        fig.update_layout(xaxis=dict(range=[range_min, range_max]))

    if units and "nan" not in units:
        fig.update_layout(
            xaxis=dict(title="Value ({})".format(units)),
            height=500,
        )
    else:
        fig.update_layout(
            xaxis=dict(title="Value (uint)"),
            height=500,
        )

    fig.show()


def get_product_image(product):
    import os

    folder_path = os.path.join(product)

    for filename in os.listdir(folder_path):
        if filename.lower().endswith(".svg"):
            svg_path = os.path.join(folder_path, filename)
            with open(svg_path, "r", encoding="utf-8") as f:
                content = f.read()
            return content

    return ""


def get_personalization(parameter, name, old=None):
    import os
    import json5

    file_path = os.path.join(
        parameter["MAIN"].split(parameter["CODE"])[0],
        parameter["CODE"],
        "ART.jsonc",
    )

    if not os.path.isfile(file_path):
        return old

    with open(file_path, "r", encoding="utf-8") as file:
        dati = json5.load(file)

    if name in dati:
        dato = dati[name]
        return dato
    else:
        return old
