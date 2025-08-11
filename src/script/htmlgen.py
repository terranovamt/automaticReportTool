import os
import json
import datetime
import pandas as pd
import plotly.offline as pyo
from scipy.stats import kurtosis


# MAIN_PATH = "\\\\gpm-pe-data.gnb.st.com\\ENGI_MCD_STDF"
MAIN_PATH = ".\\STDF"
HEAD = "[CHAR]"
FLUSH = " " * 200

import pandas as pd
from scipy.stats import kurtosis

import pandas as pd
from scipy.stats import kurtosis

try:
    import script.graph as graph
except ModuleNotFoundError:
    import graph as graph


def calculate_clamp_threshold(value, threshold_type, adjustment_percent=0.01):
    """
    Calculate clamp threshold values with percentage-based adjustment.

    Args:
        value: Current extreme value (min or max)
        threshold_type: 'min' or 'max'
        adjustment_percent: Percentage to adjust the threshold (default 0.01 = 1%)

    Returns:
        Calculated threshold value
    """
    if value == 0:
        return adjustment_percent if threshold_type == "max" else -adjustment_percent

    if threshold_type == "min":
        # For minimum: if negative, make less negative; if positive, make more positive
        if value < 0:
            return value * (1 - adjustment_percent)  # Less negative (closer to 0)
        else:
            return value * (1 + adjustment_percent)  # More positive
    else:  # threshold_type == "max"
        # For maximum: if negative, make more negative; if positive, make less positive
        if value < 0:
            return value * (1 + adjustment_percent)  # More negative
        else:
            return value * (1 - adjustment_percent)  # Less positive


def detect_clamps(subset, limits, std_multiplier=0.9, adjustment_percent=0.01):
    """
    Detect clamp values using improved distance-based algorithm.

    Algorithm:
    1. Find min and max values
    2. Find second min and second max
    3. Calculate distance between extreme and second extreme
    4. If distance > (std_deviation * std_multiplier), it's likely a clamp
    5. Set threshold by adjusting extreme value by adjustment_percent
    6. Filter values beyond threshold

    Args:
        subset: DataFrame subset for specific corner/temperature
        limits: Dict with 'low' and 'high' limit values
        std_multiplier: Multiplier for std deviation to determine clamp distance (default 0.9)
        adjustment_percent: Percentage to adjust threshold from extreme value (default 0.01 = 1%)

    Returns:
        Tuple of (clamps_min_df, clamps_max_df, filtered_subset)
    """
    if subset.empty:
        return pd.DataFrame(), pd.DataFrame(), subset

    clamps_min = pd.DataFrame()
    clamps_max = pd.DataFrame()

    # Get basic statistics
    values = subset["Value"].copy()
    std_dev = values.std()

    # If std is 0 or very small, no clamps can be detected reliably
    if std_dev == 0 or std_dev < 1e-10:
        return clamps_min, clamps_max, subset

    # Get sorted unique values for distance calculation
    sorted_values = values.drop_duplicates().sort_values().reset_index(drop=True)

    if len(sorted_values) < 2:
        # Not enough unique values to detect clamps
        return clamps_min, clamps_max, subset

    min_val = sorted_values.iloc[0]
    max_val = sorted_values.iloc[-1]

    # Check if limits exist (both non-zero means limits are set)
    has_limits = limits["low"] != 0 or limits["high"] != 0

    # Detect minimum clamps
    if len(sorted_values) >= 2:
        second_min = sorted_values.iloc[1]
        distance_min = abs(second_min - min_val)

        # Check if distance is significant compared to standard deviation
        is_min_clamp = distance_min > (std_dev * std_multiplier)

        # Additional check: if limits exist, min should violate low limit
        violates_limit = not has_limits or (
            limits["low"] != 0 and min_val < limits["low"]
        )

        if is_min_clamp and violates_limit:
            # Calculate threshold by adjusting minimum value
            threshold_min = calculate_clamp_threshold(
                min_val, "min", adjustment_percent
            )
            potential_clamps_min = subset.query("Value <= @threshold_min")

            if not potential_clamps_min.empty:
                clamps_min = potential_clamps_min.assign(clamp_threshold=threshold_min)
                subset = subset.query("Value > @threshold_min")
                print(
                    HEAD,
                    f"Min clamp detected: {len(clamps_min)} values <= {threshold_min:.3f} (distance: {distance_min:.3f}, std: {std_dev:.3f})",
                    FLUSH,
                    end="\r",
                    flush=True,
                )

    # Detect maximum clamps
    if len(sorted_values) >= 2:
        second_max = sorted_values.iloc[-2]
        distance_max = abs(max_val - second_max)

        # Check if distance is significant compared to standard deviation
        is_max_clamp = distance_max > (std_dev * std_multiplier)

        # Additional check: if limits exist, max should violate high limit
        violates_limit = not has_limits or (
            limits["high"] != 0 and max_val > limits["high"]
        )

        if is_max_clamp and violates_limit:
            # Calculate threshold by adjusting maximum value
            threshold_max = calculate_clamp_threshold(
                max_val, "max", adjustment_percent
            )
            potential_clamps_max = subset.query("Value >= @threshold_max")

            if not potential_clamps_max.empty:
                clamps_max = potential_clamps_max.assign(clamp_threshold=threshold_max)
                subset = subset.query("Value < @threshold_max")
                print(
                    HEAD,
                    f"Max clamp detected: {len(clamps_max)} values >= {threshold_max:.3f} (distance: {distance_max:.3f}, std: {std_dev:.3f})",
                    FLUSH,
                    end="\r",
                    flush=True,
                )

    return clamps_min, clamps_max, subset


def calculate_process_capability(data, limits, std_val, mean_val):
    """
    Calculate Cp and Cpk process capability indices.

    Args:
        data: DataFrame with values
        limits: Dict with 'low' and 'high' limits
        std_val: Standard deviation
        mean_val: Mean value

    Returns:
        Dict with Cp, Cpk values or "-" if no limits
    """
    if limits["low"] == 0 and limits["high"] == 0:
        return {"Cp": "-", "Cpk": "-"}

    if std_val == 0:
        return {"Cp": "-", "Cpk": "-"}

    # Calculate Cp (process capability)
    cp = (limits["high"] - limits["low"]) / (6 * std_val)

    # Calculate Cpk (process capability index)
    cpu = (limits["high"] - mean_val) / (3 * std_val)  # Upper capability
    cpl = (mean_val - limits["low"]) / (3 * std_val)  # Lower capability
    cpk = min(cpu, cpl)

    return {"Cp": round(cp, 3), "Cpk": round(cpk, 3)}


def calculate_yield_metrics(data, limits):
    """
    Calculate yield and failure metrics.

    Args:
        data: DataFrame with values grouped by temperature and corner
        limits: Dict with 'low' and 'high' limits

    Returns:
        Dict with yield metrics or "-" if no limits
    """
    if limits["low"] == 0 and limits["high"] == 0:
        return {"yield": "-", "fail_count": "-"}

    data["is_within_limits"] = (data["Value"] >= limits["low"]) & (
        data["Value"] <= limits["high"]
    )

    yield_data = data.groupby(["°C", "Corner"]).agg(
        within_limits=("is_within_limits", "sum"), total=("is_within_limits", "count")
    )

    yield_data["yield_pct"] = round(
        (yield_data["within_limits"] / yield_data["total"] * 100), 3
    )
    yield_data["fail_count"] = yield_data["total"] - yield_data["within_limits"]

    return {"yield": yield_data["yield_pct"], "fail_count": yield_data["fail_count"]}


def process_test_data(td):
    """
    Main function to process test data and calculate statistics with improved clamp detection.

    Args:
        td: Input DataFrame with test data

    Returns:
        DataFrame with statistical summary and conditional clamp information
    """
    print(HEAD, f"Starting test data processing...", FLUSH, end="\r", flush=True)

    if td.empty:
        print(HEAD, f"Empty dataframe received", FLUSH, end="\r", flush=True)
        return pd.DataFrame()

    # Data preparation
    td = td.copy()
    td["°C"] = td["°C"].astype("str")
    td["Unit"] = td["Unit"].fillna("")

    print(HEAD, f"Data prepared - {len(td)} rows", FLUSH, end="\r", flush=True)

    # Extract limits and units (preferably from 30°C, otherwise use first available)
    if not td.loc[td["°C"] == "30"].empty:
        reference_row = td.loc[td["°C"] == "30"].iloc[0]
    else:
        reference_row = td.iloc[0]

    limits = {"low": reference_row["Low Limit"], "high": reference_row["High Limit"]}
    units = reference_row["Unit"]

    print(
        HEAD,
        f"Limits extracted - Low: {limits['low']}, High: {limits['high']}",
        FLUSH,
        end="\r",
        flush=True,
    )

    # Initialize clamp containers
    all_clamps_min = pd.DataFrame()
    all_clamps_max = pd.DataFrame()
    filtered_data = pd.DataFrame()

    # Process each corner/temperature combination
    total_combinations = len(td["Corner"].unique()) * len(td["°C"].unique())
    processed_combinations = 0
    total_min_clamps = 0
    total_max_clamps = 0

    print(
        HEAD,
        f"Processing {total_combinations} corner/temperature combinations",
        FLUSH,
        end="\r",
        flush=True,
    )

    for corner in td["Corner"].unique():
        for temp in td["°C"].unique():
            subset = td.loc[(td["°C"] == temp) & (td["Corner"] == corner)].copy()
            processed_combinations += 1

            if subset.empty:
                continue

            # Detect and remove clamps using improved algorithm
            clamps_min, clamps_max, clean_subset = detect_clamps(
                subset, limits, std_multiplier=0.9, adjustment_percent=0.01
            )

            # Debug clamp detection
            if not clamps_min.empty:
                total_min_clamps += len(clamps_min)
            if not clamps_max.empty:
                total_max_clamps += len(clamps_max)

            # Collect clamps
            if not clamps_min.empty:
                all_clamps_min = pd.concat(
                    [all_clamps_min, clamps_min], ignore_index=True
                )
            if not clamps_max.empty:
                all_clamps_max = pd.concat(
                    [all_clamps_max, clamps_max], ignore_index=True
                )

            # Collect clean data (after clamp removal)
            filtered_data = pd.concat([filtered_data, clean_subset], ignore_index=True)

    print(
        HEAD,
        f"Clamp detection complete - Min: {total_min_clamps}, Max: {total_max_clamps}",
        FLUSH,
        end="\r",
        flush=True,
    )

    # Calculate pivot table with statistics ON FILTERED DATA (without clamps)
    print(
        HEAD,
        f"Calculating statistics on {len(filtered_data)} filtered data points",
        FLUSH,
        end="\r",
        flush=True,
    )

    stats = pd.pivot_table(
        filtered_data,
        values="Value",
        index=["°C", "Corner"],
        aggfunc={"Value": ["size", "min", "max", "mean", "std"]},
    ).round(3)

    # Flatten column names
    stats = stats.rename(columns={"size": "count"})

    # Add limit columns
    stats["Low Limit"] = limits["low"]
    stats["High Limit"] = limits["high"]
    stats["unit"] = units

    # Fill missing std with 0
    stats["std"] = stats["std"].fillna(0)

    print(
        HEAD,
        f"Basic statistics calculated for {len(stats)} groups",
        FLUSH,
        end="\r",
        flush=True,
    )

    # Calculate process capability metrics ON FILTERED DATA
    capability_metrics = []
    for idx, row in stats.iterrows():
        subset_data = filtered_data.loc[
            (filtered_data["°C"] == idx[0]) & (filtered_data["Corner"] == idx[1])
        ]
        metrics = calculate_process_capability(
            subset_data,
            limits,
            row["std"],
            row["mean"],
        )
        capability_metrics.append(metrics)

    capability_df = pd.DataFrame(capability_metrics, index=stats.index)
    stats = pd.concat([stats, capability_df], axis=1)

    # Calculate yield metrics ON FILTERED DATA
    yield_metrics = calculate_yield_metrics(filtered_data, limits)
    if isinstance(yield_metrics["yield"], pd.Series):
        stats["Yield"] = yield_metrics["yield"]
        stats["Fail Duts"] = yield_metrics["fail_count"]
    else:
        stats["Yield"] = yield_metrics["yield"]
        stats["Fail Duts"] = yield_metrics["fail_count"]

    # Add 3-sigma bounds ON FILTERED DATA
    std_not_zero = stats["std"] != 0

    # Initialize columns as object type to handle mixed data types
    stats["max3sigma"] = pd.NA
    stats["min3sigma"] = pd.NA

    # Calculate 3-sigma bounds for non-zero std
    stats.loc[std_not_zero, "max3sigma"] = (
        stats.loc[std_not_zero, "mean"] + 3 * stats.loc[std_not_zero, "std"]
    ).round(3)
    stats.loc[std_not_zero, "min3sigma"] = (
        stats.loc[std_not_zero, "mean"] - 3 * stats.loc[std_not_zero, "std"]
    ).round(3)

    # Set "-" for zero std cases
    stats.loc[~std_not_zero, "max3sigma"] = "-"
    stats.loc[~std_not_zero, "min3sigma"] = "-"

    # Calculate kurtosis ON FILTERED DATA
    if limits["low"] != 0 or limits["high"] != 0:
        stats["Kurtosis"] = (
            filtered_data.groupby(["°C", "Corner"])["Value"].apply(kurtosis).round(3)
        )
    else:
        # Initialize as object type to handle mixed data types
        stats["Kurtosis"] = "-"

    # Add clamp information ONLY if clamps exist
    has_min_clamps = not all_clamps_min.empty
    has_max_clamps = not all_clamps_max.empty

    print(
        HEAD,
        f"Adding clamp info - Min clamps: {has_min_clamps}, Max clamps: {has_max_clamps}",
        FLUSH,
        end="\r",
        flush=True,
    )

    if has_min_clamps:
        clamp_min_counts = (
            all_clamps_min.groupby(["°C", "Corner"])["Value"]
            .count()
            .reindex(stats.index, fill_value=0)
        )
        clamp_min_thresholds = (
            all_clamps_min.groupby(["°C", "Corner"])["clamp_threshold"]
            .mean()
            .reindex(stats.index, fill_value=0)
        )
        stats["Clamps Min Count"] = clamp_min_counts.astype(int)
        stats["Clamps Min Threshold"] = clamp_min_thresholds.round(3)
        print(
            HEAD,
            f"Min clamps added to stats - Total count: {clamp_min_counts.sum()}",
            FLUSH,
            end="\r",
            flush=True,
        )

    if has_max_clamps:
        clamp_max_counts = (
            all_clamps_max.groupby(["°C", "Corner"])["Value"]
            .count()
            .reindex(stats.index, fill_value=0)
        )
        clamp_max_thresholds = (
            all_clamps_max.groupby(["°C", "Corner"])["clamp_threshold"]
            .mean()
            .reindex(stats.index, fill_value=0)
        )
        stats["Clamps Max Count"] = clamp_max_counts.astype(int)
        stats["Clamps Max Threshold"] = clamp_max_thresholds.round(3)
        print(
            HEAD,
            f"Max clamps added to stats - Total count: {clamp_max_counts.sum()}",
            FLUSH,
            end="\r",
            flush=True,
        )

    if not has_min_clamps and not has_max_clamps:
        print(HEAD, f"No clamps detected - clean dataset", FLUSH, end="\r", flush=True)

    stats = stats.rename(
        columns={
            "max": "Max",
            "min": "Min",
            "mean": "Mean",
            "std": "Std",
            "count": "Tested Part",
        },
    )[
        [
            "Tested Part",
            "Low Limit",
            "Min",
            "Mean",
            "Max",
            "High Limit",
            "Std",
            "unit",
            "Cp",
            "Cpk",
            "Yield",
            "min3sigma",
            "max3sigma",
            "Kurtosis",
        ]
    ]

    print(
        HEAD,
        f"Test data processing completed successfully",
        FLUSH,
        end="\r",
        flush=True,
    )

    return stats, filtered_data


def get_web_content(filename):
    """
    Reads the content of the given file and returns it as a string.

    :param filename: Path to the file to read.
    :return: String content of the file.
    """
    with open(f".\\src\\web\\{filename}", "r", encoding="utf-8") as f:
        content = f.read()
    return content


def get_product_image(product):
    folder_path = os.path.join(MAIN_PATH, product, "ARTstdf")

    for filename in os.listdir(folder_path):
        if filename.lower().endswith(".svg"):
            svg_path = os.path.join(folder_path, filename)
            with open(svg_path, "r", encoding="utf-8") as f:
                content = f.read()
            return content

    return None


def gen_menu(parameter, destinationfolder):
    print(HEAD, f"Generate main menu... ", FLUSH, end="\r", flush=True)
    # Full path for the index.html file
    destinationfolder = (
        destinationfolder.split(parameter["FLOW"])[0] + parameter["FLOW"] + "\\Report"
    )
    # Ensure the destination folder exists; create it if it doesn't
    os.makedirs(destinationfolder, exist_ok=True)
    file_path = os.path.join(destinationfolder, "index.html")

    composite_list = [
        nome
        for nome in os.listdir(destinationfolder)
        if os.path.isdir(os.path.join(destinationfolder, nome))
    ]
    composite_list = sorted(composite_list)

    # Carica i dati di personalizzazione
    try:
        with open("src/jupiter/personalization.json", "r") as file:
            data = json.load(file)

        # Recupera il nome del prodotto
        product_data = data.get(parameter["CODE"], {})
        product_name = product_data.get("product_name", "")
        parameter["PRODUCT"] = product_name
    except FileNotFoundError:
        print("[WARNING] personalization.json not found, using default product name")
        parameter["PRODUCT"] = parameter.get("CODE", "")
    except Exception as e:
        print(f"[ERROR] Error reading personalization.json: {e}")
        parameter["PRODUCT"] = parameter.get("CODE", "")

    # Sample HTML content for the file
    html_content = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <title>{parameter["CUT"]} CHAR</title>
        <link rel="shortcut icon" type="image/png" href="/etc/clientlibs/st-site/media/app/images/favicon.ico">
        <style>{get_web_content("style.css")}</style>
    </head>
    <body style="margin: auto; max-width: 1200px;">
        {get_web_content("stlogo.html")}
        <h1 style="font-family:Arial; font-weight: normal; text-align:center; font-size: 4em; color:#03234B">{parameter["PRODUCT"]} {parameter["CUT"]}</h1> 
        <h1 style="font-family:Arial; font-weight: normal; text-align:center; font-size: 3em; color:#03234B">{parameter["FLOW"]} Report</h1> 
        <div encoding="UTF-8" standalone="no" style="width: 700px; margin: 0 auto; text-align: center;">
        {get_product_image(parameter["CODE"])}
        </div>      
    """

    html_content += '<div class="contentconteiner">'
    # Populate the list in HTML using composite_list items
    for item in composite_list:
        folder_path = os.path.join(destinationfolder, item)
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)
            index_file_path = os.path.join(folder_path, "index.html")
            print(
                HEAD,
                f"File created: {folder_path}",
                FLUSH,
                end="\r",
                flush=True,
            )
            try:
                with open(index_file_path, "w") as f:
                    f.write(get_web_content("404.html"))
            except Exception as e:
                print(f"Error creating file {index_file_path}: {e}")
        html_content += f'<a class="btn" href="./{item}/index.html">{item}</a>\n'
    html_content += "</div>"

    html_content += """
    </body>
    </html>
    """

    # Write the HTML content to index.html file
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(html_content)

    print(
        HEAD,
        f"File created: {file_path}",
        FLUSH,
        end="\r",
        flush=True,
    )


def gen_ptr(tname, parameter, df_stdf, path):
    file_path = os.path.join(path, parameter["COM"], f"{tname.replace(":","_")}.html")

    td = pd.DataFrame(df_stdf["ptr"][(df_stdf["ptr"]["TestName"] == tname)])

    stats, td = process_test_data(td)
    stats.rename(
        columns={
            "max": "Max",
            "min": "Min",
            "mean": "Mean",
            "std": "Std",
            "count": "Tested Part",
        },
        inplace=True,
    )

    STPalette = {
        "-40": "#03234B",
        "-10": "#3CB4E6",
        "30": "#49B170",
        "60": "#A4C238",
        "90": "#FFD200",
        "110": "#F3693F",
        "130": "#ED355F",
        "140": "#E6007E",
    }
    xwafer = [19, 152]
    ywafer = [21, 173]

    if not td.loc[td["°C"] == "30"].empty:
        ul = td.loc[td["°C"] == "30", "High Limit"].unique()[0]
        ll = td.loc[td["°C"] == "30", "Low Limit"].unique()[0]
        units = td.loc[td["°C"] == "30", "Unit"].unique()[0]
    else:
        ul = td["High Limit"].unique()[0]
        ll = td["Low Limit"].unique()[0]
        units = td["Unit"].unique()[0]

    # Get the unique values from the 'pltype' column
    pl_types = pd.unique(td['pltype'])

    # Check if there is more than one unique value and if "SPLIT" is one of them
    if len(pl_types) > 1 and "SPLIT" in pl_types:
        # Filter the DataFrame to only include rows where 'pltype' is 'SPLIT'
        td_split = td[td['pltype'] == 'SPLIT']
        fig = graph.combined_hist_heatmap_box(
            td_split,
            ll,
            ul,
            units,
            STPalette,
            xwafer,
            ywafer,
        )
    
    else:
        # fig = graph.std_hist(td, ll, ul, units, STPalette)
        # fig.show()
        # fig.write_html("grafico.html")

        fig = graph.combined_hist_heatmap_box(
            td_split,
            ll,
            ul,
            units,
            STPalette,
            xwafer,
            ywafer,
        )

    html_plot = pyo.plot(
        fig, output_type="div", include_plotlyjs=True, config={"responsive": True}
    )
    html_table = graph.generate_colored_table(stats)

    # Sample HTML content for the file
    html_content = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <title>{parameter["COM"].replace("_"," ")} {parameter["CUT"]} CHAR</title>
        <link rel="shortcut icon" type="image/png" href="/etc/clientlibs/st-site/media/app/images/favicon.ico">
        <style>{get_web_content("style.css")}</style>
    </head>
    <body>
        {get_web_content("navbar.html")}
        {html_plot} 
        {html_table} 
    """
    # <h2 style="font-family:Arial; font-weight: normal; text-align:center; font-size: 4em; color:#03234B">{tname}</h2>

    with open(file_path, "w", encoding="utf-8") as f:
        f.write(html_content)

    print(
        HEAD,
        f"File created: {file_path}",
        FLUSH,
        end="\r",
        flush=True,
    )


def gen_ftr(tname, parameter, df_stdf, path):
    file_path = os.path.join(path, parameter["COM"], f"{tname.replace(":","_")}.html")

    # Sample HTML content for the file
    html_content = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <title>{parameter["COM"].replace("_"," ")} {parameter["CUT"]} CHAR</title>
        <link rel="shortcut icon" type="image/png" href="/etc/clientlibs/st-site/media/app/images/favicon.ico">
        <style>{get_web_content("style.css")}</style>
    </head>
    <body>
        {get_web_content("navbar.html")}
        <h2 style="font-family:Arial; font-weight: normal; text-align:center; font-size: 4em; color:#03234B">{tname}</h2> 
    """

    print(HEAD, f"Generate {tname}... ", FLUSH, end="\r", flush=True)
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(html_content)

    print(
        HEAD,
        f"File created: {file_path}",
        FLUSH,
        end="\r",
        flush=True,
    )


def gen_composite(parameter, df_stdf, destinationfolder):
    print(HEAD, f"Generate Composite test list... ", FLUSH, end="\r", flush=True)
    file_path = os.path.join(destinationfolder, parameter["COM"])
    os.makedirs(file_path, exist_ok=True)
    file_path = os.path.join(destinationfolder, parameter["COM"], "index.html")

    mir = df_stdf["mir"]
    tsr = df_stdf["tsr"]

    # Sample HTML content for the file
    html_content = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <title>{parameter["COM"].replace("_"," ")} {parameter["CUT"]} CHAR</title>
        <link rel="shortcut icon" type="image/png" href="/etc/clientlibs/st-site/media/app/images/favicon.ico">
        <style>{get_web_content("style.css")}</style>
    </head>
    <body style="margin: auto; max-width: 1200px;">
        {get_web_content("progressicon.html")}
        <div class="contentconteiner" style="max-width: unset;">
        {get_web_content("stlogo.html")}
        <p>
        {get_web_content("homebutton.html")}
        </p>
        </div>
        <h1 style="font-family:Arial; font-weight: normal; text-align:center; font-size: 4em; color:#03234B">{parameter["COM"].replace("_"," ")} {parameter["FLOW"]} </h1> 
        <h1 style="font-family:Arial; font-weight: normal; text-align:center; font-size: 3em; color:#03234B">{parameter["PRODUCT"]} {parameter["CUT"]}</h1> 
        <div encoding="UTF-8" standalone="no" style="width: 700px; margin: 0 auto; text-align: center;">
        {get_product_image(parameter["CODE"])}
        </div>
        <hr>
        <table border= "1" align="center" style="height: 100%; width: 100%;">                                     
            <tr>  
                <td>FLOW</td> 
                <td>
                {parameter["FLOW"]}
                </td>
            </tr>
            <tr>
                <td>Part ID-CUT</td>
                <td>{str(mir.FAMLY_ID[0])}</td>
            </tr>
            <tr>
                <td>Test Program</td>
                <td>{str(mir.JOB_NAM[0])} version {str(mir.JOB_REV[0])}</td>
            </tr>
            <tr>
                <td>Tester Name</td>
                <td>{str(mir.NODE_NAM[0])}</td>
            </tr>
            <tr>
                <td>Division</td>
                <td>
                {parameter["GROUP"]}
                </td>
            </tr>
            <tr>
                <td>Revision</td>
                <td>
                {parameter["REVISION"]}
                </td>
            </tr>
            <tr>
                <td>Date</td> 
                <td>
                {datetime.datetime.now().strftime(" %d %B %Y %H:%M")}
                </td>
            </tr>
        </table>
    """

    test_numbers = parameter["TEST_NUM"]

    with open("src/jupiter/personalization.json", "r") as file:
        data = json.load(file)
    product_data = data.get(parameter["CODE"], {})
    STblue = product_data.get("STblue", "#000000")
    STcyan = product_data.get("STcyan", "#000000")
    STred = product_data.get("STred", "#000000")
    STyellow = product_data.get("STyellow", "#000000")
    STgreen = product_data.get("STgreen", "#000000")
    STViolet = product_data.get("STViolet", "#000000")
    STdarkgreen = product_data.get("STdarkgreen", "#000000")
    STcolors = product_data.get("STcolors", ["#000000"])
    STHBIN = product_data.get("STHBIN", ["#000000"])
    gradientcolor = product_data.get("gradientcolor", ["#000000"])
    tempSTcolort8 = product_data.get("tempSTcolort8", ["#000000"])
    tempSTcolort = product_data.get("tempSTcolort", ["#000000"])
    STPalette = product_data.get("STPalette", {})
    xwafer = product_data.get("xwafer", [0, 200])
    ywafer = product_data.get("ywafer", [0, 200])

    # PTR Parametric Test Record
    ptr = df_stdf["ptr"]

    # FTR Functional Test Record
    ftr = df_stdf["ftr"]

    def ordina_test_name(test_names):
        def sort_key(name):
            prefix, suffix = name.split(":", 1) if ":" in name else (name, "")
            if "Untrimmed" in suffix:
                return (prefix, 0)
            elif "TrimValue" in suffix:
                return (prefix, 1)
            elif "Trimmed" in suffix:
                return (prefix, 2)
            return (
                prefix,
                3,
            )  # Per tutti gli altri nomi che non contengono le parole chiave

        # Mantieni l'ordine generale dei test ma ordina all'interno dei gruppi
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
                del grouped_tests[prefix_key]  # Rimuovi il gruppo una volta aggiunto

        return sorted_tests

    # Ottieni i nomi dei test unici
    ptrtname = ptr["TestName"].unique() if not ptr.empty else []
    ftrtname = ftr["TestName"].unique() if not ftr.empty else []

    # Ordina i nomi dei test
    ptrtname = ordina_test_name(ptrtname)
    ftrtname = ordina_test_name(ftrtname)

    html_content += '<h2 id="TableOfContent">Table of Contents<a class="anchor-link" href="#TableOfContent"></a></h2><hr>'

    # Populate the list in HTML using composite_list items

    content = ""
    if len(ptrtname) != 0 and len(ftrtname) != 0:
        for tname in ptrtname:
            content = (
                content
                + "<a class='btn' href='"
                + tname.replace(":", "_")
                + ".html'> "
                + tname
                + " </a>\n"
            )
        for tname in ftrtname:
            content = (
                content
                + "<a class='btn' href='"
                + tname.replace(":", "_")
                + ".html'> "
                + tname
                + " </a>\n"
            )
    elif len(ptrtname) != 0:
        for tname in ptrtname:
            content = (
                content
                + "<a class='btn' href='"
                + tname.replace(":", "_")
                + ".html'> "
                + tname
                + " </a>\n"
            )
    elif len(ftrtname) != 0:
        for tname in ftrtname:
            content = (
                content
                + "<a class='btn' href='"
                + tname.replace(":", "_")
                + ".html'> "
                + tname
                + " </a>\n"
            )

    html_content += '<div class="contentconteiner">' + content + "</div>"

    html_content += f"""
    <script>{get_web_content("script.js")}</script>
    </body>
    </html>
    """

    # Writ
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(html_content)

    print(
        HEAD,
        f"File created: {file_path}",
        FLUSH,
        end="\r",
        flush=True,
    )
    return ptrtname, ftrtname


def main_graph(DEBUG):
    td = pd.read_csv("./src/td.csv")

    stats, td = process_test_data(td)
    print(HEAD, f"Generate graph... ", FLUSH, end="\r", flush=True)
    STPalette = {
        "-40": "#03234B",
        "-10": "#3CB4E6",
        "30": "#49B170",
        "60": "#A4C238",
        "90": "#FFD200",
        "110": "#F3693F",
        "130": "#ED355F",
        "140": "#E6007E",
    }
    xwafer = [19, 152]
    ywafer = [21, 173]
    if not td.loc[td["°C"] == "30"].empty:
        ul = td.loc[td["°C"] == "30", "High Limit"].unique()[0]
        ll = td.loc[td["°C"] == "30", "Low Limit"].unique()[0]
        units = td.loc[td["°C"] == "30", "Unit"].unique()[0]
    else:
        ul = td["High Limit"].unique()[0]
        ll = td["Low Limit"].unique()[0]
        units = td["Unit"].unique()[0]

    if pd.unique(td.pltype)[0] == "STD":
        # fig = graph.std_hist(td, ll, ul, units, STPalette)
        # fig.show()
        # fig.write_html("grafico.html")

        fig = graph.combined_hist_heatmap_box(
            td,
            ll,
            ul,
            units,
            STPalette,
            xwafer,
            ywafer,
        )
        fig.show()

    print(stats)


def main_ptr():
    parameter = {
        "TITLE": "PMU EWSCHAR char",
        "COM": "STDF",
        "FLOW": "EWSCHAR",
        "TYPE": "CHAR",
        "PRODUCT": "",
        "CODE": "44E",
        "LOT": "Q445172",
        "WAFER": "05",
        "CUT": "44EZ",
        "REVISION": "0.1",
        "FILE": {
            "05": {
                "corner": "TTTT",
                "path": ".\\STDF\\44E\\44EZ\\EWSCHAR\\Q445172_05_SSTT",
            }
        },
        "AUTHOR": "Matteo Terranova",
        "MAIL": "matteo.terranova@st.com",
        "SITE": "Catania",
        "GROUP": "MDRF - EP - GPAM",
        "TEST_NUM": "",
        "CSV": ".\\STDF\\44E\\44EZ\\EWSCHAR\\Q445172_05_TTTT",
    }
    td = pd.read_csv("./src/ptrPMU.csv")
    path = "./"
    tname = "MEAS_OBL_LDO:pa7"
    df_stdf = {}
    df_stdf["ptr"]= td

    gen_ptr(tname, parameter, df_stdf, path)


if __name__ == "__main__":
    DEBUG = True
    main_ptr()
    # main_graph(DEBUG)
