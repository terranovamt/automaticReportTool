import os
import json5
import datetime
import numpy as np
import polars as pl
import plotly.offline as pyo
from scipy.stats import kurtosis


# MAIN_PATH = "\\\\gpm-pe-data.gnb.st.com\\ENGI_MCD_STDF"
MAIN_PATH = ".\\STDF"
HEAD = "[CHAR]"

try:
    import graphv2 as graph
except ModuleNotFoundError:
    import script.graphv2 as graph


def get_personalizzation(parameter, name):
    file_path = os.path.join(
        parameter["MAIN"].split(parameter["CODE"])[0], parameter["CODE"], "ART.jsonc"
    )

    if not os.path.isfile(file_path):
        return {}

    with open(file_path, "r", encoding="utf-8") as file:
        dati = json5.load(file)

    if name in dati:
        dato = dati[name]
        return dato
    else:
        return {}


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
        return -adjustment_percent if threshold_type == "max" else adjustment_percent

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


def detect_clamps(
    subset, limits, std_multiplier=1.9, adjustment_percent=0.01, sigma_threshold=5
):
    """
    Detect clamp values using improved distance-based algorithm with 6-sigma outlier removal.

    Algorithm:
    1. Calculate 6-sigma bounds
    2. Find min and max values and calculate extreme thresholds
    3. Compare sigma bounds vs extreme thresholds and choose most stringent
    4. Filter once with most stringent thresholds
    5. Save clamps with chosen thresholds

    Args:
        subset: Polars DataFrame subset for specific corner/temperature
        limits: Dict with 'low' and 'high' limit values
        std_multiplier: Multiplier for std deviation to determine clamp distance (default 1.9)
        adjustment_percent: Percentage to adjust threshold from extreme value (default 0.01 = 1%)
        sigma_threshold: Number of standard deviations for outlier removal (default 6)

    Returns:
        Tuple of (clamps_min_df, clamps_max_df, filtered_subset)
    """
    if subset.is_empty():
        return pl.DataFrame(), pl.DataFrame(), subset

    clamps_min = pl.DataFrame()
    clamps_max = pl.DataFrame()

    # Get basic statistics
    values = subset["Value"]
    mean_val = values.mean()
    std_dev = values.std()

    # If std is 0 or very small, no clamps can be detected reliably
    if std_dev == 0 or std_dev is None or np.isnan(std_dev) or std_dev < 1e-10:
        return clamps_min, clamps_max, subset

    # Step 1: Calculate 6-sigma bounds
    lower_sigma_bound = mean_val - (sigma_threshold * std_dev)
    upper_sigma_bound = mean_val + (sigma_threshold * std_dev)

    # Get sorted unique values for distance calculation
    sorted_values = values.unique().sort()

    if len(sorted_values) < 2:
        # Not enough unique values to detect clamps, use only sigma
        clamps_min = subset.filter(pl.col("Value") < lower_sigma_bound)
        clamps_max = subset.filter(pl.col("Value") > upper_sigma_bound)

        if not clamps_min.is_empty():
            clamps_min = clamps_min.with_columns(
                pl.lit(lower_sigma_bound).alias("clamp_threshold")
            )
        if not clamps_max.is_empty():
            clamps_max = clamps_max.with_columns(
                pl.lit(upper_sigma_bound).alias("clamp_threshold")
            )

        filtered_subset = subset.filter(
            (pl.col("Value") >= lower_sigma_bound)
            & (pl.col("Value") <= upper_sigma_bound)
        )
        return clamps_min, clamps_max, filtered_subset

    min_val = sorted_values[0]
    max_val = sorted_values[-1]

    # Check if limits exist
    has_limits = limits["low"] != 0 or limits["high"] != 0

    # Initialize final thresholds with sigma bounds
    final_threshold_min = lower_sigma_bound
    final_threshold_max = upper_sigma_bound
    threshold_type_min = "sigma"
    threshold_type_max = "sigma"

    # Step 2: Check for extreme clamps and compare with sigma

    # Check minimum clamps
    if len(sorted_values) >= 2:
        second_min = sorted_values[1]
        distance_min = abs(second_min - min_val)
        is_min_clamp = distance_min > (std_dev * std_multiplier)

        should_remove_min_clamp = False
        if is_min_clamp:
            if not has_limits:
                should_remove_min_clamp = True
            else:
                should_remove_min_clamp = limits["low"] != 0 and min_val < limits["low"]

        if should_remove_min_clamp:
            threshold_min_extreme = calculate_clamp_threshold(
                min_val, "min", adjustment_percent
            )
            # Choose most stringent (highest value for minimum threshold)
            if threshold_min_extreme > final_threshold_min:
                final_threshold_min = threshold_min_extreme
                threshold_type_min = "extreme"

    # Check maximum clamps
    if len(sorted_values) >= 2:
        second_max = sorted_values[-2]
        distance_max = abs(max_val - second_max)
        is_max_clamp = distance_max > (std_dev * std_multiplier)

        should_remove_max_clamp = False
        if is_max_clamp:
            if not has_limits:
                should_remove_max_clamp = True
            else:
                should_remove_max_clamp = (
                    limits["high"] != 0 and max_val > limits["high"]
                )

        if should_remove_max_clamp:
            threshold_max_extreme = calculate_clamp_threshold(
                max_val, "max", adjustment_percent
            )
            # Choose most stringent (lowest value for maximum threshold)
            if threshold_max_extreme < final_threshold_max:
                final_threshold_max = threshold_max_extreme
                threshold_type_max = "extreme"

    # Step 3: Filter once with final thresholds and save clamps
    # Filter subset with final thresholds
    filtered_subset = subset.filter(
        (pl.col("Value") > final_threshold_min)
        & (pl.col("Value") < final_threshold_max)
    )
    if not filtered_subset.is_empty():
        # Save minimum clamps
        potential_clamps_min = subset.filter(pl.col("Value") <= final_threshold_min)
        if not potential_clamps_min.is_empty():
            clamps_min = potential_clamps_min.with_columns(
                pl.lit(final_threshold_min).alias("clamp_threshold")
            )
            print(
                HEAD,
                f"Min clamp ({threshold_type_min}): {len(clamps_min)} values <= {final_threshold_min:.3f}".ljust(
                    150
                ),
                end="\r",
                flush=True,
            )

        # Save maximum clamps
        potential_clamps_max = subset.filter(pl.col("Value") >= final_threshold_max)
        if not potential_clamps_max.is_empty():
            clamps_max = potential_clamps_max.with_columns(
                pl.lit(final_threshold_max).alias("clamp_threshold")
            )
            print(
                HEAD,
                f"Max clamp ({threshold_type_max}): {len(clamps_max)} values >= {final_threshold_max:.3f}".ljust(
                    150
                ),
                end="\r",
                flush=True,
            )

    else:
        filtered_subset = subset.filter(
            (pl.col("Value") > lower_sigma_bound)
            & (pl.col("Value") < upper_sigma_bound)
        )
        # Save minimum clamps
        potential_clamps_min = subset.filter(pl.col("Value") <= lower_sigma_bound)
        if not potential_clamps_min.is_empty():
            clamps_min = potential_clamps_min.with_columns(
                pl.lit(lower_sigma_bound).alias("clamp_threshold")
            )
            print(
                HEAD,
                f"Min clamp ({threshold_type_min}): {len(clamps_min)} values <= {lower_sigma_bound:.3f}".ljust(
                    150
                ),
                end="\r",
                flush=True,
            )

        # Save maximum clamps
        potential_clamps_max = subset.filter(pl.col("Value") >= upper_sigma_bound)
        if not potential_clamps_max.is_empty():
            clamps_max = potential_clamps_max.with_columns(
                pl.lit(upper_sigma_bound).alias("clamp_threshold")
            )
            print(
                HEAD,
                f"Max clamp ({threshold_type_max}): {len(clamps_max)} values >= {upper_sigma_bound:.3f}".ljust(
                    150
                ),
                end="\r",
                flush=True,
            )

    return clamps_min, clamps_max, filtered_subset


def calculate_process_capability(data, limits, std_val, mean_val):
    """
    Calculate Cp and Cpk process capability indices.

    Args:
        data: Polars DataFrame with values
        limits: Dict with 'low' and 'high' limits
        std_val: Standard deviation
        mean_val: Mean value

    Returns:
        Dict with Cp, Cpk values or "-" if no limits
    """
    if limits["low"] == 0 and limits["high"] == 0:
        return {"Cp": 0.0, "Cpk": 0.0}

    if std_val == 0:
        return {"Cp": 0.0, "Cpk": 0.0}

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
        data: Polars DataFrame with values grouped by temperature and corner
        limits: Dict with 'low' and 'high' limits

    Returns:
        Dict with yield metrics or "-" if no limits
    """
    if limits["low"] == 0 and limits["high"] == 0:
        return 100.0

    data_with_limits = data.with_columns(
        [
            (
                (pl.col("Value") >= limits["low"]) & (pl.col("Value") <= limits["high"])
            ).alias("is_within_limits")
        ]
    )

    yield_data = (
        data_with_limits.group_by(["°C", "Corner"])
        .agg(
            [
                pl.col("is_within_limits").sum().alias("within_limits"),
                pl.col("is_within_limits").count().alias("total"),
            ]
        )
        .with_columns(
            [
                (pl.col("within_limits") / pl.col("total") * 100)
                .round(3)
                .alias("yield_pct"),
                (pl.col("total") - pl.col("within_limits")).alias("fail_count"),
            ]
        )
    )

    return yield_data["yield_pct"][0]


def process_ptr(td):
    """
    Main function to process test data and calculate statistics with improved clamp detection.

    Args:
        td: Input Polars DataFrame with test data

    Returns:
        Polars DataFrame with statistical summary and conditional clamp information
    """
    print(HEAD, f"Starting test data processing...".ljust(150), end="\r", flush=True)

    if td.is_empty():
        print(HEAD, f"Empty dataframe received".ljust(150), end="\r", flush=True)
        return pl.DataFrame(), pl.DataFrame()

    # Data preparation
    td = td.clone()
    td = td.with_columns([pl.col("°C").cast(pl.String), pl.col("Unit").fill_null("")])

    # # Verifica e filtra condizionalmente
    # unique_splits = set(td["Split"].unique().to_list())
    # if len(unique_splits) > 1:
    #     td = td.filter(pl.col("Split") != "Standard")

    print(HEAD, f"Data prepared - {len(td)} rows".ljust(150), end="\r", flush=True)

    temp_30_data = td.filter(pl.col("°C") == "30")
    if temp_30_data.height > 0:
        ul = temp_30_data.select(pl.col("High Limit")).to_series().mode()[0]
        ll = temp_30_data.select(pl.col("Low Limit")).to_series().mode()[0]
        units = temp_30_data.select(pl.col("Unit")).to_series().mode()[0]
    else:
        ul = td.select(pl.col("High Limit")).to_series().mode()[0]
        ll = td.select(pl.col("Low Limit")).to_series().mode()[0]
        units = td.select(pl.col("Unit")).to_series().mode()[0]

    limits = {"low": ll, "high": ul}

    print(
        HEAD,
        f"Limits extracted - Low: {limits['low']}, High: {limits['high']}".ljust(150),
        end="\r",
        flush=True,
    )

    # Initialize clamp containers
    all_clamps_min = pl.DataFrame()
    all_clamps_max = pl.DataFrame()
    filtered_data = pl.DataFrame()

    # Process each corner/temperature combination
    total_combinations = len(td["Corner"].unique()) * len(td["°C"].unique())
    processed_combinations = 0
    total_min_clamps = 0
    total_max_clamps = 0

    print(
        HEAD,
        f"Processing {total_combinations} corner/temperature combinations".ljust(150),
        end="\r",
        flush=True,
    )

    # Check if data is binary (FTR case)
    unique_values = td["Value"].unique().to_list()
    if all(val in [0, 1] for val in unique_values):
        if not all(val == 0 for val in unique_values) and not all(
            val == 1 for val in unique_values
        ):
            # USE AS FTR
            return {}, pl.DataFrame(), True

    for corner in td["Corner"].unique():
        for temp in td["°C"].unique():
            subset = td.filter((pl.col("°C") == temp) & (pl.col("Corner") == corner))
            processed_combinations += 1
            print(
                HEAD,
                f"Processing {processed_combinations}/{total_combinations} {corner} {temp} combinations".ljust(
                    150
                ),
                end="\r",
                flush=True,
            )
            if subset.is_empty():
                continue

            # Detect and remove clamps using improved algorithm
            clamps_min, clamps_max, clean_subset = detect_clamps(subset, limits)

            # Debug clamp detection
            if not clamps_min.is_empty():
                total_min_clamps += len(clamps_min)
            if not clamps_max.is_empty():
                total_max_clamps += len(clamps_max)

            # Collect clamps
            if not clamps_min.is_empty():
                if all_clamps_min.is_empty():
                    all_clamps_min = clamps_min
                else:
                    all_clamps_min = pl.concat([all_clamps_min, clamps_min])
            if not clamps_max.is_empty():
                if all_clamps_max.is_empty():
                    all_clamps_max = clamps_max
                else:
                    all_clamps_max = pl.concat([all_clamps_max, clamps_max])

            # Collect clean data (after clamp removal)
            if filtered_data.is_empty():
                filtered_data = clean_subset
            else:
                filtered_data = pl.concat([filtered_data, clean_subset])

    print(
        HEAD,
        f"Clamp detection complete - Min: {total_min_clamps}, Max: {total_max_clamps}".ljust(
            150
        ),
        end="\r",
        flush=True,
    )

    # Calculate pivot table with statistics ON FILTERED DATA (without clamps)
    print(
        HEAD,
        f"Calculating statistics on {len(filtered_data)} filtered data points".ljust(
            150
        ),
        end="\r",
        flush=True,
    )

    # Sort before pivot
    filtered_data = filtered_data.with_columns(
        pl.col("°C").cast(pl.Int32, strict=False)
    ).sort(["°C", "Corner", "Split"])

    stats = (
        filtered_data.group_by(["°C", "Corner", "Split"])
        .agg(
            [
                pl.col("Value").count().alias("count"),
                pl.col("Value").min().alias("min"),
                pl.col("Value").max().alias("max"),
                pl.col("Value").mean().alias("mean"),
                pl.col("Value").std().alias("std"),
            ]
        )
        .with_columns(
            [
                pl.lit(limits["low"]).alias("Low Limit"),
                pl.lit(limits["high"]).alias("High Limit"),
                pl.lit(units).alias("unit"),
                pl.col("std").fill_null(0),
            ]
        )
    )

    print(
        HEAD,
        f"Basic statistics calculated for {len(stats)} groups".ljust(150),
        end="\r",
        flush=True,
    )

    # Calculate process capability metrics ON FILTERED DATA
    capability_data = []
    if stats.is_empty():
        return

    for row in stats.iter_rows(named=True):
        subset_data = filtered_data.filter(
            (pl.col("°C") == row["°C"]) & (pl.col("Corner") == row["Corner"])
        )
        metrics = calculate_process_capability(
            subset_data,
            limits,
            row["std"],
            row["mean"],
        )
        metrics["Yield"] = calculate_yield_metrics(subset_data, limits)
        capability_data.append(metrics)

    capability_df = pl.DataFrame(capability_data, infer_schema_length=None)
    stats = stats.with_columns(
        [capability_df["Cp"], capability_df["Cpk"], capability_df["Yield"]]
    )

    # Calculate yield metrics ON FILTERED DATA
    # yield_metrics = calculate_yield_metrics(filtered_data, limits)
    # if hasattr(yield_metrics["yield"], "to_list"):
    #     stats = stats.with_columns(
    #         [
    #             pl.Series("Yield", yield_metrics["yield"].to_list()),
    #             pl.Series("Fail Duts", yield_metrics["fail_count"].to_list()),
    #         ]
    #     )
    # else:
    #     stats = stats.with_columns(
    #         [
    #             pl.lit(yield_metrics["yield"]).alias("Yield"),
    #             pl.lit(yield_metrics["fail_count"]).alias("Fail Duts"),
    #         ]
    #     )

    # Add 3-sigma bounds ON FILTERED DATA
    stats = stats.with_columns(
        [
            pl.when(pl.col("std") != 0)
            .then((pl.col("mean") + 3 * pl.col("std")).round(3))
            .otherwise(pl.lit("-"))
            .alias("max3sigma"),
            pl.when(pl.col("std") != 0)
            .then((pl.col("mean") - 3 * pl.col("std")).round(3))
            .otherwise(pl.lit("-"))
            .alias("min3sigma"),
        ]
    )

    # Calculate kurtosis ON FILTERED DATA
    if limits["low"] != 0 or limits["high"] != 0:
        kurtosis_data = []
        for temp in filtered_data["°C"].unique():
            for corner in filtered_data["Corner"].unique():
                subset = filtered_data.filter(
                    (pl.col("°C") == temp) & (pl.col("Corner") == corner)
                )
                if not subset.is_empty():
                    std = subset["Value"].std()
                    if std == 0 or std is None or np.isnan(std) or std < 1e-10:
                        kurt_val = 0  # Correzione: mancava "= 0"
                    else:
                        kurt_val = kurtosis(subset["Value"].to_numpy())

                    # Aggiungi sempre alla lista (sia che sia 0 o calcolato)
                    kurtosis_data.append(
                        {"°C": temp, "Corner": corner, "Kurtosis": round(kurt_val, 3)}
                    )

        kurtosis_df = pl.DataFrame(kurtosis_data)
        stats = stats.join(kurtosis_df, on=["°C", "Corner"], how="left")
    else:
        stats = stats.with_columns(pl.lit("-").alias("Kurtosis"))

    # Initialize clamp columns
    stats = stats.with_columns(
        [
            pl.lit(0).alias("Clamps Min Count"),
            pl.lit(0.0).alias("Clamps Min Threshold"),
            pl.lit(0).alias("Clamps Max Count"),
            pl.lit(0.0).alias("Clamps Max Threshold"),
        ]
    )

    # Add clamp information ONLY if clamps exist
    if total_min_clamps > 0:
        print(
            HEAD,
            f"Processing {total_min_clamps} min clamps".ljust(150),
            end="\r",
            flush=True,
        )

        # Convert clamp data °C to numeric to match stats
        all_clamps_min = all_clamps_min.with_columns(
            pl.col("°C").cast(pl.Int32, strict=False)
        )

        # Group clamps by temperature and corner
        clamp_min_grouped = all_clamps_min.group_by(["°C", "Corner"]).agg(
            [
                pl.col("Value").count().alias("clamp_count"),
                pl.col("clamp_threshold").min().alias("min_threshold"),
            ]
        )

        # Update stats with clamp information
        stats = stats.join(clamp_min_grouped, on=["°C", "Corner"], how="left")
        stats = stats.with_columns(
            [
                pl.col("clamp_count").fill_null(0).alias("Clamps Min Count"),
                pl.col("min_threshold")
                .fill_null(0.0)
                .round(3)
                .alias("Clamps Min Threshold"),
            ]
        ).drop(["clamp_count", "min_threshold"])

    # Process MAX clamps
    if total_max_clamps > 0:
        print(
            HEAD,
            f"Processing {total_max_clamps} max clamps".ljust(150),
            end="\r",
            flush=True,
        )

        # Convert clamp data °C to numeric to match stats
        all_clamps_max = all_clamps_max.with_columns(
            pl.col("°C").cast(pl.Int32, strict=False)
        )

        # Group clamps by temperature and corner
        clamp_max_grouped = all_clamps_max.group_by(["°C", "Corner"]).agg(
            [
                pl.col("Value").count().alias("clamp_count"),
                pl.col("clamp_threshold").max().alias("max_threshold"),
            ]
        )

        # Update stats with clamp information
        stats = stats.join(clamp_max_grouped, on=["°C", "Corner"], how="left")
        stats = stats.with_columns(
            [
                pl.col("clamp_count").fill_null(0).alias("Clamps Max Count"),
                pl.col("max_threshold")
                .fill_null(0.0)
                .round(3)
                .alias("Clamps Max Threshold"),
            ]
        ).drop(["clamp_count", "max_threshold"])

    # Rename columns
    stats = stats.rename(
        {
            "max": "Max",
            "min": "Min",
            "mean": "Mean",
            "std": "Std",
            "count": "Tested Part",
        }
    )

    # Define base columns
    base_columns = [
        "°C",
        "Corner",
        "Split",
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

    # Determine which clamp columns to include
    final_columns = base_columns.copy()

    # Check if min clamp columns should be included (not all zeros)
    if (stats["Clamps Min Count"] > 0).any():
        final_columns.extend(["Clamps Min Count", "Clamps Min Threshold"])

    # Check if max clamp columns should be included (not all zeros)
    if (stats["Clamps Max Count"] > 0).any():
        final_columns.extend(["Clamps Max Count", "Clamps Max Threshold"])

    # Select only existing columns
    existing_columns = [col for col in final_columns if col in stats.columns]
    stats = stats.select(existing_columns)
    filtered_data = filtered_data.with_columns(pl.col("°C").cast(pl.Utf8))
    print(
        HEAD,
        f"Test data processing completed successfully".ljust(150),
        end="\r",
        flush=True,
    )

    return stats, filtered_data, False


def calculate_yield(x):
    """Calcola la resa in percentuale con formato ottimizzato."""
    pass_count = (x == 1).sum()
    total = len(x)
    return f"{pass_count / total * 100:.2f} %" if total > 0 else "0.00 %"


def get_metric_order():
    """Definisce l'ordine delle metriche per il sorting."""
    return {"PASS": 1, "FAIL": 2, "Gross": 3, "Yield": 4}


def process_ftr(td):
    """
    Main function to process test data and calculate statistics with improved clamp detection.

    Args:
        td: Input Polars DataFrame with test data

    Returns:
        Polars DataFrame with statistical summary and conditional clamp information
    """
    print(HEAD, f"Starting test data processing...".ljust(150), end="\r", flush=True)

    if td.is_empty():
        print(HEAD, f"Empty dataframe received".ljust(150), end="\r", flush=True)
        return pl.DataFrame(), pl.DataFrame()

    # Data preparation
    td = td.clone()
    td = td.with_columns(pl.col("°C").cast(pl.String))

    print(HEAD, f"Data prepared - {len(td)} rows".ljust(150), end="\r", flush=True)

    # Calculate pivot table with statistics ON FILTERED DATA (without clamps)
    print(
        HEAD,
        f"Calculating statistics on {len(td)} filtered data points".ljust(150),
        end="\r",
        flush=True,
    )

    # Aggregazione ottimizzata
    pv = td.group_by(["Corner", "°C", "Split"]).agg(
        [
            (pl.col("RESULT") == 1).sum().alias("PASS"),
            pl.col("RESULT").count().alias("Gross"),
            ((pl.col("RESULT") == 1).sum() / pl.col("RESULT").count() * 100)
            .round(2)
            .alias("Yield")
            .cast(pl.String)
            + (pl.lit("%")),
        ]
    )

    # Melt per ottenere il formato desiderato
    metrics = ["PASS", "Gross", "Yield"]
    id_columns = ["Corner", "°C", "Split"]

    pv_melted = pv.unpivot(
        index=id_columns, on=metrics, variable_name="Metric", value_name="Value"
    )

    # Conversione e sort
    pv_melted = pv_melted.with_columns(pl.col("°C").cast(pl.Int32, strict=False)).sort(
        ["°C", "Corner", "Metric"]
    )

    # Pivot con Split come colonne
    pv_pivot = pv_melted.pivot(
        values="Value", index=["°C", "Corner", "Metric"], on="Split"
    )

    # Sorting ottimizzato
    metric_order = get_metric_order()
    pv_pivot = pv_pivot.with_columns(
        pl.col("Metric")
        .map_elements(lambda x: metric_order.get(x, 5), return_dtype=pl.Int32)
        .alias("Sort_Code")
    )

    # Final result
    stats = pv_pivot.sort(["°C", "Corner", "Sort_Code"]).drop("Sort_Code")

    print(
        HEAD,
        f"Test data processing completed successfully".ljust(150),
        end="\r",
        flush=True,
    )

    return stats, td


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
    folder_path = os.path.join(MAIN_PATH, product)

    for filename in os.listdir(folder_path):
        if filename.lower().endswith(".svg"):
            svg_path = os.path.join(folder_path, filename)
            with open(svg_path, "r", encoding="utf-8") as f:
                content = f.read()
            return content

    return None


def gen_menu(parameter, destinationfolder):
    print(HEAD, f"Generate main menu... ".ljust(150), end="\r", flush=True)
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

    # # Carica i dati di personalizzazione
    # try:
    #     with open("src/jupiter/personalization.json", "r") as file:
    #         data = json5.load(file)

    #     # Recupera il nome del prodotto
    #     product_data = data.get(parameter["CODE"], {})
    #     product_name = product_data.get("product_name", "")
    #     parameter["PRODUCT"] = product_name
    # except FileNotFoundError:
    #     print("[WARNING] personalization.json not found, using default product name")
    #     parameter["PRODUCT"] = parameter.get("CODE", "")
    # except Exception as e:
    #     print(f"[ERROR] Error reading personalization.json: {e}")
    #     parameter["PRODUCT"] = parameter.get("CODE", "")

    # Sample HTML content for the file
    html_content = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <title>{parameter["CUT"]} CHAR</title>
        <link rel="shortcut icon" type="image/png" href="https://www.st.com/etc/clientlibs/st-site/media/app/images/favicon.ico">
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
                f"File created: {folder_path}".ljust(150),
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
        f"File created: {file_path}".ljust(150),
        end="\r",
        flush=True,
    )



def get_limit_data(key, value):
    try :
        value = value.select([col for col in value.columns if "clamp" not in col.lower()])

        # Prendi i valori unici dalla colonna "°C" e convertili in interi
        temp_values = value.select("°C").unique().to_series().to_list()
        temp_values_int = [int(temp) for temp in temp_values if temp is not None]

        # Trova temperatura minima e massima
        temp_min = min(temp_values_int)
        temp_max = max(temp_values_int)
        temp_mid = 30

        # Definisci le temperature target
        target_temps = [temp_min, temp_mid, temp_max]
    except Exception:
            print("[ERROR]")

    def determine_scaling(values, unit):
        """Determina il fattore di scaling e l'unità appropriata basandosi sui valori"""
        # Trova il valore massimo assoluto per determinare la scala
        max_abs = max(abs(val) for val in values if val is not None)

        if max_abs >= 1e12:
            return (
                1e-12,
                f"T{unit}",
            )  # Tera (10^12)
        elif max_abs >= 1e9:
            return 1e-9, f"G{unit}"  # Giga (10^9)
        elif max_abs >= 1e6:
            return 1e-6, f"M{unit}"  # Mega (10^6)
        elif max_abs >= 1e3:
            return 1e-3, f"k{unit}"  # Kilo (10^3)
        elif max_abs >= 1:
            return 1, unit  # unità base
        elif max_abs >= 1e-3:
            return 1e3, f"m{unit}"  # milli (10^-3)
        elif max_abs >= 1e-6:
            return 1e6, f"µ{unit}"  # micro (10^-6)
        elif max_abs >= 1e-9:
            return 1e9, f"n{unit}"  # nano (10^-9)
        else:
            return 1e12, f"p{unit}"  # pico (10^-12)

    grouped = (
        value.filter(
            (pl.col("°C").is_in(target_temps)) & 
            (pl.col("Split") == "Standard")
        )
        .group_by("°C")
        .agg(
            [
                pl.col("Mean").mean().alias("Average"),
                pl.col("Min").min().alias("Min_val"),
                pl.col("Max").max().alias("Max_val"),
                pl.col("Std").max().alias("Std_max"),
                pl.col("Low Limit").first().alias("Low_Limit"),
                pl.col("High Limit").first().alias("High_Limit"),
                pl.col("unit").first().alias("unit"),
            ]
        )
        .with_columns(pl.col("°C").cast(pl.Utf8))
    )
    if not len(grouped):
        grouped = (
            value.filter(
                (pl.col("°C").is_in(target_temps)) & 
                (pl.col("Split") == "3v3")
            )
            .group_by("°C")
            .agg(
                [
                    pl.col("Mean").mean().alias("Average"),
                    pl.col("Min").min().alias("Min_val"),
                    pl.col("Max").max().alias("Max_val"),
                    pl.col("Std").max().alias("Std_max"),
                    pl.col("Low Limit").first().alias("Low_Limit"),
                    pl.col("High Limit").first().alias("High_Limit"),
                    pl.col("unit").first().alias("unit"),
                ]
            )
            .with_columns(pl.col("°C").cast(pl.Utf8))
        )

    # Raccogli tutti i valori per determinare il scaling
    all_values = []
    for row in grouped.iter_rows(named=True):
        all_values.extend([row["Average"], row["Min_val"], row["Max_val"]])

    unit = grouped["unit"].first()

    # Determina il fattore di scaling e l'unità
    if not len(all_values): 
        return []
    scale_factor, unit = determine_scaling(all_values, unit)

    # print(f"Scaling applicato: {scale_factor}x, Unità: {unit}")
    # print(f"Temperature elaborate: {target_temps}")

    results = []

    for row in grouped.iter_rows(named=True):
        temp = row["°C"]
        average = row["Average"]
        min_val = row["Min_val"]
        max_val = row["Max_val"]
        std_max = row["Std_max"]
        low_limit = row["Low_Limit"]
        high_limit = row["High_Limit"]
        
        # Applica il scaling
        average_scaled = average * scale_factor
        min_val_scaled = min_val * scale_factor
        max_val_scaled = max_val * scale_factor
        std_max_scaled = std_max * scale_factor
        low_limit_scaled = low_limit * scale_factor
        high_limit_scaled = high_limit * scale_factor
        
        # Controllo iterativo per assicurarsi che Cp e Cpk > 1.67
        actual_cpk = 1.67
        target_cpk = 1.67
        max_iterations = 100  # Limite per evitare loop infiniti
        iteration = 0
        
        # Valori iniziali per i limiti
        ll_new = low_limit_scaled
        hl_new = high_limit_scaled
        
        while iteration < max_iterations:
            # Calcola limiti per Cpk > target_cpk
            required_distance = target_cpk * 3 * std_max_scaled
            ll_calc = average_scaled - required_distance
            hl_calc = average_scaled + required_distance
            
            # Aggiorna i limiti
            ll_new = round(ll_calc, 2)
            hl_new = round(hl_calc, 2)
            
            # Calcola Cp e Cpk finali (usando valori scalati)
            if std_max_scaled > 0:
                cp = (hl_new - ll_new) / (6 * std_max_scaled)
                cpk_lower = (average_scaled - ll_new) / (3 * std_max_scaled)
                cpk_upper = (hl_new - average_scaled) / (3 * std_max_scaled)
                cpk = min(cpk_lower, cpk_upper)
            else:
                cp = float("inf")
                cpk = float("inf")
            
            # Controlla se i valori sono sufficienti
            if cp > target_cpk and cpk > target_cpk:
                break
            
            # Se non sono sufficienti, aumenta il target per la prossima iterazione
            # Questo amplia progressivamente i limiti
            actual_cpk += 0.01  # Incremento piccolo per affinamento
            iteration += 1
        
        # Se non è riuscito a convergere, usa i limiti calcolati con il target originale
        if iteration >= max_iterations:
            required_distance = 1.67 * 3 * std_max_scaled
            ll_new = round(average_scaled - required_distance, 2)
            hl_new = round(average_scaled + required_distance, 2)
            
            # Ricalcola Cp e Cpk finali
            if std_max_scaled > 0:
                cp = (hl_new - ll_new) / (6 * std_max_scaled)
                cpk_lower = (average_scaled - ll_new) / (3 * std_max_scaled)
                cpk_upper = (hl_new - average_scaled) / (3 * std_max_scaled)
                cpk = min(cpk_lower, cpk_upper)
            else:
                cp = float("inf")
                cpk = float("inf")
        
        results.append(
            {
                "°C": temp,
                "Low Limit": round(low_limit_scaled, 3),
                "LL new": ll_new,
                "Average": round(average_scaled, 3),
                "Std": round(std_max_scaled, 6),
                "HL new": hl_new,
                "High Limit": round(high_limit_scaled, 3),
                "unit": unit,
                "Cp": round(cp, 2),
                "Cpk": round(cpk, 2)
            }
        )

    # Ordina i risultati per temperatura
    results.sort(key=lambda x: x["°C"])

    # Crea tabella finale
    results_df = pl.DataFrame(results)

    # Stampa la tabella per verifica
    # print(results_df)

    return results_df


def gen_limits(stats, parameter, report_path):
    file_path = os.path.join(report_path, parameter["COM"], "limits.html")
    html_main = ""

    STPaletteChar = get_personalizzation(parameter, "STPaletteChar")

    for key, value in stats.items():
        
        print(
            HEAD,
            f"Generate limits {key}".ljust(150),
            end="\r",
            flush=True,
        )
        if not len(value):
            continue
        limits_data = get_limit_data(key, value)
        if len(limits_data): 
            fig = graph.generate_limits(key, limits_data, STPaletteChar)
            html_plot = pyo.plot(
                fig, output_type="div", include_plotlyjs=True, config={"responsive": True}
            )
            html_main += html_plot
            html_table = graph.generate_colored_limittable_html(df =limits_data, tname=key)
            html_main += html_table
            
        if html_main == "":
            return

    # Sample HTML content for the file
    html_content = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <title>{parameter["COM"].replace("_"," ")} {parameter["CUT"]} CHAR</title>
        <link rel="shortcut icon" type="image/png" href="https://www.st.com/etc/clientlibs/st-site/media/app/images/favicon.ico">
        <style>{get_web_content("style.css")}</style>
    </head>
    <body>
    <main>
        {get_web_content("navbar.html")}
        {html_main}
        {get_web_content("gotop.html")} 
    </main>
        {get_web_content("footer.html")} 
    <script>
    </script>
    """
    print(
        HEAD,
        f"Write html".ljust(150),
        end="\r",
        flush=True,
    )
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(html_content)

    print(
        HEAD,
        f"File created: {file_path}".ljust(150),
        end="\r",
        flush=True,
    )


def gen_ptr(tname, parameter, df_stdf, report_path):
    file_path = os.path.join(
        report_path, parameter["COM"], f"{tname.replace(":","_")}.html"
    )

    td = df_stdf["ptr"].filter(pl.col("TestName") == tname)

    if td.is_empty():
        return pl.DataFrame()

    stats, td, ftrflag = process_ptr(td)

    if ftrflag:
        df_stdf["ftr"] = df_stdf["ptr"].filter(pl.col("TestName") == tname)
        df_stdf["ftr"] = df_stdf["ftr"].with_columns(
            pl.col("PARM_FLG")
            .map_elements(lambda x: 1 if x == 192 else 0, return_dtype=pl.Int32)
            .alias("RESULT")
        )
        gen_ftr(tname, parameter, df_stdf, report_path)
        return pl.DataFrame()
    
    # td.write_csv(f"{tname.replace(":","_")}.csv")
    # return
    
    print(
        HEAD,
        f"Generate graph".ljust(150),
        end="\r",
        flush=True,
    )

    STPaletteChar = get_personalizzation(parameter, "STPaletteChar")
    xwafer = get_personalizzation(parameter, "xwafer")
    ywafer = get_personalizzation(parameter, "ywafer")

    temp_30_data = td.filter(pl.col("°C") == "30")
    if temp_30_data.height > 0:
        ul = temp_30_data.select(pl.col("High Limit")).to_series().mode()[0]
        ll = temp_30_data.select(pl.col("Low Limit")).to_series().mode()[0]
        units = temp_30_data.select(pl.col("Unit")).to_series().mode()[0]
    else:
        ul = td.select(pl.col("High Limit")).to_series().mode()[0]
        ll = td.select(pl.col("Low Limit")).to_series().mode()[0]
        units = td.select(pl.col("Unit")).to_series().mode()[0]

    # Get the unique values from the 'pltype' column
    pl_types = td.select(pl.col("pltype")).unique().to_series().to_list()

    # Check if there is more than one unique value and if "SPLIT" is one of them
    if len(pl_types) > 1 and "SPLIT" in pl_types:
        # Filter the DataFrame to only include rows where 'pltype' is 'SPLIT'
        td_split = td.filter(pl.col("pltype") == "SPLIT")
        fig = graph.boxploth(
            td_split,
            ll,
            ul,
            units,
            STPaletteChar,
        )

    else:
        fig = graph.boxploth(
            td,
            ll,
            ul,
            units,
            STPaletteChar,
        )

    print(
        HEAD,
        f"Generate html".ljust(150),
        end="\r",
        flush=True,
    )

    html_plot = pyo.plot(
        fig, output_type="div", include_plotlyjs=True, config={"responsive": True}
    )
    html_table = graph.generate_colored_ptrtable_html(stats)

    # Sample HTML content for the file
    html_content = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <title>{parameter["COM"].replace("_"," ")} {parameter["CUT"]} CHAR</title>
        <link rel="shortcut icon" type="image/png" href="https://www.st.com/etc/clientlibs/st-site/media/app/images/favicon.ico">
        <style>{get_web_content("style.css")}</style>
    </head>
    <body>
    <main>
        {get_web_content("navbar.html")}
        {html_plot} 
        {html_table} 
        {get_web_content("gotop.html")} 
    </main>
        {get_web_content("footer.html")} 
    <script>
    </script>
    """
    print(
        HEAD,
        f"Write html".ljust(150),
        end="\r",
        flush=True,
    )
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(html_content)

    print(
        HEAD,
        f"File created: {file_path}".ljust(150),
        end="\r",
        flush=True,
    )
    return stats


def gen_ftr(tname, parameter, df_stdf, report_path):
    file_path = os.path.join(
        report_path, parameter["COM"], f"{tname.replace(':','_')}.html"
    )
    td = df_stdf["ftr"].filter(pl.col("TestName") == tname)

    if td.is_empty():
        return

    stats, td = process_ftr(td)
    print(
        HEAD,
        f"Generat graph".ljust(150),
        end="\r",
        flush=True,
    )

    STPaletteChar = {
        "-40": "#03234B",
        "-10": "#3CB4E6",
        "30": "#49B170",
        "60": "#A4C238",
        "90": "#FFD200",
        "110": "#FBAB18",
        "130": "#F3693F",
        "140": "#E6007E",
    }
    xwafer = [19, 152]
    ywafer = [21, 173]

    STPaletteChar = get_personalizzation(parameter, "STPaletteChar")
    xwafer = get_personalizzation(parameter, "xwafer")
    ywafer = get_personalizzation(parameter, "ywafer")

    # Get the unique values from the 'pltype' column
    pl_types = td.select(pl.col("pltype")).unique().to_series().to_list()

    # Check if there is more than one unique value and if "SPLIT" is one of them
    if len(pl_types) > 1 and "SPLIT" in pl_types:
        # Filter the DataFrame to only include rows where 'pltype' is 'SPLIT'
        td_split = td.filter(pl.col("pltype") == "SPLIT")
        fig = graph.scatter(
            td_split,
            STPaletteChar,
        )

    else:
        fig = graph.scatter(
            td,
            STPaletteChar,
        )

    print(
        HEAD,
        f"Generate html".ljust(150),
        end="\r",
        flush=True,
    )

    html_plot = pyo.plot(
        fig, output_type="div", include_plotlyjs=True, config={"responsive": True}
    )
    html_table = graph.generate_colored_ftrtable_html(stats)

    # Sample HTML content for the file
    html_content = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <title>{parameter["COM"].replace("_"," ")} {parameter["CUT"]} CHAR</title>
        <link rel="shortcut icon" type="image/png" href="https://www.st.com/etc/clientlibs/st-site/media/app/images/favicon.ico">
        <style>{get_web_content("style.css")}</style>
    </head>
    <body>
        <main>
        {get_web_content("navbar.html")}
        {html_plot} 
        {html_table} 
        {get_web_content("gotop.html")} 
        </main>
        {get_web_content("footer.html")} 
    <script>
    </script>
    """
    print(
        HEAD,
        f"Write html".ljust(150),
        end="\r",
        flush=True,
    )
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(html_content)

    print(
        HEAD,
        f"File created: {file_path}".ljust(150),
        end="\r",
        flush=True,
    )


def gen_composite(parameter, df_stdf, destinationfolder):
    print(HEAD, f"Generate Composite test list... ".ljust(150), end="\r", flush=True)
    file_path = os.path.join(destinationfolder, parameter["COM"])
    os.makedirs(file_path, exist_ok=True)
    file_path = os.path.join(destinationfolder, parameter["COM"], "index.html")

    mir = df_stdf["mir"]

    # Sample HTML content for the file
    html_content = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <title>{parameter["COM"].replace("_"," ")} {parameter["CUT"]} CHAR</title>
        <link rel="shortcut icon" type="image/png" href="https://www.st.com/etc/clientlibs/st-site/media/app/images/favicon.ico">
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
                <td>{str(mir.select(pl.col("FAMLY_ID")).item(0, 0))}</td>
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
            return (prefix, 3)

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
                del grouped_tests[prefix_key]

        return sorted_tests

    # Ottieni i nomi dei test unici
    print(HEAD, f"PTR test list... ".ljust(150), end="\r", flush=True)
    ptrtname = (
        ptr.select(["TestName", "TestNumber"])
        .filter(~pl.col("TestName").cast(pl.Utf8).str.contains("LOG_TTIME"))
        .sort("TestNumber")
        .unique(subset=["TestName"])
        .select("TestName")
        .to_series()
        .to_list()
        if ptr.height > 0
        else []
    )
    ptrtname = ordina_test_name(ptrtname)
    print(HEAD, f"FTR test list... ".ljust(150), end="\r", flush=True)
    ftrtname = (
        ftr.select(["TestName", "TestNumber"])
        .filter(~pl.col("TestName").cast(pl.Utf8).str.contains("LOG_TTIME"))
        .sort("TestNumber")
        .unique(subset=["TestName"])
        .select("TestName")
        .to_series()
        .to_list()
        if ftr.height > 0
        else []
    )

    print(HEAD, f"Write HTML... ".ljust(150), end="\r", flush=True)
    html_content += '<h2 id="TableOfContent">Table of Contents<a class="anchor-link" href="#TableOfContent"></a></h2><hr>'

    # Populate the list in HTML using composite_list items
    content = ""
    testnames = sorted(ptrtname + ftrtname)

    if len(testnames) != 0:
        for tname in testnames:
            content = (
                content
                + "<a class='btn' href='"
                + tname.replace(":", "_")
                + ".html'> "
                + tname
                + " </a>\n"
            )

        html_content += '<div class="contentconteiner">' + content + "<a class='btn' href='limits.html'>Limits</a></div>"

    html_content += f"""
    <script>{get_web_content("script.js")}</script>
    </body>
    </html>
    """

    with open(file_path, "w", encoding="utf-8") as f:
        f.write(html_content)

    print(
        HEAD,
        f"File created: {file_path}".ljust(150),
        end="\r",
        flush=True,
    )
    return ptrtname, ftrtname


def main_graph(DEBUG):
    td = pl.read_csv("./src/td.csv")

    stats, td = process_ptr(td)
    print(HEAD, f"Generate graph... ".ljust(150), end="\r", flush=True)
    STPaletteChar = {
        "-40": "#03234B",
        "-10": "#3CB4E6",
        "30": "#49B170",
        "60": "#A4C238",
        "90": "#FFD200",
        "110": "#FBAB18",
        "130": "#F3693F",
        "140": "#E6007E",
    }
    xwafer = [19, 152]
    ywafer = [21, 173]

    temp_30_data = td.filter(pl.col("°C") == "30")
    if temp_30_data.height > 0:
        ul = temp_30_data.select(pl.col("High Limit")).to_series().mode()[0]
        ll = temp_30_data.select(pl.col("Low Limit")).to_series().mode()[0]
        units = temp_30_data.select(pl.col("Unit")).to_series().mode()[0]
    else:
        ul = td.select(pl.col("High Limit")).to_series().mode()[0]
        ll = td.select(pl.col("Low Limit")).to_series().mode()[0]
        units = td.select(pl.col("Unit")).to_series().mode()[0]

    pl_types = td.select(pl.col("pltype")).unique().to_series().to_list()
    if pl_types[0] == "STD":
        fig = graph.combined_hist_heatmap_box(
            td,
            ll,
            ul,
            units,
            STPaletteChar,
            xwafer,
            ywafer,
        )
        fig.show()

    print(stats)


def main_ftr():
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
    td = pl.read_csv("./src/df_stdf_ftr_SAF.csv")
    path = "./"
    tname = "SA_1"
    df_stdf = {}
    valori_da_rimuovere = [-10, 60, 110]

    # Filtrare il DataFrame mantenendo solo le righe che NON hanno quei valori nella colonna "°C"
    td = td.filter(~pl.col("°C").is_in(valori_da_rimuovere))
    df_stdf["ftr"] = td

    gen_ftr(tname, parameter, df_stdf, path)


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
        "MAIN": ".\\STDF\\44E\\44EZ\\EWSCHAR\\Q445172_05_TTTT",
    }

    # Test case 1
    td = pl.read_csv("./src/MEAS_PLLFASTpll_144storvar.csv")
    path = "./"
    tname = "MEAS_PLLFAST:pll_144storvar"
    td = td.with_columns(pl.col("°C").cast(pl.Utf8))
    df_stdf = {}
    df_stdf["ptr"] = td
    gen_ptr(tname, parameter, df_stdf, path)

    # Test case 1
    td = pl.read_csv("./src/df_stdf_ptr_MBIST.csv")
    path = "./"
    tname = "ALLRAM_PLL:HSI_READY_PLL"
    td = td.with_columns(pl.col("°C").cast(pl.Utf8))
    df_stdf = {}
    df_stdf["ptr"] = td
    gen_ptr(tname, parameter, df_stdf, path)

    # Test case 2
    td = pl.read_csv("./src/df_stdf_ptr_PMB.csv")
    td = td.with_columns(pl.col("°C").cast(pl.Utf8))
    tname = "GET_DATA:SVT_P0_SpeedN"
    df_stdf = {}
    df_stdf["ptr"] = td
    gen_ptr(tname, parameter, df_stdf, path)

    # Test case 3
    td = pl.read_csv("./src/ptrPMU.csv")
    tname = "MEAS_OBL_LDO:pa7"
    td = td.with_columns(pl.col("°C").cast(pl.Utf8))
    df_stdf = {}
    df_stdf["ptr"] = td
    gen_ptr(tname, parameter, df_stdf, path)

    # Test case 4
    td = pl.read_csv("./src/td.csv")
    tname = "OPEN_PE:pa0"
    df_stdf = {}
    df_stdf["ptr"] = td
    gen_ptr(tname, parameter, df_stdf, path)


if __name__ == "__main__":
    DEBUG = True
    main_ptr()
    # main_ftr()
    # main_graph(DEBUG)
