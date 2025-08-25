import os
import re
import glob
import json
import numpy as np
import pandas as pd
# import dask.dataframe as dd
from script.htmlgen import gen_menu, gen_composite, gen_ptr, gen_ftr

HEAD = "[CHAR]"


def power_of_10(value):
    return 10**value


def get_df_stdf_dask(parameter, path):
    all_data = {
        "ptr": [],
        "ftr": [],
        "mir": [],
        "prr": [],
        # "pcr": [],
        # "hbr": [],
        # "sbr": [],
        "tsr": [],
    }

    mainfolder = path.split("CHAR")[0] + "CHAR"

    if not os.path.exists(mainfolder):
        print(f"Main folder {mainfolder} does not exist")
        return

    # Get all directories in main folder
    corner_folders = [
        os.path.join(mainfolder, f)
        for f in os.listdir(mainfolder)
        if os.path.isdir(os.path.join(mainfolder, f))
    ]

    order_list = [
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
    # Ordina le cartelle secondo l'ordine dei codici
    corner_folders = sorted(
        corner_folders,
        key=lambda folder: next(
            (
                i
                for i, code in enumerate(order_list)
                if code in os.path.basename(folder)
            ),
            len(order_list),
        ),
    )

    print(HEAD, f"Found {len(corner_folders)} folders to process", end="\r", flush=True)

    consolidated_data = {
        "ptr": dd.from_pandas(pd.DataFrame(), npartitions=1),
        "ftr": dd.from_pandas(pd.DataFrame(), npartitions=1),
        "mir": dd.from_pandas(pd.DataFrame(), npartitions=1),
        "prr": dd.from_pandas(pd.DataFrame(), npartitions=1),
        # "pcr": dd.from_pandas(pd.DataFrame(), npartitions=1),
        # "hbr": dd.from_pandas(pd.DataFrame(), npartitions=1),
        # "sbr": dd.from_pandas(pd.DataFrame(), npartitions=1),
        "tsr": dd.from_pandas(pd.DataFrame(), npartitions=1),
    }

    # Process each corner folder
    for corner_folder in corner_folders:
        corner_name = os.path.basename(corner_folder).split("_")[-1]
        std_files = glob.glob(os.path.join(corner_folder, "*.std"))

        for std_file in std_files:
            csv_file = os.path.join(
                os.path.dirname(std_file), "csv", os.path.basename(std_file)
            )
            try:
                df_stdf = read_csv_to_daskdataframe(csv_path=csv_file)
                if df_stdf is None:
                    continue

                file_data = process_single_file(corner_name, df_stdf)

                # Concatena direttamente i Dask DataFrame
                for key in consolidated_data.keys():
                    if key in file_data:
                        value = file_data[key]
                        print(
                            HEAD, f"Merging {key}...".ljust(150), end="\r", flush=True
                        )
                        consolidated_data[key] = dd.concat(
                            [consolidated_data[key], value], ignore_index=True
                        )

            except Exception as e:
                print(f"[ERROR] processing {csv_file}: {e}")
                continue

    return consolidated_data


def read_csv_to_daskdataframe(csv_path):
    """
    Legge i file CSV e restituisce un dizionario di DataFrame.
    Questa funzione ora si occupa solo della lettura dei file individuali.

    Args:
        parameter: Dizionario dei parametri contenente le informazioni del file
        csv_path: Percorso base del file CSV

    Returns:
        Dict: Dizionario contenente i DataFrame dei file CSV
    """
    print(HEAD, f"Processing {os.path.basename(csv_path)}", end="\r", flush=True)

    def read_csv_file(file_path: str, usecols=None) -> pd.DataFrame:
        """
        Funzione helper per leggere un file CSV.
        """
        if os.path.exists(file_path):
            try:
                # return pd.read_csv(file_path, usecols=usecols, low_memory=False)

                return dd.read_csv(
                    file_path,
                    usecols=usecols,
                    assume_missing=True,
                    dtype={"UNITS": "object"},
                )

            except Exception as e:
                print(f"[ERROR] Error reading {file_path}: {e}")
                return
        else:
            print(f"[WARNING] File not found: {file_path}")
            return

    # Legge tutti i file CSV necessari
    print(
        HEAD,
        f"Reading... {os.path.basename(csv_path)}.ptr.csv".ljust(150),
        end="\r",
        flush=True,
    )
    ptr = read_csv_file(
        f"{csv_path}.ptr.csv", usecols=[0, 1, 5, 6, 7, 10, 11, 12, 13, 14, 15]
    )
    print(
        HEAD,
        f"Reading... {os.path.basename(csv_path)}.ftr.csv".ljust(150),
        end="\r",
        flush=True,
    )
    ftr = read_csv_file(f"{csv_path}.ftr.csv", usecols=[0, 1, 4, 23])
    print(
        HEAD,
        f"Reading... {os.path.basename(csv_path)}.mtr.csv".ljust(150),
        end="\r",
        flush=True,
    )
    mir = read_csv_file(f"{csv_path}.mir.csv")
    print(
        HEAD,
        f"Reading... {os.path.basename(csv_path)}.prr.csv".ljust(150),
        end="\r",
        flush=True,
    )
    prr = read_csv_file(f"{csv_path}.prr.csv")
    print(
        HEAD,
        f"Reading... {os.path.basename(csv_path)}.pcr.csv".ljust(150),
        end="\r",
        flush=True,
    )
    # pcr = read_csv_file(f"{csv_path}.pcr.csv")
    # print(
    #     HEAD,
    #     f"Reading... {os.path.basename(csv_path)}.hbr.csv".ljust(150),
    #     end="\r",
    #     flush=True,
    # )
    # hbr = read_csv_file(f"{csv_path}.hbr.csv")
    # print(
    #     HEAD,
    #     f"Reading... {os.path.basename(csv_path)}.sbr.csv".ljust(150),
    #     end="\r",
    #     flush=True,
    # )
    # sbr = read_csv_file(f"{csv_path}.sbr.csv")
    # print(
    #     HEAD,
    #     f"Reading... {os.path.basename(csv_path)}.tsr.csv".ljust(150),
    #     end="\r",
    #     flush=True,
    # )
    tsr = read_csv_file(f"{csv_path}.tsr.csv")

    # Crea un dizionario per accedere ai DataFrame
    df_stdf = {
        "ptr": ptr,
        "ftr": ftr,
        "mir": mir,
        "prr": prr,
        # "pcr": pcr,
        # "hbr": hbr,
        # "sbr": sbr,
        "tsr": tsr,
    }

    return df_stdf


def get_df_stdf(parameter, path):
    all_data = {
        "ptr": [],
        "ftr": [],
        "mir": [],
        "prr": [],
        # "pcr": [],
        # "hbr": [],
        # "sbr": [],
        "tsr": [],
    }

    mainfolder = path.split("CHAR")[0] + "CHAR"

    if not os.path.exists(mainfolder):
        print(f"Main folder {mainfolder} does not exist")
        return

    # Get all directories in main folder
    corner_folders = [
        os.path.join(mainfolder, f)
        for f in os.listdir(mainfolder)
        if os.path.isdir(os.path.join(mainfolder, f))
    ]

    order_list = [
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
    # Ordina le cartelle secondo l'ordine dei codici
    corner_folders = sorted(
        corner_folders,
        key=lambda folder: next(
            (
                i
                for i, code in enumerate(order_list)
                if code in os.path.basename(folder)
            ),
            len(order_list),
        ),
    )

    print(HEAD, f"Found {len(corner_folders)} folders to process", end="\r", flush=True)

    consolidated_data = {
        "ptr": pd.DataFrame(),
        "ftr": pd.DataFrame(),
        "mir": pd.DataFrame(),
        "prr": pd.DataFrame(),
        # "pcr": pd.DataFrame(),
        # "hbr": pd.DataFrame(),
        # "sbr": pd.DataFrame(),
        "tsr": pd.DataFrame(),
    }

    # Process each corner folder
    for corner_folder in corner_folders:
        corner_name = os.path.basename(corner_folder).split("_")[-1]
        std_files = glob.glob(os.path.join(corner_folder, "*.std"))

        for std_file in std_files:
            csv_file = os.path.join(
                os.path.dirname(std_file), "csv", os.path.basename(std_file)
            )
            try:
                df_stdf = read_csv_to_dataframe(csv_path=csv_file)
                if df_stdf is None:
                    continue

                file_data = process_single_file(corner_name, df_stdf)

                # Concatena direttamente i Dask DataFrame
                for key in consolidated_data.keys():
                    if key in file_data:
                        value = file_data[key]
                        print(
                            HEAD, f"Merging {key}...".ljust(150), end="\r", flush=True
                        )
                        consolidated_data[key] = pd.concat(
                            [consolidated_data[key], value], ignore_index=True
                        )

            except Exception as e:
                print(f"[ERROR] processing {csv_file}: {e}")
                continue

    return consolidated_data


def read_csv_to_dataframe(csv_path):
    """
    Legge i file CSV e restituisce un dizionario di DataFrame.
    Questa funzione ora si occupa solo della lettura dei file individuali.

    Args:
        parameter: Dizionario dei parametri contenente le informazioni del file
        csv_path: Percorso base del file CSV

    Returns:
        Dict: Dizionario contenente i DataFrame dei file CSV
    """
    print(HEAD, f"Processing {os.path.basename(csv_path)}", end="\r", flush=True)

    def read_csv_file(file_path: str, usecols=None) -> pd.DataFrame:
        """
        Funzione helper per leggere un file CSV.
        """
        if os.path.exists(file_path):
            try:
                return pd.read_csv(file_path, usecols=usecols, memory_map=True)

            except Exception as e:
                print(f"[ERROR] Error reading {file_path}: {e}")
                return
        else:
            print(f"[WARNING] File not found: {file_path}")
            return

    # Legge tutti i file CSV necessari
    print(
        HEAD,
        f"Reading... {os.path.basename(csv_path)}.ptr.csv".ljust(150),
        end="\r",
        flush=True,
    )
    ptr = read_csv_file(
        f"{csv_path}.ptr.csv", usecols=[0, 1, 5, 6, 7, 10, 11, 12, 13, 14, 15]
    )
    print(
        HEAD,
        f"Reading... {os.path.basename(csv_path)}.ftr.csv".ljust(150),
        end="\r",
        flush=True,
    )
    ftr = read_csv_file(f"{csv_path}.ftr.csv", usecols=[0, 1, 4, 23])
    print(
        HEAD,
        f"Reading... {os.path.basename(csv_path)}.mtr.csv".ljust(150),
        end="\r",
        flush=True,
    )
    mir = read_csv_file(f"{csv_path}.mir.csv")
    print(
        HEAD,
        f"Reading... {os.path.basename(csv_path)}.prr.csv".ljust(150),
        end="\r",
        flush=True,
    )
    prr = read_csv_file(f"{csv_path}.prr.csv")
    print(
        HEAD,
        f"Reading... {os.path.basename(csv_path)}.pcr.csv".ljust(150),
        end="\r",
        flush=True,
    )
    # pcr = read_csv_file(f"{csv_path}.pcr.csv")
    # print(
    #     HEAD,
    #     f"Reading... {os.path.basename(csv_path)}.hbr.csv".ljust(150),
    #     end="\r",
    #     flush=True,
    # )
    # hbr = read_csv_file(f"{csv_path}.hbr.csv")
    # print(
    #     HEAD,
    #     f"Reading... {os.path.basename(csv_path)}.sbr.csv".ljust(150),
    #     end="\r",
    #     flush=True,
    # )
    # sbr = read_csv_file(f"{csv_path}.sbr.csv")
    # print(
    #     HEAD,
    #     f"Reading... {os.path.basename(csv_path)}.tsr.csv".ljust(150),
    #     end="\r",
    #     flush=True,
    # )
    tsr = read_csv_file(f"{csv_path}.tsr.csv")

    # Crea un dizionario per accedere ai DataFrame
    df_stdf = {
        "ptr": ptr,
        "ftr": ftr,
        "mir": mir,
        "prr": prr,
        # "pcr": pcr,
        # "hbr": hbr,
        # "sbr": sbr,
        "tsr": tsr,
    }

    return df_stdf


def process_single_file(corner_name, df_stdf):
    """Process a single CSV file and return processed dataframes"""

    ptr = df_stdf["ptr"].copy()
    ftr = df_stdf["ftr"].copy()
    prr = df_stdf["prr"].copy()
    # pcr = df_stdf["pcr"].copy()
    # hbr = df_stdf["hbr"].copy()
    # sbr = df_stdf["sbr"].copy()
    mir = df_stdf["mir"].copy()
    tsr = df_stdf["tsr"].copy()

    # Un solo compute per estrarre i valori necessari
    mir_first_row = mir[["SBLOT_ID", "LOT_ID", "TST_TEMP"]].head(1).iloc[0]

    # Calcola temperatura
    temperature = (
        30
        if str(mir_first_row["TST_TEMP"]) == "nan"
        else int(round(float(mir_first_row["TST_TEMP"]) / 5.0) * 5.0)
    )

    # Lista di tutti i dataframe da aggiornare
    # all_dataframes = [ptr, ftr, prr, hbr, sbr, pcr, mir, tsr]
    all_dataframes = [ptr, ftr, prr, mir, tsr]

    # Assegnazioni in batch
    for df in all_dataframes:
        df["CORNER"] = corner_name
        df["TEMPERATURE"] = temperature

    # Assegna wafer e lot solo a ptr e ftr
    for df in [ptr, ftr]:
        df["WAFER"] = mir_first_row["SBLOT_ID"]
        df["LOT_ID"] = mir_first_row["LOT_ID"]

    return {
        "ptr": ptr,
        "ftr": ftr,
        "mir": mir,
        "prr": prr,
        # "pcr": pcr,
        # "hbr": hbr,
        # "sbr": sbr,
        "tsr": tsr,
    }


def rework_stdf_multiple(parameter, corner_folders):
    """
    Process multiple corner folders containing CSV files

    Args:
        parameter: Configuration dictionary
        corner_folders: List of folder paths, each containing CSV files for different temperatures
    """
    composite = parameter["COM"]
    all_data = {
        "ptr": [],
        "ftr": [],
        "mir": [],
        "prr": [],
        # "pcr": [],
        # "hbr": [],
        # "sbr": [],
        "tsr": [],
    }

    # Load personalization data
    with open("src/jupiter/personalization.json", "r") as file:
        data = json.load(file)

    product_data = data.get(parameter["CODE"], {})
    product_name = product_data.get("product_name", {})
    XY_XL = product_data.get("XY_XL", {})
    XY_XH = product_data.get("XY_XH", {})
    XY_YL = product_data.get("XY_YL", {})
    XY_YH = product_data.get("XY_YH", {})
    XY_Waf = product_data.get("XY_Waf", {})
    XY_Lot0 = product_data.get("XY_Lot0", {})
    XY_Lot1 = product_data.get("XY_Lot1", {})
    XY_Lot2 = product_data.get("XY_Lot2", {})
    XY_Lot3 = product_data.get("XY_Lot3", {})
    XY_Lot4 = product_data.get("XY_Lot4", {})
    XY_Lot5 = product_data.get("XY_Lot5", {})
    XY_Lot6 = product_data.get("XY_Lot6", {})
    xwafer = product_data.get("xwafer", [0, 200])
    ywafer = product_data.get("ywafer", [0, 200])

    parameter["PRODUCT"] = product_name

    # Process each corner folder
    for corner_folder in corner_folders:
        corner_name = os.path.basename(corner_folder).split("_")[-1]

        # Find all CSV files in the corner folder
        std_files = glob.glob(os.path.join(corner_folder, "*.std"))

        for std_file in std_files:
            csv_file = os.path.join(
                os.path.dirname(std_file), "csv", os.path.basename(std_file)
            )
            try:
                df_stdf = read_csv_to_dataframe(parameter=parameter, csv_path=csv_file)
                if df_stdf is None:
                    return

                file_data = process_single_file(
                    csv_file, corner_name, parameter, df_stdf
                )

                # Append to master collections
                for key in all_data.keys():
                    if file_data is None:
                        print(f"[CHAR] WARNIGN: No Data for {composite}".ljust(150))
                        return {}, {}
                    if key not in file_data:
                        continue
                    else:
                        value = file_data[key]
                        if hasattr(value, "empty") and not value.empty:
                            all_data[key].append(value)
                        elif hasattr(value, "__len__") and len(value) > 0:
                            all_data[key].append(value)

            except Exception as e:
                print(f"[ERROR] processing {csv_file}: {e}")
                return {}, {}

    # Consolidate all data
    consolidated_data = {}
    for key, data_list in all_data.items():
        print(HEAD, f"Merge... {key}".ljust(150), end="\r", flush=True)
        if data_list:
            if key in ["ptr", "ftr", "mir", "prr", "pcr", "hbr", "sbr", "tsr"]:
                consolidated_data[key] = pd.concat(data_list, ignore_index=True)
            else:
                consolidated_data[key] = data_list
        else:
            consolidated_data[key] = pd.DataFrame()

    # Now process the consolidated data using the processing logic
    return process_consolidated_data(
        parameter,
        consolidated_data,
        XY_XL,
        XY_XH,
        XY_YL,
        XY_YH,
        XY_Waf,
        XY_Lot0,
        XY_Lot1,
        XY_Lot2,
        XY_Lot3,
        XY_Lot4,
        XY_Lot5,
        XY_Lot6,
        xwafer,
        ywafer,
    )


def process_consolidated_data_dask(parameter, df_stdf):
    """
    Optimized version for Dask DataFrames with minimal conversions
    """
    print(
        f"{HEAD} Data processing started for {parameter.get('COM', 'unknown')}".ljust(
            150
        ),
        end="\r",
        flush=True,
    )

    # Load configuration once
    with open("src/jupiter/personalization.json", "r") as file:
        data = json.load(file)

    product_data = data.get(parameter["CODE"], {})
    XY_XL = product_data.get("XY_XL", {})
    XY_XH = product_data.get("XY_XH", {})
    XY_YL = product_data.get("XY_YL", {})
    XY_YH = product_data.get("XY_YH", {})
    xwafer = product_data.get("xwafer", [0, 200])
    ywafer = product_data.get("ywafer", [0, 200])

    print(HEAD, "Get TEST_NUM...".ljust(150), end="\r", flush=True)

    tsr = df_stdf["tsr"]
    composite = parameter["COM"]

    # Filter test names using Dask operations
    base_pattern = f"_{composite}_"
    mask1 = tsr["TEST_NAM"].str.contains(base_pattern, regex=False, na=False)

    if not mask1.any().compute():
        return parameter, {}

    # Use regex pattern for filtering
    pattern = re.compile(
        f".*_{composite}_.*:.*|.*_{composite}_..$|.*_{composite}_.*_DELTA_.*"
    )

    # Get filtered test names and numbers efficiently
    filtered_tsr = tsr[mask1]
    test_nam_series = filtered_tsr["TEST_NAM"].compute()
    mask2 = test_nam_series.apply(lambda x: bool(pattern.search(str(x))))

    if not mask2.any():
        return parameter, {}

    final_test_nums = filtered_tsr["TEST_NUM"].compute()[mask2.values]
    test_numbers = np.unique(final_test_nums).tolist()

    if len(test_numbers) < 1:
        return parameter, {}

    # Add additional test numbers for non-EWS flows
    if "EWS" not in str(parameter["FLOW"]).upper():
        tnum_keys = [
            "XY_XL",
            "XY_XH",
            "XY_YL",
            "XY_YH",
            "XY_Waf",
            "XY_Lot0",
            "XY_Lot1",
            "XY_Lot2",
            "XY_Lot3",
            "XY_Lot4",
            "XY_Lot5",
            "XY_Lot6",
        ]
        for key in tnum_keys:
            test_numbers.append(product_data.get(key, {}))

    parameter["TEST_NUM"] = test_numbers
    test_nums = (
        parameter["TEST_NUM"]
        if isinstance(parameter["TEST_NUM"], list)
        else [parameter["TEST_NUM"]]
    )
    test_nums_set = set((test_nums))

    # Get data references (Dask DataFrames)
    prr = df_stdf["prr"]
    pcr = df_stdf["pcr"]

    # Filter PTR and FTR data using Dask query (more efficient than isin for large datasets)
    print(HEAD, "Filtering PTR data...".ljust(150), end="\r", flush=True)
    tmpptr = df_stdf["ptr"][df_stdf["ptr"]["TEST_NUM"].isin(test_nums_set)]

    print(HEAD, "Filtering FTR data...".ljust(150), end="\r", flush=True)
    tmpftr = df_stdf["ftr"][df_stdf["ftr"]["TEST_NUM"].isin(test_nums_set)]

    print(HEAD, "Rework data...".ljust(150), end="\r", flush=True)
    # Handle coordinate recalculation efficiently
    # Handle coordinate recalculation for each corner/temperature combination
    if tmpptr.npartitions == 0:
        try:
            if "EWS" not in str(parameter["FLOW"]).upper():
                # Group by corner and temperature for coordinate calculation
                for (corner, temp), group in tmpptr.groupby(["CORNER", "TEMPERATURE"]):
                    mask = (tmpptr["CORNER"] == corner) & (
                        tmpptr["TEMPERATURE"] == temp
                    )

                    # Get corresponding PRR data
                    prr_mask = (prr["CORNER"] == corner) & (prr["TEMPERATURE"] == temp)

                    # Recalculate coordinates for this group
                    if (
                        not group[group["TEST_NUM"] == XY_XH].empty
                        and not group[group["TEST_NUM"] == XY_XL].empty
                    ):
                        combined_X = (
                            group[group["TEST_NUM"] == XY_XH]
                            .set_index("PartID")["RESULT"]
                            .astype(int)
                            .apply(lambda x: x << 8)
                        ) + group[group["TEST_NUM"] == XY_XL].set_index("PartID")[
                            "RESULT"
                        ].astype(
                            int
                        )
                        combined_Y = (
                            group[group["TEST_NUM"] == XY_YH]
                            .set_index("PartID")["RESULT"]
                            .astype(int)
                            .apply(lambda x: x << 8)
                        ) + group[group["TEST_NUM"] == XY_YL].set_index("PartID")[
                            "RESULT"
                        ].astype(
                            int
                        )

                        # Update PRR coordinates for this group
                        prr.loc[prr_mask, "X_COORD"] = prr.loc[prr_mask, "PartID"].map(
                            combined_X
                        )
                        prr.loc[prr_mask, "Y_COORD"] = prr.loc[prr_mask, "PartID"].map(
                            combined_Y
                        )

                        # Apply range check
                        prr.loc[prr_mask, "X_COORD"] = prr.loc[
                            prr_mask, "X_COORD"
                        ].apply(lambda x: x if xwafer[0] <= x <= xwafer[1] else np.nan)
                        prr.loc[prr_mask, "Y_COORD"] = prr.loc[
                            prr_mask, "Y_COORD"
                        ].apply(lambda y: y if ywafer[0] <= y <= ywafer[1] else np.nan)

        except Exception as e:
            print(f"[ERROR] UID Test number wrong ({e})")

    # Remove retest for each corner/temperature combination
    if str(parameter["TYPE"]).upper() != "X30":
        prr = prr.drop_duplicates(
            subset=["X_COORD", "Y_COORD", "CORNER", "TEMPERATURE"], keep="last"
        )

        if not tmpptr.npartitions == 0:
            tmpptr = tmpptr.merge(
                prr[
                    [
                        "PartID",
                        "X_COORD",
                        "Y_COORD",
                        "SOFT_BIN",
                        "HARD_BIN",
                        "CORNER",
                        "TEMPERATURE",
                    ]
                ],
                how="inner",
                on=["PartID", "CORNER", "TEMPERATURE"],
            )
        if not tmpftr.npartitions == 0:
            tmpftr = tmpftr.merge(
                prr[["PartID", "X_COORD", "Y_COORD", "CORNER", "TEMPERATURE"]],
                how="inner",
                on=["PartID", "CORNER", "TEMPERATURE"],
            )
            # Remove retest for each corner/temperature combination
            tmpftr = tmpftr.drop_duplicates(
                subset=["X_COORD", "Y_COORD", "CORNER", "TEMPERATURE", "TEST_TXT"],
                keep="last",
            )

    # Process PTR data
    ptr_dict = {}
    ftr_dict = {}
    SPLIT = "(_vio_|_vbt_|_v11_|_v12_|_v33_|_FRC_)"
    regexsplit = (
        f"(?P<TestName>.*){SPLIT}(?P<SPLIT>[^_]+)_(?P<COM>{composite})_(?P<TARGET>.*)"
    )
    regextest = f"(?P<TestName>.*)_(?P<COM>{composite})_(?P<TARGET>.*)"

    if not tmpptr.npartitions == 0:
        print(HEAD, f"Result Scaling... ".ljust(150), end="\r", flush=True)
        # Result scaling logic (same as original)
        tmpptr["PARM_FLG"] = np.array([int(str(x), 2) for x in tmpptr["PARM_FLG"]])

        def custom_res_scal(group):
            print(
                HEAD,
                f"Result Scaling... {group['TEST_TXT'].iloc[0]} ".ljust(150),
                end="\r",
                flush=True,
            )

            combined = pd.concat(
                [group["RES_SCAL"], group["LLM_SCAL"], group["HLM_SCAL"]]
            )
            combined = combined[combined != 0]
            valid_values = [3, 6, 9, 12, 15, 18, -6, -9]
            combined = combined[combined.isin(valid_values)]

            if combined.empty:
                return 0
            elif all(combined > 0):
                return combined.max()
            elif all(combined < 0):
                return combined.min()
            else:
                return 0

        # Usa transform() invece di apply()
        tmpptr["RES_SCAL"] = tmpptr.groupby("TEST_TXT")["RES_SCAL"].transform(
            lambda x: custom_res_scal(tmpptr.loc[x.index])
        )

        # Apply scaling and unit conversion
        print(HEAD, f"Result Scaling... UNITS".ljust(150), end="\r", flush=True)
        tmpptr["UNITS"] = tmpptr["UNITS"].astype(str)
        tmpptr.loc[tmpptr["RES_SCAL"] == 3, "UNITS"] = (
            "m" + tmpptr.loc[tmpptr["RES_SCAL"] == 3, "UNITS"]
        )
        tmpptr.loc[tmpptr["RES_SCAL"] == 6, "UNITS"] = (
            "u" + tmpptr.loc[tmpptr["RES_SCAL"] == 6, "UNITS"]
        )
        tmpptr.loc[tmpptr["RES_SCAL"] == 9, "UNITS"] = (
            "n" + tmpptr.loc[tmpptr["RES_SCAL"] == 9, "UNITS"]
        )
        tmpptr.loc[tmpptr["RES_SCAL"] == 12, "UNITS"] = (
            "p" + tmpptr.loc[tmpptr["RES_SCAL"] == 12, "UNITS"]
        )
        tmpptr.loc[tmpptr["RES_SCAL"] == 18, "UNITS"] = (
            "a" + tmpptr.loc[tmpptr["RES_SCAL"] == 18, "UNITS"]
        )
        tmpptr.loc[tmpptr["RES_SCAL"] == -3, "UNITS"] = (
            "K" + tmpptr.loc[tmpptr["RES_SCAL"] == -3, "UNITS"]
        )
        tmpptr.loc[tmpptr["RES_SCAL"] == -6, "UNITS"] = (
            "M" + tmpptr.loc[tmpptr["RES_SCAL"] == -6, "UNITS"]
        )
        tmpptr.loc[tmpptr["RES_SCAL"] == -9, "UNITS"] = (
            "G" + tmpptr.loc[tmpptr["RES_SCAL"] == -9, "UNITS"]
        )

        print(HEAD, f"Result Scaling... ".ljust(150), end="\r", flush=True)
        scaling_factor = 10 ** tmpptr["RES_SCAL"].astype(float)

        columns_to_scale = ["RESULT", "HI_LIMIT", "LO_LIMIT"]
        tmpptr[columns_to_scale] = tmpptr[columns_to_scale]
        tmpptr[columns_to_scale] = (
            tmpptr[columns_to_scale].astype(float).mul(scaling_factor, axis=0)
        )

        tmpptr[["HI_LIMIT", "LO_LIMIT"]] = tmpptr[["HI_LIMIT", "LO_LIMIT"]].round(3)

        # Test splitting logic
        if "TTIME" not in composite:

            # ----------==================================================---------- #
            # SPLIT TESTS
            print(HEAD, f"PTR Split... ".ljust(150), end="\r", flush=True)
            # work in a copy dataframe
            testsplit = tmpptr.loc[tmpptr["TEST_TXT"].str.match(regexsplit)].copy()
            # execute split test name
            testsplit[["TestName", "tmp", "Split", "COM", "TARGET"]] = testsplit[
                "TEST_TXT"
            ].str.extract(regexsplit, expand=True)
            # remove all unusefull test
            testsplit = testsplit.dropna(subset=["TestName"])
            testsplit["pltype"] = "SPLIT"
            # remove in test all
            if not testsplit.empty:
                tmpptr = tmpptr.loc[~tmpptr["TEST_TXT"].str.match(regexsplit)]
            # ----------==================================================---------- #

            # ----------==================================================---------- #
            # SPLIT STANDARD TEST
            print(HEAD, f"PTR Standard... ".ljust(150), end="\r", flush=True)
            test = tmpptr.copy()
            test[["TestName", "COM", "TARGET"]] = test["TEST_TXT"].str.extract(
                regextest, expand=True
            )
            test["pltype"] = "STD"
            test = test[test["COM"].notna()]
            # ----------==================================================---------- #

            # ----------==================================================---------- #
            # Rework Test name - Versione corretta con formattazione cifre
            print(HEAD, f"Create Split column... ".ljust(150), end="\r", flush=True)
            test = test.sort_values(["TEST_TXT", "TEST_NUM"]).reset_index(drop=True)
            test["_temp_rank"] = (
                test.groupby("TEST_TXT", sort=False)["TEST_NUM"]
                .rank(method="dense")
                .astype(int)
                - 1
            )
            mask = (
                test.groupby("TEST_TXT", sort=False)["TEST_NUM"].transform("nunique")
                > 1
            )
            test["Split"] = ""
            for group_name, group_data in test[mask].groupby("TEST_TXT"):
                group_indices = group_data.index
                digits = len(str(group_data["_temp_rank"].max() + 1))
                test.loc[group_indices, "Split"] = "Code" + test.loc[
                    group_indices, "_temp_rank"
                ].astype(str).str.zfill(digits)
            test.loc[mask, "pltype"] = "SPLIT"
            test.drop("_temp_rank", axis=1, inplace=True)
            # ----------==================================================---------- #

        else:
            # ----------==================================================---------- #
            # SPLIT STANDARD TEST
            regex = f"(?P<COM>log_ttime)__(?P<TestName>.*)::(?P<TARGET>.*)"
            test = tmpptr.copy()
            test[["COM", "TestName", "TARGET"]] = test["TEST_TXT"].str.extract(
                regextest, expand=True
            )
            test["pltype"] = "STD"
            test = test[test["COM"].notna()]
            # ----------==================================================---------- #

        # ----------==================================================---------- #
        # Choosing the Chart Type
        print(HEAD, f"PTR Cleaning... ".ljust(150), end="\r", flush=True)
        clearptr = pd.concat([test, testsplit])

        if not clearptr.empty:
            regex = "(.*(:.*):.*)|(.*(:.*|DELTA.*))|(.*)"
            extracted = clearptr["TARGET"].str.extract(regex, expand=True)

            clearptr["tmp"] = (
                extracted[0].combine_first(extracted[2]).combine_first(extracted[4])
            )

            clearptr["TARGET"] = extracted[1].combine_first(extracted[3])
            clearptr["FTYPE"] = extracted[2].combine_first(extracted[4])
            clearptr.pop("tmp")
            clearptr.fillna({"TARGET": ""}, inplace=True)
            clearptr["TEST_TXT"] = (
                clearptr.pop("TestName").str.upper() + clearptr["TARGET"]
            )
            clearptr.loc[
                clearptr["TARGET"].str.contains("Trim", case=False),
                "pltype",
            ] = "TRIM"
            clearptr = clearptr.drop(
                clearptr[clearptr["TARGET"].str.contains("TestTime")].index
            )
            clearptr = clearptr.drop(
                clearptr[clearptr["TARGET"].str.contains("ttime")].index
            )

            clearptr.rename(
                columns={
                    "RESULT": "Value",
                    "LO_LIMIT": "Low Limit",
                    "HI_LIMIT": "High Limit",
                    "UNITS": "Unit",
                    "TEMPERATURE": "°C",
                    "TEST_NUM": "TestNumber",
                    "CORNER": "Corner",
                    "TEST_TXT": "TestName",
                },
                inplace=True,
            )
            clearptr = clearptr.drop(["LOT_ID", "TARGET"], axis=1)
            clearptr.fillna({"Split": "Standard"}, inplace=True)
            ptr_dict[parameter["CSV"]] = clearptr
            # ptrtname = clearptr["TestName"].unique()
        # ----------==================================================---------- #

        # ----------==================================================---------- #
        # CLEANING SHEREDE VAR
        testsplit = pd.DataFrame()
        test = pd.DataFrame()
        # ----------==================================================---------- #

    # ----------==================================================---------- #
    if not tmpftr.empty:

        # ----------==================================================---------- #
        # SPLIT TESTS
        print(HEAD, f"FTR Split... ".ljust(150), end="\r", flush=True)
        testsplit = tmpftr.loc[tmpftr["TEST_TXT"].str.match(regexsplit)].copy()
        testsplit[["TestName", "tmp", "Split", "COM", "TARGET"]] = testsplit[
            "TEST_TXT"
        ].str.extract(regexsplit, expand=True)
        testsplit["pltype"] = "SPLIT"
        if not testsplit.empty:
            tmpftr = tmpftr.loc[~tmpftr["TEST_TXT"].str.match(regexsplit)]
        # ----------==================================================---------- #

        # ----------==================================================---------- #
        # SPLIT STANDARD TEST
        print(HEAD, f"FTR Standard... ".ljust(150), end="\r", flush=True)
        test = tmpftr.copy()
        test[["TestName", "COM", "TARGET"]] = test["TEST_TXT"].str.extract(
            regextest, expand=True
        )
        test["pltype"] = "STD"
        test = test[test["COM"].notna()]
        # ----------==================================================---------- #

        # ----------==================================================---------- #
        print(HEAD, f"FTR Cleaning... ".ljust(150), end="\r", flush=True)

        # clearftr = pd.concat(
        #     [testsplit]  # ONLY SPECIAL ARE COMPUTED, STD TEST ARE IGNORED
        # )
        clearftr = pd.concat([test, testsplit])

        if not clearftr.empty:
            clearftr["TEST_TXT"] = clearftr.pop("TestName").str.upper()
            clearftr.rename(
                columns={
                    "TEMPERATURE": "°C",
                    "TEST_NUM": "TestNumber",
                    "CORNER": "Corner",
                    "TEST_TXT": "TestName",
                },
                inplace=True,
            )
            clearftr = clearftr.drop(["LOT_ID", "TARGET", "tmp"], axis=1)
            clearftr.fillna({"Split": "Standard"}, inplace=True)
            # Create Result (1 = test PASS) (0 = test FAIL)
            # That's because we print the passes, so we just have to count
            clearftr["TEST_FLG"] = clearftr["TEST_FLG"].apply(lambda x: int(str(x), 2))
            clearftr["RESULT"] = clearftr["TEST_FLG"].apply(
                lambda x: 1 if x == 0 else 0 if x == 128 else None
            )
            clearftr = clearftr.dropna(subset=["RESULT"])
            # clearftr["RESULT"] = clearftr["RESULT"].apply(lambda x: 1 if x == 0 else 0)
            # clearftr["RESULT"] = (
            #     clearftr["TEST_FLG"]
            #     .apply(lambda x: int(str(x)[-8]) if len(str(x)) >= 8 else 0)
            #     .apply(lambda x: 1 if x == 0 else 0 if x == 1 else "N/A")
            # )
            ftr_dict[parameter["CSV"]] = clearftr
            # ftrtname = clearftr["TestName"].unique()
        # ----------==================================================---------- #

    print(HEAD, f"Save dataframe... ".ljust(150), end="\r", flush=True)
    ptr = pd.DataFrame()
    ftr = pd.DataFrame()
    if len(ptr_dict) != 0:
        ptr = pd.concat(ptr_dict.values(), ignore_index=True)
    if len(ftr_dict) != 0:
        ftr = pd.concat(ftr_dict.values(), ignore_index=True)

    ptr.drop(
        ["TestNumber", "RES_SCAL", "LLM_SCAL", "HLM_SCAL", "FTYPE"],
        axis="columns",
        inplace=True,
        errors="ignore",
    )
    ftr.drop(["TestNumber"], axis="columns", inplace=True, errors="ignore")

    # os.makedirs("./src/jupiter/tmp", exist_ok=True)
    # ptr.to_csv(os.path.abspath("./src/jupiter/tmp/ptr.csv"), index=False)
    # ftr.to_csv(os.path.abspath("./src/jupiter/tmp/ftr.csv"), index=False)

    df_stdf = {
        "ptr": ptr,
        "ftr": ftr,
        "mir": df_stdf["mir"],
        "prr": prr,
        # "pcr": pcr,
        # "hbr": df_stdf["hbr"],
        # "sbr": df_stdf["sbr"],
        "tsr": df_stdf["tsr"],
    }

    print(HEAD, "Processing completed successfully".ljust(150), end="\r", flush=True)
    return parameter, df_stdf


def process_consolidated_data(
    parameter,
    df_stdf,
):
    """
    Process the consolidated data using the original logic
    This function now works on consolidated data with CORNER and TEMPERATURE columns
    """
    print(HEAD, f"Data copy...".ljust(150), end="\r", flush=True)

    with open("src/jupiter/personalization.json", "r") as file:
        data = json.load(file)

    product_data = data.get(parameter["CODE"], {})
    XY_XL = product_data.get("XY_XL", {})
    XY_XH = product_data.get("XY_XH", {})
    XY_YL = product_data.get("XY_YL", {})
    XY_YH = product_data.get("XY_YH", {})
    xwafer = product_data.get("xwafer", [0, 200])
    ywafer = product_data.get("ywafer", [0, 200])

    tsr = df_stdf["tsr"]
    composite = parameter["COM"]

    # Converti esplicitamente a string numpy array
    test_nam_array = tsr["TEST_NAM"].fillna("").astype(str).values
    test_num_array = tsr["TEST_NUM"].values

    # Prima scrematura con numpy string operations corrette
    base_pattern = f"_{composite}_"
    # Usa vectorized string operations di pandas invece di numpy
    mask1 = tsr["TEST_NAM"].str.contains(base_pattern, regex=False, na=False).values

    if not mask1.any():
        return parameter, {}

    pattern = re.compile(
        f".*_{composite}_.*:.*|.*_{composite}_..$|.*_{composite}_.*_DELTA_.*"
    )

    filtered_names = test_nam_array[mask1]
    mask2 = np.array([bool(pattern.search(name)) for name in filtered_names])

    if not mask2.any():
        return parameter, {}

    # Get test numbers finali
    final_test_nums = test_num_array[mask1][mask2]
    test_numbers = np.unique(final_test_nums).tolist()

    if len(test_numbers) < 1:
        return parameter, {}

    if "EWS" not in str(parameter["FLOW"]).upper():
        tnum_keys = [
            "XY_XL",
            "XY_XH",
            "XY_YL",
            "XY_YH",
            "XY_Waf",
            "XY_Lot0",
            "XY_Lot1",
            "XY_Lot2",
            "XY_Lot3",
            "XY_Lot4",
            "XY_Lot5",
            "XY_Lot6",
        ]
        with open("src/jupiter/personalization.json", "r") as file:
            data = json.load(file)
        product_data = data.get(parameter["CODE"], {})
        for key in tnum_keys:
            test_numbers.append(product_data.get(key, {}))
    else:
        with open("src/jupiter/personalization.json", "r") as file:
            data = json.load(file)
        product_data = data.get(parameter["CODE"], {})

    # Process PTR and FTR data similar to original function
    parameter["TEST_NUM"] = test_numbers
    test_nums = (
        parameter["TEST_NUM"]
        if isinstance(parameter["TEST_NUM"], list)
        else [parameter["TEST_NUM"]]
    )
    test_nums_set = set(test_nums)
    print(HEAD, f"Data copy prr...".ljust(150), end="\r", flush=True)
    # prr = df_stdf["prr"].copy(deep=False)
    prr = df_stdf["prr"]
    # print(HEAD, f"Data copy pcr...".ljust(150), end="\r", flush=True)
    # pcr = df_stdf["pcr"].copy(deep=False)
    # pcr = df_stdf["pcr"]
    print(HEAD, f"Data copy ptr...".ljust(150), end="\r", flush=True)
    # tmpptr = df_stdf["ptr"][df_stdf["ptr"]["TEST_NUM"].isin(test_nums)].copy()
    # tmpptr = df_stdf["ptr"].query("TEST_NUM in @test_nums_set").copy(deep=False)
    tmpptr = df_stdf["ptr"].query("TEST_NUM in @test_nums_set")
    print(HEAD, f"Data copy ftr...".ljust(150), end="\r", flush=True)
    # tmpftr = df_stdf["ftr"][df_stdf["ftr"]["TEST_NUM"].isin(test_nums)].copy()
    # tmpftr = df_stdf["ftr"].query("TEST_NUM in @test_nums_set").copy(deep=False)
    tmpftr = df_stdf["ftr"].query("TEST_NUM in @test_nums_set")

    # Handle coordinate recalculation for each corner/temperature combination
    if not tmpptr.empty:
        try:
            if "EWS" not in str(parameter["FLOW"]).upper():
                # Group by corner and temperature for coordinate calculation
                for (corner, temp), group in tmpptr.groupby(["CORNER", "TEMPERATURE"]):
                    mask = (tmpptr["CORNER"] == corner) & (
                        tmpptr["TEMPERATURE"] == temp
                    )

                    # Get corresponding PRR data
                    prr_mask = (prr["CORNER"] == corner) & (prr["TEMPERATURE"] == temp)

                    # Recalculate coordinates for this group
                    if (
                        not group[group["TEST_NUM"] == XY_XH].empty
                        and not group[group["TEST_NUM"] == XY_XL].empty
                    ):
                        combined_X = (
                            group[group["TEST_NUM"] == XY_XH]
                            .set_index("PartID")["RESULT"]
                            .astype(int)
                            .apply(lambda x: x << 8)
                        ) + group[group["TEST_NUM"] == XY_XL].set_index("PartID")[
                            "RESULT"
                        ].astype(
                            int
                        )
                        combined_Y = (
                            group[group["TEST_NUM"] == XY_YH]
                            .set_index("PartID")["RESULT"]
                            .astype(int)
                            .apply(lambda x: x << 8)
                        ) + group[group["TEST_NUM"] == XY_YL].set_index("PartID")[
                            "RESULT"
                        ].astype(
                            int
                        )

                        # Update PRR coordinates for this group
                        prr.loc[prr_mask, "X_COORD"] = prr.loc[prr_mask, "PartID"].map(
                            combined_X
                        )
                        prr.loc[prr_mask, "Y_COORD"] = prr.loc[prr_mask, "PartID"].map(
                            combined_Y
                        )

                        # Apply range check
                        prr.loc[prr_mask, "X_COORD"] = prr.loc[
                            prr_mask, "X_COORD"
                        ].apply(lambda x: x if xwafer[0] <= x <= xwafer[1] else np.nan)
                        prr.loc[prr_mask, "Y_COORD"] = prr.loc[
                            prr_mask, "Y_COORD"
                        ].apply(lambda y: y if ywafer[0] <= y <= ywafer[1] else np.nan)

        except Exception as e:
            print(f"[ERROR] UID Test number wrong ({e})")

    # Remove retest for each corner/temperature combination
    if str(parameter["TYPE"]).upper() != "X30":
        prr = prr.drop_duplicates(
            subset=["X_COORD", "Y_COORD", "CORNER", "TEMPERATURE"], keep="last"
        )

        if not tmpptr.empty:
            tmpptr = tmpptr.merge(
                prr[
                    [
                        "PartID",
                        "X_COORD",
                        "Y_COORD",
                        "SOFT_BIN",
                        "HARD_BIN",
                        "CORNER",
                        "TEMPERATURE",
                    ]
                ],
                how="inner",
                on=["PartID", "CORNER", "TEMPERATURE"],
            )
        if not tmpftr.empty:
            tmpftr = tmpftr.merge(
                prr[["PartID", "X_COORD", "Y_COORD", "CORNER", "TEMPERATURE"]],
                how="inner",
                on=["PartID", "CORNER", "TEMPERATURE"],
            )
            # Remove retest for each corner/temperature combination
            tmpftr = tmpftr.drop_duplicates(
                subset=["X_COORD", "Y_COORD", "CORNER", "TEMPERATURE", "TEST_TXT"],
                keep="last",
            )

    # Process PTR data
    ptr_dict = {}
    ftr_dict = {}
    SPLIT = "(_vio_|_vbt_|_v11_|_v12_|_v33_|_FRC_)"
    regexsplit = (
        f"(?P<TestName>.*){SPLIT}(?P<SPLIT>[^_]+)_(?P<COM>{composite})_(?P<TARGET>.*)"
    )
    regextest = f"(?P<TestName>.*)_(?P<COM>{composite})_(?P<TARGET>.*)"

    if not tmpptr.empty:
        print(HEAD, f"Result Scaling... ".ljust(150), end="\r", flush=True)
        # Result scaling logic (same as original)
        tmpptr["PARM_FLG"] = np.array([int(str(x), 2) for x in tmpptr["PARM_FLG"]])

        tesnames = tmpptr["TEST_TXT"].unique()

        def custom_res_scal(group):
            print(
                HEAD,
                f"Result Scaling... {group['TEST_TXT'].iloc[0]} ".ljust(150),
                end="\r",
                flush=True,
            )

            combined = pd.concat(
                [group["RES_SCAL"], group["LLM_SCAL"], group["HLM_SCAL"]]
            )
            combined = combined[combined != 0]
            valid_values = [3, 6, 9, 12, 15, 18, -6, -9]
            combined = combined[combined.isin(valid_values)]

            if combined.empty:
                return 0
            elif all(combined > 0):
                return combined.max()
            elif all(combined < 0):
                return combined.min()
            else:
                return 0

        # Usa transform() invece di apply()
        tmpptr["RES_SCAL"] = tmpptr.groupby("TEST_TXT")["RES_SCAL"].transform(
            lambda x: custom_res_scal(tmpptr.loc[x.index])
        )

        # Apply scaling and unit conversion
        print(HEAD, f"Result Scaling... UNITS".ljust(150), end="\r", flush=True)
        tmpptr["UNITS"] = tmpptr["UNITS"].astype(str)
        tmpptr.loc[tmpptr["RES_SCAL"] == 3, "UNITS"] = (
            "m" + tmpptr.loc[tmpptr["RES_SCAL"] == 3, "UNITS"]
        )
        tmpptr.loc[tmpptr["RES_SCAL"] == 6, "UNITS"] = (
            "u" + tmpptr.loc[tmpptr["RES_SCAL"] == 6, "UNITS"]
        )
        tmpptr.loc[tmpptr["RES_SCAL"] == 9, "UNITS"] = (
            "n" + tmpptr.loc[tmpptr["RES_SCAL"] == 9, "UNITS"]
        )
        tmpptr.loc[tmpptr["RES_SCAL"] == 12, "UNITS"] = (
            "p" + tmpptr.loc[tmpptr["RES_SCAL"] == 12, "UNITS"]
        )
        tmpptr.loc[tmpptr["RES_SCAL"] == 18, "UNITS"] = (
            "a" + tmpptr.loc[tmpptr["RES_SCAL"] == 18, "UNITS"]
        )
        tmpptr.loc[tmpptr["RES_SCAL"] == -3, "UNITS"] = (
            "K" + tmpptr.loc[tmpptr["RES_SCAL"] == -3, "UNITS"]
        )
        tmpptr.loc[tmpptr["RES_SCAL"] == -6, "UNITS"] = (
            "M" + tmpptr.loc[tmpptr["RES_SCAL"] == -6, "UNITS"]
        )
        tmpptr.loc[tmpptr["RES_SCAL"] == -9, "UNITS"] = (
            "G" + tmpptr.loc[tmpptr["RES_SCAL"] == -9, "UNITS"]
        )

        print(HEAD, f"Result Scaling... ".ljust(150), end="\r", flush=True)
        scaling_factor = 10 ** tmpptr["RES_SCAL"].astype(float)

        columns_to_scale = ["RESULT", "HI_LIMIT", "LO_LIMIT"]
        tmpptr[columns_to_scale] = tmpptr[columns_to_scale]
        tmpptr[columns_to_scale] = (
            tmpptr[columns_to_scale].astype(float).mul(scaling_factor, axis=0)
        )

        tmpptr[["HI_LIMIT", "LO_LIMIT"]] = tmpptr[["HI_LIMIT", "LO_LIMIT"]].round(3)

        # Test splitting logic
        if "TTIME" not in composite:

            # ----------==================================================---------- #
            # SPLIT TESTS
            print(HEAD, f"PTR Split... ".ljust(150), end="\r", flush=True)
            # work in a copy dataframe
            testsplit = tmpptr.loc[tmpptr["TEST_TXT"].str.match(regexsplit)].copy()
            # execute split test name
            testsplit[["TestName", "tmp", "Split", "COM", "TARGET"]] = testsplit[
                "TEST_TXT"
            ].str.extract(regexsplit, expand=True)
            # remove all unusefull test
            testsplit = testsplit.dropna(subset=["TestName"])
            testsplit["pltype"] = "SPLIT"
            # remove in test all
            if not testsplit.empty:
                tmpptr = tmpptr.loc[~tmpptr["TEST_TXT"].str.match(regexsplit)]
            # ----------==================================================---------- #

            # ----------==================================================---------- #
            # SPLIT STANDARD TEST
            print(HEAD, f"PTR Standard... ".ljust(150), end="\r", flush=True)
            test = tmpptr.copy()
            test[["TestName", "COM", "TARGET"]] = test["TEST_TXT"].str.extract(
                regextest, expand=True
            )
            test["pltype"] = "STD"
            test = test[test["COM"].notna()]
            # ----------==================================================---------- #

            # ----------==================================================---------- #
            # Rework Test name - Versione corretta con formattazione cifre
            print(HEAD, f"Create Split column... ".ljust(150), end="\r", flush=True)
            test = test.sort_values(["TEST_TXT", "TEST_NUM"]).reset_index(drop=True)
            test["_temp_rank"] = (
                test.groupby("TEST_TXT", sort=False)["TEST_NUM"]
                .rank(method="dense")
                .astype(int)
                - 1
            )
            mask = (
                test.groupby("TEST_TXT", sort=False)["TEST_NUM"].transform("nunique")
                > 1
            )
            test["Split"] = ""
            for group_name, group_data in test[mask].groupby("TEST_TXT"):
                group_indices = group_data.index
                digits = len(str(group_data["_temp_rank"].max() + 1))
                test.loc[group_indices, "Split"] = "Code" + test.loc[
                    group_indices, "_temp_rank"
                ].astype(str).str.zfill(digits)
            test.loc[mask, "pltype"] = "SPLIT"
            test.drop("_temp_rank", axis=1, inplace=True)
            # ----------==================================================---------- #

        else:
            # ----------==================================================---------- #
            # SPLIT STANDARD TEST
            regex = f"(?P<COM>log_ttime)__(?P<TestName>.*)::(?P<TARGET>.*)"
            test = tmpptr.copy()
            test[["COM", "TestName", "TARGET"]] = test["TEST_TXT"].str.extract(
                regextest, expand=True
            )
            test["pltype"] = "STD"
            test = test[test["COM"].notna()]
            # ----------==================================================---------- #

        # ----------==================================================---------- #
        # Choosing the Chart Type
        print(HEAD, f"PTR Cleaning... ".ljust(150), end="\r", flush=True)
        clearptr = pd.concat([test, testsplit])

        if not clearptr.empty:
            regex = "(.*(:.*):.*)|(.*(:.*|DELTA.*))|(.*)"
            extracted = clearptr["TARGET"].str.extract(regex, expand=True)

            clearptr["tmp"] = (
                extracted[0].combine_first(extracted[2]).combine_first(extracted[4])
            )

            clearptr["TARGET"] = extracted[1].combine_first(extracted[3])
            clearptr["FTYPE"] = extracted[2].combine_first(extracted[4])
            clearptr.pop("tmp")
            clearptr.fillna({"TARGET": ""}, inplace=True)
            clearptr["TEST_TXT"] = (
                clearptr.pop("TestName").str.upper() + clearptr["TARGET"]
            )
            clearptr.loc[
                clearptr["TARGET"].str.contains("Trim", case=False),
                "pltype",
            ] = "TRIM"
            clearptr = clearptr.drop(
                clearptr[clearptr["TARGET"].str.contains("TestTime")].index
            )
            clearptr = clearptr.drop(
                clearptr[clearptr["TARGET"].str.contains("ttime")].index
            )

            clearptr.rename(
                columns={
                    "RESULT": "Value",
                    "LO_LIMIT": "Low Limit",
                    "HI_LIMIT": "High Limit",
                    "UNITS": "Unit",
                    "TEMPERATURE": "°C",
                    "TEST_NUM": "TestNumber",
                    "CORNER": "Corner",
                    "TEST_TXT": "TestName",
                },
                inplace=True,
            )
            clearptr = clearptr.drop(["LOT_ID", "TARGET"], axis=1)
            clearptr.fillna({"Split": "Standard"}, inplace=True)
            ptr_dict[parameter["CSV"]] = clearptr
            # ptrtname = clearptr["TestName"].unique()
        # ----------==================================================---------- #

        # ----------==================================================---------- #
        # CLEANING SHEREDE VAR
        testsplit = pd.DataFrame()
        test = pd.DataFrame()
        # ----------==================================================---------- #

    # ----------==================================================---------- #
    if not tmpftr.empty:

        # ----------==================================================---------- #
        # SPLIT TESTS
        print(HEAD, f"FTR Split... ".ljust(150), end="\r", flush=True)
        testsplit = tmpftr.loc[tmpftr["TEST_TXT"].str.match(regexsplit)].copy()
        testsplit[["TestName", "tmp", "Split", "COM", "TARGET"]] = testsplit[
            "TEST_TXT"
        ].str.extract(regexsplit, expand=True)
        testsplit["pltype"] = "SPLIT"
        if not testsplit.empty:
            tmpftr = tmpftr.loc[~tmpftr["TEST_TXT"].str.match(regexsplit)]
        # ----------==================================================---------- #

        # ----------==================================================---------- #
        # SPLIT STANDARD TEST
        print(HEAD, f"FTR Standard... ".ljust(150), end="\r", flush=True)
        test = tmpftr.copy()
        test[["TestName", "COM", "TARGET"]] = test["TEST_TXT"].str.extract(
            regextest, expand=True
        )
        test["pltype"] = "STD"
        test = test[test["COM"].notna()]
        # ----------==================================================---------- #

        # ----------==================================================---------- #
        print(HEAD, f"FTR Cleaning... ".ljust(150), end="\r", flush=True)

        # clearftr = pd.concat(
        #     [testsplit]  # ONLY SPECIAL ARE COMPUTED, STD TEST ARE IGNORED
        # )
        clearftr = pd.concat([test, testsplit])

        if not clearftr.empty:
            clearftr["TEST_TXT"] = clearftr.pop("TestName").str.upper()
            clearftr.rename(
                columns={
                    "TEMPERATURE": "°C",
                    "TEST_NUM": "TestNumber",
                    "CORNER": "Corner",
                    "TEST_TXT": "TestName",
                },
                inplace=True,
            )
            clearftr = clearftr.drop(["LOT_ID", "TARGET", "tmp"], axis=1)
            clearftr.fillna({"Split": "Standard"}, inplace=True)
            # Create Result (1 = test PASS) (0 = test FAIL)
            # That's because we print the passes, so we just have to count
            clearftr["TEST_FLG"] = clearftr["TEST_FLG"].apply(lambda x: int(str(x), 2))
            clearftr["RESULT"] = clearftr["TEST_FLG"].apply(
                lambda x: 1 if x == 0 else 0 if x == 128 else None
            )
            clearftr = clearftr.dropna(subset=["RESULT"])
            # clearftr["RESULT"] = clearftr["RESULT"].apply(lambda x: 1 if x == 0 else 0)
            # clearftr["RESULT"] = (
            #     clearftr["TEST_FLG"]
            #     .apply(lambda x: int(str(x)[-8]) if len(str(x)) >= 8 else 0)
            #     .apply(lambda x: 1 if x == 0 else 0 if x == 1 else "N/A")
            # )
            ftr_dict[parameter["CSV"]] = clearftr
            # ftrtname = clearftr["TestName"].unique()
        # ----------==================================================---------- #

    print(HEAD, f"Save dataframe... ".ljust(150), end="\r", flush=True)
    ptr = pd.DataFrame()
    ftr = pd.DataFrame()
    if len(ptr_dict) != 0:
        ptr = pd.concat(ptr_dict.values(), ignore_index=True)
    if len(ftr_dict) != 0:
        ftr = pd.concat(ftr_dict.values(), ignore_index=True)

    ptr.drop(
        ["TestNumber", "RES_SCAL", "LLM_SCAL", "HLM_SCAL", "FTYPE"],
        axis="columns",
        inplace=True,
        errors="ignore",
    )
    ftr.drop(["TestNumber"], axis="columns", inplace=True, errors="ignore")

    # os.makedirs("./src/jupiter/tmp", exist_ok=True)
    # ptr.to_csv(os.path.abspath("./src/jupiter/tmp/ptr.csv"), index=False)
    # ftr.to_csv(os.path.abspath("./src/jupiter/tmp/ftr.csv"), index=False)

    df_stdf = {
        "ptr": ptr,
        "ftr": ftr,
        "mir": df_stdf["mir"],
        "prr": prr,
        # "pcr": pcr,
        # "hbr": df_stdf["hbr"],
        # "sbr": df_stdf["sbr"],
        "tsr": df_stdf["tsr"],
    }

    return parameter, df_stdf


def run_report(parameter, df_stdf, report_path):
    parameter, df_stdf = process_consolidated_data(
        parameter=parameter, df_stdf=df_stdf
    )
    if len(parameter) == 0 or len(df_stdf) == 0:
        print(HEAD, f"No test for {parameter["COM"]}".ljust(150))
        return

    ptrtname, ftrtname = gen_composite(parameter, df_stdf, report_path)

    if len(ptrtname) != 0 and len(ftrtname) != 0:
        for tname in ptrtname:
            gen_ptr(tname, parameter, df_stdf, report_path)
        for tname in ftrtname:
            gen_ftr(tname, parameter, df_stdf, report_path)

    elif len(ptrtname) != 0:
        for tname in ptrtname:
            gen_ptr(tname, parameter, df_stdf, report_path)

    elif len(ftrtname) != 0:
        for tname in ftrtname:
            gen_ftr(tname, parameter, df_stdf, report_path)


def run(df_stdf, parameter, composite, report_path, DEBUG=False):
    """Main processing function"""

    print(
        f"{HEAD} Start {parameter['CUT']} {composite}".ljust(150), end="\r", flush=True
    )

    if not parameter and not df_stdf:
        print(HEAD, "No test found... ".ljust(150), end="\r", flush=True)
        return

    print(HEAD, "Start Report generation... ".ljust(150), end="\r", flush=True)
    run_report(parameter, df_stdf, report_path)


def gen_mainmenu(parameter, path):
    gen_menu(parameter=parameter, destinationfolder=path)


def main(DEBUG):
    # Example usage
    path = ".\\STDF\\44E\\44EZ\\EWSCHAR\\"
    parameter = {
        "TITLE": "PMU EWSCHAR char",
        "COM": "PMU",
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

    composite = "PMU"

    run(path, parameter, composite, DEBUG)


if __name__ == "__main__":
    DEBUG = True
    main(DEBUG)
