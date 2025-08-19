import os
import pandas as pd
import glob
import json
import numpy as np
from script.htmlgen import gen_menu, gen_composite, gen_ptr, gen_ftr

HEAD = "[CHAR]"
FLUSH = " " * 200


def power_of_10(value):
    return 10**value


def read_csv_to_dataframe(parameter, csv_path):
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
                return pd.read_csv(file_path, usecols=usecols, low_memory=False)
            except Exception as e:
                print(f"[ERROR] Error reading {file_path}: {e}")
                return pd.DataFrame()
        else:
            print(f"[WARNING] File not found: {file_path}")
            return pd.DataFrame()

    # Legge tutti i file CSV necessari
    print(
        HEAD,
        f"Reading... {os.path.basename(csv_path)}.ptr.csv",
        FLUSH,
        end="\r",
        flush=True,
    )
    ptr = pd.DataFrame()
    ftr = pd.DataFrame()
    mir = pd.DataFrame()
    prr = pd.DataFrame()
    pcr = pd.DataFrame()
    hbr = pd.DataFrame()
    sbr = pd.DataFrame()
    tsr = pd.DataFrame()
    ptr = read_csv_file(
        f"{csv_path}.ptr.csv", usecols=[0, 1, 5, 6, 7, 10, 11, 12, 13, 14, 15]
    )
    print(
        HEAD,
        f"Reading... {os.path.basename(csv_path)}.ftr.csv",
        FLUSH,
        end="\r",
        flush=True,
    )
    ftr = read_csv_file(f"{csv_path}.ftr.csv", usecols=[0, 1, 4, 23])
    print(
        HEAD,
        f"Reading... {os.path.basename(csv_path)}.mtr.csv",
        FLUSH,
        end="\r",
        flush=True,
    )
    mir = read_csv_file(f"{csv_path}.mir.csv")
    print(
        HEAD,
        f"Reading... {os.path.basename(csv_path)}.prr.csv",
        FLUSH,
        end="\r",
        flush=True,
    )
    prr = read_csv_file(f"{csv_path}.prr.csv")
    print(
        HEAD,
        f"Reading... {os.path.basename(csv_path)}.pcr.csv",
        FLUSH,
        end="\r",
        flush=True,
    )
    pcr = read_csv_file(f"{csv_path}.pcr.csv")
    print(
        HEAD,
        f"Reading... {os.path.basename(csv_path)}.hbr.csv",
        FLUSH,
        end="\r",
        flush=True,
    )
    hbr = read_csv_file(f"{csv_path}.hbr.csv")
    print(
        HEAD,
        f"Reading... {os.path.basename(csv_path)}.sbr.csv",
        FLUSH,
        end="\r",
        flush=True,
    )
    sbr = read_csv_file(f"{csv_path}.sbr.csv")
    print(
        HEAD,
        f"Reading... {os.path.basename(csv_path)}.tsr.csv",
        FLUSH,
        end="\r",
        flush=True,
    )
    tsr = read_csv_file(f"{csv_path}.tsr.csv")

    # Crea un dizionario per accedere ai DataFrame
    df_stdf = {
        "ptr": ptr,
        "ftr": ftr,
        "mir": mir,
        "prr": prr,
        "pcr": pcr,
        "hbr": hbr,
        "sbr": sbr,
        "tsr": tsr,
    }

    return df_stdf


def process_single_file(csv_file, corner_name, parameter, df_stdf):
    """Process a single CSV file and return processed dataframes"""

    # Extract temperature from MIR
    mir = df_stdf["mir"].copy()
    if str(mir["TST_TEMP"].iloc[0]) == "nan":
        mir["TST_TEMP"] = "30"
    temperature = int(round(float(mir["TST_TEMP"].iloc[0]) / 5.0) * 5.0)
    print(
        HEAD, f"Store... {corner_name} at {temperature}°C", FLUSH, end="\r", flush=True
    )

    tsr = df_stdf["tsr"].copy()
    match_group = tsr["TEST_NAM"].str.extract(
        r"(.*_{0}_.*:.*|.*_{0}_..$|.*_{0}_.*_DELTA_.*)".format(parameter["COM"])
    )
    tsr["match_group"] = match_group[0]

    test_numbers = tsr.loc[tsr["match_group"].notnull(), "TEST_NUM"].unique().tolist()
    if len(test_numbers) < 1:
        return

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
    tmpptr = df_stdf["ptr"][df_stdf["ptr"]["TEST_NUM"].isin(test_nums)].copy()
    tmpftr = df_stdf["ftr"][df_stdf["ftr"]["TEST_NUM"].isin(test_nums)].copy()
    # tmpptr = df_stdf["ptr"].copy()
    # tmpftr = df_stdf["ftr"].copy()
    prr = df_stdf["prr"].copy()
    pcr = df_stdf["pcr"].copy()
    hbr = df_stdf["hbr"].copy()
    sbr = df_stdf["sbr"].copy()

    # Add corner and temperature info
    tmpptr["CORNER"] = corner_name
    tmpptr["TEMPERATURE"] = temperature
    tmpftr["CORNER"] = corner_name
    tmpftr["TEMPERATURE"] = temperature
    prr["CORNER"] = corner_name
    prr["TEMPERATURE"] = temperature
    hbr["CORNER"] = corner_name
    hbr["TEMPERATURE"] = temperature
    sbr["CORNER"] = corner_name
    sbr["TEMPERATURE"] = temperature
    pcr["CORNER"] = corner_name
    pcr["TEMPERATURE"] = temperature
    mir["CORNER"] = corner_name
    mir["TEMPERATURE"] = temperature

    # Add wafer and lot info
    tmpptr["WAFER"] = mir.SBLOT_ID.iloc[0]
    tmpftr["WAFER"] = mir.SBLOT_ID.iloc[0]
    tmpptr["LOT_ID"] = mir.LOT_ID.iloc[0]
    tmpftr["LOT_ID"] = mir.LOT_ID.iloc[0]

    return {
        "ptr": tmpptr,
        "ftr": tmpftr,
        "mir": mir,
        "prr": prr,
        "pcr": pcr,
        "hbr": hbr,
        "sbr": sbr,
        "tsr": tsr,
        "temperature": temperature,
        "corner": corner_name,
        "csv_file": csv_file,
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
        "pcr": [],
        "hbr": [],
        "sbr": [],
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
        print(HEAD, f"Merge... {key}", FLUSH, end="\r", flush=True)
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


def process_consolidated_data(
    parameter,
    df_stdf,
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
):
    """
    Process the consolidated data using the original logic
    This function now works on consolidated data with CORNER and TEMPERATURE columns
    """
    print(HEAD, f"Data copy...", FLUSH, end="\r", flush=True)
    composite = parameter["COM"]

    tmpptr = df_stdf["ptr"].copy()
    tmpftr = df_stdf["ftr"].copy()
    prr = df_stdf["prr"].copy()
    pcr = df_stdf["pcr"].copy()

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
        print(HEAD, f"Result Scaling... ", FLUSH, end="\r", flush=True)
        # Result scaling logic (same as original)
        tmpptr["PARM_FLG"] = np.array([int(str(x), 2) for x in tmpptr["PARM_FLG"]])

        tesnames = tmpptr["TEST_TXT"].unique()

        def custom_res_scal(group):
            print(
                HEAD,
                f"Result Scaling... {group['TEST_TXT'].iloc[0]} ",
                FLUSH,
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
        print(HEAD, f"Result Scaling... UNITS", FLUSH, end="\r", flush=True)
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

        print(HEAD, f"Result Scaling... ", FLUSH, end="\r", flush=True)
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
            print(HEAD, f"PTR Split... ", FLUSH, end="\r", flush=True)
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
            print(HEAD, f"PTR Standard... ", FLUSH, end="\r", flush=True)
            test = tmpptr.copy()
            test[["TestName", "COM", "TARGET"]] = test["TEST_TXT"].str.extract(
                regextest, expand=True
            )
            test["pltype"] = "STD"
            test = test[test["COM"].notna()]
            # ----------==================================================---------- #

            # ----------==================================================---------- #
            # Rework Test name - Versione corretta con formattazione cifre
            print(HEAD, f"Create Split column... ", FLUSH, end="\r", flush=True)
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
        print(HEAD, f"PTR Cleaning... ", FLUSH, end="\r", flush=True)
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
        print(HEAD, f"FTR Split... ", FLUSH, end="\r", flush=True)
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
        print(HEAD, f"FTR Standard... ", FLUSH, end="\r", flush=True)
        test = tmpftr.copy()
        test[["TestName", "COM", "TARGET"]] = test["TEST_TXT"].str.extract(
            regextest, expand=True
        )
        test["pltype"] = "STD"
        test = test[test["COM"].notna()]
        # ----------==================================================---------- #

        # ----------==================================================---------- #
        print(HEAD, f"FTR Cleaning... ", FLUSH, end="\r", flush=True)

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

    print(HEAD, f"Save dataframe... ", FLUSH, end="\r", flush=True)
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

    os.makedirs("./src/jupiter/tmp", exist_ok=True)
    ptr.to_csv(os.path.abspath("./src/jupiter/tmp/ptr.csv"), index=False)
    ftr.to_csv(os.path.abspath("./src/jupiter/tmp/ftr.csv"), index=False)

    df_stdf = {
        "ptr": ptr,
        "ftr": ftr,
        "mir": df_stdf["mir"],
        "prr": prr,
        "pcr": pcr,
        "hbr": df_stdf["hbr"],
        "sbr": df_stdf["sbr"],
        "tsr": df_stdf["tsr"],
    }

    return parameter, df_stdf


def run_report(parameter, df_stdf, path):
    ptrtname, ftrtname = gen_composite(parameter, df_stdf, path)
    if len(ptrtname) != 0 and len(ftrtname) != 0:
        for tname in ptrtname:
            gen_ptr(tname, parameter, df_stdf, path)
        for tname in ftrtname:
            gen_ftr(tname, parameter, df_stdf, path)

    elif len(ptrtname) != 0:
        for tname in ptrtname:
            gen_ptr(tname, parameter, df_stdf, path)

    elif len(ftrtname) != 0:
        for tname in ftrtname:
            gen_ftr(tname, parameter, df_stdf, path)


def run(path, parameter, composite, DEBUG=False):
    """Main processing function"""

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

    print(f"{HEAD} Start {parameter['CUT']} {composite}", FLUSH)

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

    print(HEAD, f"Found {len(corner_folders)} folders to process", end="\r", flush=True)

    parameter, df_stdf = rework_stdf_multiple(parameter, corner_folders)

    if not parameter and not df_stdf:
        print(HEAD, "No test found... ", FLUSH, end="\r", flush=True)
        return

    print(HEAD, f"Start Report generation... ", FLUSH, end="\r", flush=True)
    run_report(parameter, df_stdf, path)


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
