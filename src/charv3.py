import os
import polars as pl
import glob
import json
import numpy as np
from script.htmlgenv2 import gen_menu, gen_composite, gen_ptr, gen_ftr, gen_limits

HEAD = "[CHAR]"

# Configuration constants
# Configurazioni DTYPE specifiche per ogni tipo di file STDF
DTYPE_CONFIGS = {
    "ptr": {
        "PartID": pl.UInt16,
        "TEST_NUM": pl.UInt32,
        "PARM_FLG": pl.UInt32,
        "RESULT": pl.Float64,
        "TEST_TXT": pl.Utf8,
        "RES_SCAL": pl.Int8,
        "LLM_SCAL": pl.Int8,
        "HLM_SCAL": pl.Int8,
        "LO_LIMIT": pl.Float64,
        "HI_LIMIT": pl.Float64,
        "UNITS": pl.Utf8,
        "TST_TEMP": pl.Utf8,
        "X_COORD": pl.UInt16,
        "Y_COORD": pl.UInt16,
        "SOFT_BIN": pl.UInt16,
        "HARD_BIN": pl.UInt16,
    },
    "ftr": {
        "PartID": pl.UInt16,
        "TEST_NUM": pl.UInt32,
        "TEST_FLG": pl.Utf8,
        "TEST_TXT": pl.Utf8,
        "TST_TEMP": pl.Utf8,
        "X_COORD": pl.UInt16,
        "Y_COORD": pl.UInt16,
        "SOFT_BIN": pl.UInt16,
        "HARD_BIN": pl.UInt16,
    },
    "mir": {
        "SETUP_T": pl.UInt32,
        "START_T": pl.UInt32,
        "STAT_NUM": pl.UInt8,
        "MODE_COD": pl.Utf8,
        "RTST_COD": pl.Utf8,
        "PROT_COD": pl.Utf8,
        "BURN_TIM": pl.UInt16,
        "CMOD_COD": pl.Utf8,
        "TST_TEMP": pl.Utf8,
        "USER_TXT": pl.Utf8,
        "AUX_FILE": pl.Utf8,
        "PKG_TYP": pl.Utf8,
        "FAMLY_ID": pl.Utf8,
        "DATE_COD": pl.Utf8,
        "FACIL_ID": pl.Utf8,
        "FLOOR_ID": pl.Utf8,
        "PROC_ID": pl.Utf8,
        "OPER_NAM": pl.Utf8,
        "OPER_TYP": pl.Utf8,
        "NODE_NAM": pl.Utf8,
        "TSTR_TYP": pl.Utf8,
        "JOB_NAM": pl.Utf8,
        "JOB_REV": pl.Utf8,
        "SBLOT_ID": pl.Utf8,
        "OPER_FRQ": pl.Utf8,
        "SPEC_NAM": pl.Utf8,
        "SPEC_VER": pl.Utf8,
        "FLOW_ID": pl.Utf8,
        "SETUP_ID": pl.Utf8,
        "DSGN_REV": pl.Utf8,
        "ENG_ID": pl.Utf8,
        "ROM_COD": pl.Utf8,
        "SERL_NUM": pl.Utf8,
        "SUPR_NAM": pl.Utf8,
    },
    "prr": {
        "PartID": pl.UInt16,
        "HEAD_NUM": pl.UInt8,
        "SITE_NUM": pl.UInt8,
        "PART_FLG": pl.UInt8,
        "NUM_TEST": pl.UInt16,
        "HARD_BIN": pl.UInt16,
        "SOFT_BIN": pl.UInt16,
        "X_COORD": pl.UInt16,
        "Y_COORD": pl.UInt16,
        "TEST_T": pl.UInt32,
        "PART_ID": pl.Utf8,
        "PART_TXT": pl.Utf8,
        "PART_FIX": pl.Utf8,
    },
    "tsr": {
        "HEAD_NUM": pl.UInt8,
        "SITE_NUM": pl.UInt8,
        "TEST_TYP": pl.Utf8,
        "TEST_NUM": pl.UInt32,
        "EXEC_CNT": pl.UInt32,
        "FAIL_CNT": pl.UInt32,
        "ALRM_CNT": pl.UInt32,
        "TEST_NAM": pl.Utf8,
        "SEQ_NAME": pl.Utf8,
        "TEST_LBL": pl.Utf8,
        "OPT_FLAG": pl.UInt8,
        "TEST_TIM": pl.Float32,
        "TEST_MIN": pl.Float32,
        "TEST_MAX": pl.Float32,
        "TST_SUMS": pl.Float32,
        "TST_SQRS": pl.Float32,
    },
    "pcr": {
        "HEAD_NUM": pl.UInt8,
        "SITE_NUM": pl.UInt8,
        "PART_CNT": pl.UInt32,
        "RTST_CNT": pl.UInt32,
        "ABRT_CNT": pl.UInt32,
        "GOOD_CNT": pl.UInt32,
        "FUNC_CNT": pl.UInt32,
    },
    "hbr": {
        "HEAD_NUM": pl.UInt8,
        "SITE_NUM": pl.UInt8,
        "HBIN_NUM": pl.UInt16,
        "HBIN_CNT": pl.UInt32,
        "HBIN_PF": pl.Utf8,
        "HBIN_NAM": pl.Utf8,
    },
    "sbr": {
        "HEAD_NUM": pl.UInt8,
        "SITE_NUM": pl.UInt8,
        "SBIN_NUM": pl.UInt16,
        "SBIN_CNT": pl.UInt32,
        "SBIN_PF": pl.Utf8,
        "SBIN_NAM": pl.Utf8,
    },
}

SCALE_PREFIXES = {
    18: "a",  # atto
    12: "p",  # pico
    9: "n",  # nano
    6: "u",  # micro
    3: "m",  # milli
    -3: "K",  # kilo
    -6: "M",  # mega
    -9: "G",  # giga
}


def load_personalization_data(code: str) -> dict:
    """Load personalization data from JSON file."""
    try:
        with open("src/jupiter/personalization.json", "r") as file:
            data = json.load(file)
        return data.get(code, {})
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"[WARNING] Error loading personalization.json: {e}")
        return {}


def read_csv_file_polars(file_path: str, columns=None, file_type=None) -> pl.DataFrame:
    """Read CSV file using polars with appropriate schema for each file type."""
    if not os.path.exists(file_path):
        # print(f"[WARNING] File not found: {file_path}")
        return pl.DataFrame()

    try:
        # Usa lo schema appropriato per il tipo di file
        schema_overrides = DTYPE_CONFIGS.get(file_type, {})

        df = pl.read_parquet(file_path, columns=columns)

        # Forza tutte le colonne a essere stringhe
        string_columns = []
        for col in df.columns:
            string_columns.append(pl.col(col).cast(pl.Utf8))
        df = df.with_columns(string_columns)

        # Poi applica le conversioni di tipo specifiche
        schema_overrides = DTYPE_CONFIGS.get(file_type, {})
        if schema_overrides:
            cast_columns = []
            for col_name, dtype in schema_overrides.items():
                if col_name in df.columns:
                    cast_columns.append(pl.col(col_name).cast(dtype))
            if cast_columns:
                df = df.with_columns(cast_columns)

        return df

    except Exception as e:
        print(f"[ERROR] Error reading {file_path}: {e}")
        # Fallback senza schema
        try:
            return pl.read_parquet(file_path, columns=columns)
        except Exception as e2:
            print(f"[ERROR] Fallback failed for {file_path}: {e2}")
            return pl.DataFrame()


def read_stdf_files(csv_path: str) -> dict:
    """Read all STDF CSV files and return dictionary of DataFrames."""
    print(HEAD, f"Processing {os.path.basename(csv_path)}", end="\r", flush=True)

    file_configs = {
        "ptr": {
            "columns": [0, 1, 5, 6, 7, 10, 11, 12, 13, 14, 15],
        },
        "ftr": {
            "columns": [0, 1, 4, 23],
        },
        "mir": {
            "columns": [8, 10, 11, 12, 13, 14, 19, 23],
        },
        "prr": {
            "columns": [0, 5, 6, 7, 8],
        },
        # "pcr": {
        #     "columns": None,
        # },
        # "hbr": {
        #     "columns": None,
        # },
        # "sbr": {
        #     "columns": None,
        # },
        "tsr": {"columns": [3, 7]},
    }

    df_stdf = {}
    for file_type, config in file_configs.items():
        file_path = f"{csv_path}.{file_type}.parquet"
        print(
            HEAD,
            f"Reading... {os.path.basename(file_path)}".ljust(150),
            end="\r",
            flush=True,
        )

        df_stdf[file_type] = read_csv_file_polars(
            file_path, config["columns"], file_type
        )

    return df_stdf


def extract_temperature(mir_df: pl.DataFrame) -> int:
    """Extract and round temperature from MIR dataframe."""
    if mir_df.is_empty():
        return 30

    temp_value = float(mir_df.select("TST_TEMP").to_numpy().flatten()[0])
    if np.isnan(temp_value):
        return 30

    return int(round(float(temp_value) / 5.0) * 5.0)


def get_remove_testnumber(path):
    file_path = os.path.join(path, "ART.json")

    if not os.path.isfile(file_path):
        return []

    with open(file_path, "r", encoding="utf-8") as file:
        dati = json.load(file)

    if "remove_TestNumber" in dati:
        toremove = dati["remove_TestNumber"]
        return toremove
    else:
        return []


def filter_test_numbers(
    tsr_df: pl.DataFrame, composite: str, product_data: dict, flow: str, path: str
) -> list:
    """Filter and extract test numbers based on composite and flow."""
    if tsr_df.is_empty():
        return []

    # Use polars string operations for better performance
    pattern = f".*_{composite}_.*:.*|.*_{composite}_..$|.*_{composite}_.*_DELTA_.*"
    filtered_df = tsr_df.filter(pl.col("TEST_NAM").str.contains(pattern))

    test_numbers = filtered_df.select("TEST_NUM").unique().to_series().to_list()

    if "EWS" not in str(flow).upper():
        xy_keys = [
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
        for key in xy_keys:
            if key in product_data:
                test_numbers.append(product_data[key])

    toremove = get_remove_testnumber(path)

    test_numbers = [num for num in test_numbers if num not in toremove]

    return test_numbers


def apply_result_scaling(ptr_df: pl.DataFrame) -> pl.DataFrame:
    """Apply result scaling using polars expressions."""
    print(HEAD, f"Result Scaling... ".ljust(150), end="\r", flush=True)
    ptr_df = ptr_df.with_columns(ptr_df['RESULT'].cast(pl.Float32))
    ptr_df = ptr_df.with_columns(ptr_df['LO_LIMIT'].cast(pl.Float32))
    ptr_df = ptr_df.with_columns(ptr_df['HI_LIMIT'].cast(pl.Float32))
    return ptr_df.drop(["LLM_SCAL", "HLM_SCAL"])
    return ptr_df.with_columns(
        [
            pl.when(pl.col("RES_SCAL").is_not_null())
            .then(pl.col("RESULT") * (10.0 ** pl.col("RES_SCAL")))
            .otherwise(pl.col("RESULT"))
            .cast(pl.Float32)
            .alias("RESULT"),
            pl.when(pl.col("LLM_SCAL").is_not_null())
            .then(pl.col("LO_LIMIT") * (10.0 ** pl.col("RES_SCAL")))
            .otherwise(pl.col("LO_LIMIT"))
            .cast(pl.Float32)
            .alias("LO_LIMIT"),
            pl.when(pl.col("HLM_SCAL").is_not_null())
            .then(pl.col("HI_LIMIT") * (10.0 ** pl.col("RES_SCAL")))
            .otherwise(pl.col("HI_LIMIT"))
            .cast(pl.Float32)
            .alias("HI_LIMIT"),
        ]
    ).drop(["LLM_SCAL", "HLM_SCAL"])


def apply_unit_prefixes(ptr_df: pl.DataFrame) -> pl.DataFrame:
    """Apply unit prefixes based on scale values."""
    # Convert UNITS to string first
    print(HEAD, f"Unit Scaling... ".ljust(150), end="\r", flush=True)
    df = ptr_df.with_columns(pl.col("UNITS").cast(pl.Utf8))
    return df.drop(["RES_SCAL"])
    mapping_df = pl.DataFrame(
        {
            "RES_SCAL": list(SCALE_PREFIXES.keys()),
            "PREFIX": list(SCALE_PREFIXES.values()),
        }
    )

    # Join con il dataframe originale
    df = df.join(mapping_df, on="RES_SCAL", how="left")

    # Costruiamo la nuova colonna UNITS
    df = df.with_columns(
        pl.when(pl.col("PREFIX").is_not_null())
        .then(pl.col("PREFIX") + pl.col("UNITS"))
        .otherwise(pl.col("UNITS"))
        .alias("UNITS")
    ).drop("PREFIX")


def process_coordinate_recalculation(
    ptr_df: pl.DataFrame, prr_df: pl.DataFrame, product_data: dict, flow: str
) -> pl.DataFrame:
    """Recalculate coordinates for each corner/temperature combination."""
    if ptr_df.is_empty() or "EWS" in str(flow).upper():
        return prr_df

    xy_keys = ["XY_XL", "XY_XH", "XY_YL", "XY_YH"]
    if not all(key in product_data for key in xy_keys):
        return prr_df

    # Extract coordinate test results efficiently
    coord_data = ptr_df.filter(
        pl.col("TEST_NUM").is_in([product_data[key] for key in xy_keys])
    ).select(["PartID", "TEST_NUM", "RESULT", "CORNER", "TEMPERATURE"])

    if coord_data.is_empty():
        return prr_df

    # Pivot to get X and Y coordinates
    coord_pivot = coord_data.pivot(
        values="RESULT", index=["PartID", "CORNER", "TEMPERATURE"], columns="TEST_NUM"
    )

    # Calculate combined coordinates
    xh_col = str(product_data["XY_XH"])
    xl_col = str(product_data["XY_XL"])
    yh_col = str(product_data["XY_YH"])
    yl_col = str(product_data["XY_YL"])

    if all(col in coord_pivot.columns for col in [xh_col, xl_col, yh_col, yl_col]):
        coord_pivot = coord_pivot.with_columns(
            [
                (
                    (pl.col(xh_col).cast(pl.Int32) << 8) + pl.col(xl_col).cast(pl.Int32)
                ).alias("X_COORD_NEW"),
                (
                    (pl.col(yh_col).cast(pl.Int32) << 8) + pl.col(yl_col).cast(pl.Int32)
                ).alias("Y_COORD_NEW"),
            ]
        )

        # Apply range filtering
        xwafer = product_data.get("xwafer", [0, 200])
        ywafer = product_data.get("ywafer", [0, 200])

        coord_pivot = coord_pivot.with_columns(
            [
                pl.when(
                    (pl.col("X_COORD_NEW") >= xwafer[0])
                    & (pl.col("X_COORD_NEW") <= xwafer[1])
                )
                .then(pl.col("X_COORD_NEW"))
                .otherwise(None)
                .alias("X_COORD_NEW"),
                pl.when(
                    (pl.col("Y_COORD_NEW") >= ywafer[0])
                    & (pl.col("Y_COORD_NEW") <= ywafer[1])
                )
                .then(pl.col("Y_COORD_NEW"))
                .otherwise(None)
                .alias("Y_COORD_NEW"),
            ]
        )

        # Join back to PRR
        coord_update = coord_pivot.select(
            ["PartID", "CORNER", "TEMPERATURE", "X_COORD_NEW", "Y_COORD_NEW"]
        )
        prr_df = prr_df.join(
            coord_update, on=["PartID", "CORNER", "TEMPERATURE"], how="left"
        )

        # Update coordinates
        prr_df = prr_df.with_columns(
            [
                pl.coalesce(pl.col("X_COORD_NEW"), pl.col("X_COORD")).alias("X_COORD"),
                pl.coalesce(pl.col("Y_COORD_NEW"), pl.col("Y_COORD")).alias("Y_COORD"),
            ]
        ).drop(["X_COORD_NEW", "Y_COORD_NEW"])

    return prr_df


def remove_retests(prr_df: pl.DataFrame, test_type: str) -> pl.DataFrame:
    """Remove retests keeping the last occurrence."""
    if test_type.upper() == "LOOP":
        return prr_df

    return prr_df.unique(
        subset=["X_COORD", "Y_COORD", "CORNER", "TEMPERATURE"], keep="last"
    )


def parse_test_names_regex(
    df: pl.DataFrame, composite: str, is_ttime: bool = False
) -> pl.DataFrame:
    """Parse test names using regex patterns optimized for polars with categorical TEST_TXT."""

    if is_ttime:
        # TTIME handling - convert to string temporarily for regex operations
        regex_ttime = rf"(?P<COM>{composite})__(?P<TestName>.*)::(?P<TARGET>.*)"

        df = df.with_columns(
            [
                pl.col("TEST_TXT")
                .cast(pl.Utf8)
                .str.extract(regex_ttime, 2)
                .alias("TestName"),
                pl.lit(composite).alias("COM"),
                pl.col("TEST_TXT")
                .cast(pl.Utf8)
                .str.extract(regex_ttime, 3)
                .alias("TARGET"),
                pl.lit("STD").cast(pl.Categorical).alias("pltype"),
                pl.lit("Standard").cast(pl.Categorical).alias("Split"),
            ]
        )

    else:
        SPLIT = "(vio|vbt|v11|v12|v33|FRC|frc)"
        # Regex pattern for split cases (with split patterns)
        split_pattern = rf"(.+)_({SPLIT})_([^_]+)_({composite})_([^:]+)(?::(.+))?"
        std_pattern = rf"(.*)_({composite})_([^:]+)(?::(.+))?"

        # Prima estrarre i gruppi unici di TEST_TXT e TEST_NUM
        unique_groups = df.select(["TEST_TXT", "TEST_NUM"]).unique()

        # Convert to string for regex operations
        unique_groups = unique_groups.with_columns(
            pl.col("TEST_TXT").cast(pl.Utf8).alias("TEST_TXT_str")
        )

        # Creare una colonna binaria temporanea per identificare i casi split sui gruppi unici
        unique_groups = unique_groups.with_columns(
            pl.col("TEST_TXT_str").str.contains(split_pattern).alias("_is_split")
        )

        # Identificare i gruppi con più TEST_NUM per lo stesso TEST_TXT sui gruppi unici
        unique_groups = unique_groups.with_columns(
            (pl.col("TEST_NUM").n_unique().over("TEST_TXT") > 1).alias("_has_multiple")
        )

        # Creare sottogruppi solo per i record che necessitano processing
        groups_multiple = unique_groups.filter(pl.col("_has_multiple"))
        groups_single = unique_groups.filter(~pl.col("_has_multiple"))

        # Processare solo i sottogruppi con più TEST_NUM
        if len(groups_multiple) > 0:
            groups_multiple = groups_multiple.with_columns(
                [
                    # TEST_TXT: Ricostruire per i casi split, mantenere originale per STD
                    pl.when(pl.col("_is_split"))
                    .then(
                        pl.when(
                            pl.col("TEST_TXT_str")
                            .str.extract(split_pattern, 7)
                            .is_not_null()
                        )
                        .then(
                            # Caso con gruppo 7 presente: include il gruppo 7
                            pl.concat_str(
                                [
                                    pl.col("TEST_TXT_str")
                                    .str.extract(split_pattern, 1)
                                    .str.to_uppercase(),
                                    pl.lit(":"),
                                    pl.col("TEST_TXT_str").str.extract(
                                        split_pattern, 7
                                    ),
                                ]
                            )
                        )
                        .otherwise(
                            # Caso senza gruppo 7: solo prefix
                            pl.col("TEST_TXT_str")
                            .str.extract(split_pattern, 1)
                            .str.to_uppercase()
                        )
                    )
                    .otherwise(
                        pl.when(
                            pl.col("TEST_TXT_str")
                            .str.extract(std_pattern, 4)
                            .is_not_null()
                        )
                        .then(
                            pl.concat_str(
                                [
                                    pl.col("TEST_TXT_str")
                                    .str.extract(std_pattern, 1)
                                    .str.to_uppercase(),
                                    pl.lit(":"),
                                    pl.col("TEST_TXT_str").str.extract(std_pattern, 4),
                                ]
                            )
                        )
                        .otherwise(
                            # Caso senza gruppo 4: solo prefix
                            pl.col("TEST_TXT_str")
                            .str.extract(std_pattern, 1)
                            .str.to_uppercase()
                        )
                    )
                    .cast(pl.Categorical)
                    .alias("new_TEST_TXT"),
                    # SPLIT: Estrarre il valore split o "standard"
                    pl.when(pl.col("_is_split"))
                    .then(pl.col("TEST_TXT_str").str.extract(split_pattern, 4))
                    .otherwise(pl.lit("standard"))
                    .cast(pl.Categorical)
                    .alias("Split"),
                    # pltype: "SPLIT" o "STD"
                    pl.when(pl.col("_is_split"))
                    .then(pl.lit("SPLIT"))
                    .otherwise(pl.lit("STD"))
                    .cast(pl.Categorical)
                    .alias("pltype"),
                ]
            )

            # Applicare la logica di ranking e padding solo ai sottogruppi
            groups_multiple = groups_multiple.sort(["new_TEST_TXT", "TEST_NUM"])

            # Create temporary rank column (0-based like pandas)
            groups_multiple = groups_multiple.with_columns(
                pl.col("TEST_NUM")
                .rank(method="dense")
                .over("new_TEST_TXT")
                .cast(pl.Int32)
                .sub(1)
                .alias("_temp_rank")
            )

            # Calculate max rank per group to determine digit padding
            groups_multiple = groups_multiple.with_columns(
                pl.col("_temp_rank").max().over("new_TEST_TXT").alias("_max_rank")
            )

            # Calculate number of digits needed for padding
            groups_multiple = groups_multiple.with_columns(
                (pl.col("_max_rank") + 1)
                .cast(pl.String)
                .str.len_chars()
                .alias("_digits")
            )

            # Update Split and pltype columns for groups with multiple TEST_NUM
            groups_multiple = groups_multiple.with_columns(
                [
                    # Update Split column
                    pl.concat_str(
                        [
                            pl.lit("Code"),
                            pl.col("_temp_rank")
                            .cast(pl.String)
                            .str.pad_start(pl.col("_digits"), "0"),
                        ]
                    )
                    .cast(pl.Categorical)
                    .alias("Split"),
                    # Update pltype column
                    pl.lit("SPLIT").cast(pl.Categorical).alias("pltype"),
                ]
            )

            # Drop temporary columns
            groups_multiple = groups_multiple.drop(
                ["_temp_rank", "_max_rank", "_digits"]
            )

        # Processare i record singoli (senza modifiche sostanziali al TEST_TXT)
        if len(groups_single) > 0:
            groups_single = groups_single.with_columns(
                [
                    # Per i record singoli, manteniamo la logica di parsing ma senza ranking
                    pl.when(pl.col("_is_split"))
                    .then(
                        pl.when(
                            pl.col("TEST_TXT_str")
                            .str.extract(split_pattern, 7)
                            .is_not_null()
                        )
                        .then(
                            pl.concat_str(
                                [
                                    pl.col("TEST_TXT_str")
                                    .str.extract(split_pattern, 1)
                                    .str.to_uppercase(),
                                    pl.lit(":"),
                                    pl.col("TEST_TXT_str").str.extract(
                                        split_pattern, 7
                                    ),
                                ]
                            )
                        )
                        .otherwise(
                            pl.col("TEST_TXT_str")
                            .str.extract(split_pattern, 1)
                            .str.to_uppercase()
                        )
                    )
                    .otherwise(
                        pl.when(
                            pl.col("TEST_TXT_str")
                            .str.extract(std_pattern, 4)
                            .is_not_null()
                        )
                        .then(
                            pl.concat_str(
                                [
                                    pl.col("TEST_TXT_str")
                                    .str.extract(std_pattern, 1)
                                    .str.to_uppercase(),
                                    pl.lit(":"),
                                    pl.col("TEST_TXT_str").str.extract(std_pattern, 4),
                                ]
                            )
                        )
                        .otherwise(
                            pl.col("TEST_TXT_str")
                            .str.extract(std_pattern, 1)
                            .str.to_uppercase()
                        )
                    )
                    .cast(pl.Categorical)
                    .alias("new_TEST_TXT"),
                    # SPLIT: Estrarre il valore split o "Standard"
                    pl.when(pl.col("_is_split"))
                    .then(pl.col("TEST_TXT_str").str.extract(split_pattern, 4))
                    .otherwise(pl.lit("Standard"))
                    .cast(pl.Categorical)
                    .alias("Split"),
                    # pltype: "SPLIT" o "STD"
                    pl.when(pl.col("_is_split"))
                    .then(pl.lit("SPLIT"))
                    .otherwise(pl.lit("STD"))
                    .cast(pl.Categorical)
                    .alias("pltype"),
                ]
            )

        # Ricombinare i gruppi processati
        if len(groups_multiple) > 0 and len(groups_single) > 0:
            processed_groups = pl.concat([groups_multiple, groups_single])
        elif len(groups_multiple) > 0:
            processed_groups = groups_multiple
        else:
            processed_groups = groups_single

        # Rimuovere le colonne temporanee
        processed_groups = processed_groups.drop(
            ["_is_split", "_has_multiple", "TEST_TXT_str"]
        )

        df = df.drop("TEST_TXT")
        df = df.join(
            processed_groups.select(
                ["TEST_NUM", "new_TEST_TXT", "Split", "pltype"]
            ),
            on=["TEST_NUM"],
            how="left",
        )

        # Riassegnare TEST_TXT con il nuovo valore e convertire di nuovo a categorical
        df = df.with_columns(
            pl.col("new_TEST_TXT").cast(pl.Categorical).alias("TEST_TXT")
        ).drop("new_TEST_TXT")

    return df


def process_ptr_data(ptr_df: pl.DataFrame, composite: str) -> pl.DataFrame:
    """Process PTR data with all transformations."""
    if ptr_df.is_empty():
        return ptr_df

    print(HEAD, f"Name extract... ".ljust(150), end="\r", flush=True)

    # Parse test names
    is_ttime = "TTIME" in composite
    ptr_df = parse_test_names_regex(ptr_df, composite, is_ttime)

    # Process chart type and target
    print(HEAD, f"PTR Cleaning... ".ljust(150), end="\r", flush=True)

    # regex_pattern = r"(.*(:.*):.*)|(.*(:.*|DELTA.*))|(.*)"
    # ptr_df = ptr_df.with_columns(
    #     [
    #         pl.col("TARGET").str.extract(regex_pattern, 3).alias("FTYPE_1"),
    #         pl.col("TARGET").str.extract(regex_pattern, 5).alias("FTYPE_2"),
    #         pl.col("TARGET").str.extract(regex_pattern, 1).alias("new_target_1"),
    #         pl.col("TARGET").str.extract(regex_pattern, 3).alias("new_target_2"),
    #     ]
    # )

    # ptr_df = ptr_df.with_columns(
    #     [
    #         pl.coalesce(pl.col("FTYPE_1"), pl.col("FTYPE_2")).alias("FTYPE"),
    #         pl.coalesce(
    #             pl.col("new_target_1"), pl.col("new_target_2"), pl.lit("")
    #         ).alias("TARGET"),
    #     ]
    # )

    # # Update TEST_TXT
    # ptr_df = ptr_df.with_columns(
    #     (pl.col("TestName").str.to_uppercase() + pl.col("TARGET")).alias("TEST_TXT")
    # )

    # # Mark trim tests
    # ptr_df = ptr_df.with_columns(
    #     pl.when(pl.col("TARGET").str.contains("(?i)trim"))
    #     .then(pl.lit("TRIM"))
    #     .otherwise(pl.col("pltype"))
    #     .alias("pltype")
    # )

    # # Remove unwanted tests
    # ptr_df = ptr_df.filter(
    #     ~pl.col("TARGET").str.contains("TestTime|ttime|TTIME|log_ttime")
    # )

    # Rename columns and clean up
    column_renames = {
        "RESULT": "Value",
        "LO_LIMIT": "Low Limit",
        "HI_LIMIT": "High Limit",
        "UNITS": "Unit",
        "TEMPERATURE": "°C",
        "TEST_NUM": "TestNumber",
        "CORNER": "Corner",
        "TEST_TXT": "TestName",
    }

    ptr_df = ptr_df.rename(column_renames)
    columns_to_drop = [
        "LOT_ID",
        "TARGET",
        "FTYPE_1",
        "FTYPE_2",
        "new_target_1",
        "new_target_2",
    ]
    existing_columns = [col for col in columns_to_drop if col in ptr_df.columns]
    if existing_columns:
        ptr_df = ptr_df.drop(existing_columns)
    ptr_df = ptr_df.with_columns(pl.col("Split").fill_null("Standard"))

    return ptr_df


def process_ftr_data(ftr_df: pl.DataFrame, composite: str) -> pl.DataFrame:
    """Process FTR data with all transformations."""
    if ftr_df.is_empty():
        return ftr_df

    print(HEAD, f"FTR Processing... ".ljust(150), end="\r", flush=True)

    # Parse test names
    ftr_df = parse_test_names_regex(ftr_df, composite, False)

    column_renames = {
        "TEMPERATURE": "°C",
        "TEST_NUM": "TestNumber",
        "CORNER": "Corner",
        "TEST_TXT": "TestName",
    }

    ftr_df = ftr_df.rename(column_renames)
    # ftr_df = ftr_df.drop(["LOT_ID", "TARGET"])
    ftr_df = ftr_df.with_columns(pl.col("Split").fill_null("Standard"))

    # Create RESULT column (1 = PASS, 0 = FAIL)
    ftr_df = ftr_df.with_columns(
        pl.when(pl.col("TEST_FLG") == "00000000")
        .then(1)
        .when(pl.col("TEST_FLG") == "10000000")
        .then(0)
        .otherwise(None)
        .cast(pl.UInt8)
        .alias("RESULT")
    ).drop("TEST_FLG")

    return ftr_df.filter(pl.col("RESULT").is_not_null())


def process_single_corner_file(
    csv_file: str, corner_name: str, parameter: dict
) -> dict:
    """Process a single corner CSV file and return processed dataframes."""
    try:
        # Read all STDF files
        df_stdf = read_stdf_files(csv_file)

        # Extract temperature
        temperature = extract_temperature(df_stdf["mir"])
        print(
            HEAD,
            f"Store... {corner_name} at {temperature}°C".ljust(150),
            end="\r",
            flush=True,
        )

        # Load product data
        product_data = load_personalization_data(parameter["CODE"])

        # Filter test numbers
        test_numbers = filter_test_numbers(
            df_stdf["tsr"],
            parameter["COM"],
            product_data,
            parameter["FLOW"],
            parameter["MAIN"],
        )
        if not test_numbers:
            return None

        # Add corner and temperature to all dataframes
        for key in df_stdf:
            if not df_stdf[key].is_empty():
                df_stdf[key] = df_stdf[key].with_columns(
                    [
                        pl.lit(corner_name).alias("CORNER"),
                        pl.lit(temperature).alias("TEMPERATURE").cast(pl.Utf8),
                    ]
                )

        # Convert PARM_FLG from binary string to int
        df_stdf["ptr"] = df_stdf["ptr"].with_columns(
            pl.when(pl.col("PARM_FLG").is_not_null())
            .then(
                pl.col("PARM_FLG").cast(pl.String).str.to_integer(base=2, strict=False)
            )
            .otherwise(None)
            .cast(pl.UInt16)
            .alias("PARM_FLG")
        )
        df_stdf["ptr"] = df_stdf["ptr"].filter(pl.col("RESULT").is_not_null())

        # Filter PTR and FTR by test numbers
        if not df_stdf["ptr"].is_empty():
            df_stdf["ptr"] = df_stdf["ptr"].filter(
                pl.col("TEST_NUM").is_in(test_numbers)
            )

        if not df_stdf["ftr"].is_empty():
            df_stdf["ftr"] = df_stdf["ftr"].filter(
                pl.col("TEST_NUM").is_in(test_numbers)
            )

        # Process coordinate recalculation
        df_stdf["prr"] = process_coordinate_recalculation(
            df_stdf["ptr"], df_stdf["prr"], product_data, parameter["FLOW"]
        )

        # Apply scaling
        df_stdf["ptr"] = apply_result_scaling(df_stdf["ptr"])
        df_stdf["ptr"] = apply_unit_prefixes(df_stdf["ptr"])

        return {
            "data": df_stdf,
            "temperature": temperature,
            "corner": corner_name,
            "csv_file": csv_file,
        }

    except Exception as e:
        print(f"[ERROR] processing {csv_file}: {e}")
        return None


def consolidate_corner_data(corner_results: list) -> dict:
    """Consolidate data from all corners into single dataframes with memory optimization."""
    all_data = {key: [] for key in ["ptr", "ftr", "mir", "prr"]}

    for result in corner_results:
        if result is None:
            continue

        for key in all_data:
            if key in result["data"] and not result["data"][key].is_empty():
                all_data[key].append(result["data"][key])

    # Concatenate all dataframes with optimization
    consolidated = {}
    for key, data_list in all_data.items():
        print(HEAD, f"Merge... {key}".ljust(150), end="\r", flush=True)
        if data_list:
            # Prima concatena
            consolidated[key] = pl.concat(data_list, rechunk=True)

            # Ottimizza solo TEST_TXT se esiste
            if "TEST_TXT" in consolidated[key].columns:
                consolidated[key] = consolidated[key].with_columns(
                    pl.col("TEST_TXT").cast(pl.Categorical)
                )
        else:
            consolidated[key] = pl.DataFrame()

    return consolidated


def rework_stdf_multiple(parameter: dict, corner_folders: list) -> tuple:
    """Process multiple corner folders containing CSV files."""
    composite = parameter["COM"]
    corner_results = []

    # Process each corner folder
    for corner_folder in corner_folders:
        corner_name = os.path.basename(corner_folder).split("_")[-1]

        # Find all STD files in the corner folder
        std_files = glob.glob(os.path.join(corner_folder, "*.std"))

        for std_file in std_files:
            csv_file = os.path.join(
                os.path.dirname(std_file), "parquet", os.path.basename(std_file)
            )

            result = process_single_corner_file(csv_file, corner_name, parameter)
            if result:
                corner_results.append(result)

        if not corner_results:
            print(f"[CHAR] WARNING: No Data for {composite}".ljust(150))
            return {}, {}

    # Consolidate all data
    consolidated_data = consolidate_corner_data(corner_results)

    print(HEAD, f"Merge data... ".ljust(150), end="\r", flush=True)

    # Remove retests
    consolidated_data["prr"] = remove_retests(
        consolidated_data["prr"], parameter["TYPE"]
    )

    # Join coordinate information to PTR and FTR
    prr_coords = consolidated_data["prr"].select(
        [
            "PartID",
            "X_COORD",
            "Y_COORD",
            # "SOFT_BIN",
            # "HARD_BIN",
            "CORNER",
            "TEMPERATURE",
        ]
    )

    # if not consolidated_data["ptr"].is_empty():
    #     consolidated_data["ptr"] = consolidated_data["ptr"].join(
    #         prr_coords, on=["PartID", "CORNER", "TEMPERATURE"], how="inner"
    #     )

    #     for col in consolidated_data["ptr"].columns:
    #         dtype = consolidated_data["ptr"][col].dtype
    #         size_bytes = consolidated_data["ptr"][col].estimated_size()
    #         size_mb = size_bytes / (1024**2)
    #         print(f"Colonna '{col}': tipo={dtype}, memoria stimata={size_mb:.6f} MB")

    if not consolidated_data["ftr"].is_empty():
        ftr_coords = consolidated_data["prr"].select(
            ["PartID", "X_COORD", "Y_COORD", "CORNER", "TEMPERATURE"]
        )
        consolidated_data["ftr"] = consolidated_data["ftr"].join(
            ftr_coords, on=["PartID", "CORNER", "TEMPERATURE"], how="inner"
        )

        # Remove FTR retests
        consolidated_data["ftr"] = consolidated_data["ftr"].unique(
            subset=["X_COORD", "Y_COORD", "CORNER", "TEMPERATURE", "TEST_TXT"],
            keep="last",
        )

    # Process PTR and FTR data
    consolidated_data["ptr"] = process_ptr_data(consolidated_data["ptr"], composite)
    consolidated_data["ftr"] = process_ftr_data(consolidated_data["ftr"], composite)

    return parameter, consolidated_data


def get_ordered_corner_folders(main_folder: str) -> list:
    """Get corner folders in predefined order."""
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

    corner_folders = [
        os.path.join(main_folder, f)
        for f in os.listdir(main_folder)
        if os.path.isdir(os.path.join(main_folder, f))
    ]

    return sorted(
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


def run_report(parameter: dict, df_stdf: dict, path: str):
    """Generate reports from processed data."""
    ptrtname, ftrtname = gen_composite(parameter, df_stdf, path)
    stats = {}
    ptrtname = sorted(ptrtname)
    ftrtname = sorted(ftrtname)
    if ptrtname:
        for tname in ptrtname:
            try:
                stats[tname] = gen_ptr(tname, parameter, df_stdf, path)
            except Exception as e:
                print(f"{HEAD} Error in {tname}: {e}")

    if ftrtname:
        for tname in ftrtname:
            try:
                gen_ftr(tname, parameter, df_stdf, path)
            except Exception as e:
                print(f"{HEAD} Error in {tname}: {e}")
    
    gen_limits(stats, parameter, path)
    
    print(
        f"{HEAD} End report {parameter["COM"]}".ljust(150),
    )


def run(report_path: str, parameter: dict, composite: str, DEBUG: bool = False):
    """Main processing function."""
    # Load product data
    product_data = load_personalization_data(parameter["CODE"])
    parameter["PRODUCT"] = product_data.get("product_name", parameter.get("CODE", ""))

    print(
        f"{HEAD} Start {parameter['CUT']} {composite}".ljust(150), end="\r", flush=True
    )

    main_folder = report_path.split("CHAR")[0] + "CHAR"

    if not os.path.exists(main_folder):
        print(f"Main folder {main_folder} does not exist")
        return

    # Get ordered corner folders
    corner_folders = get_ordered_corner_folders(main_folder)
    print(HEAD, f"Found {len(corner_folders)} folders to process", end="\r", flush=True)

    # Process all corner data
    parameter, df_stdf = rework_stdf_multiple(parameter, corner_folders)

    if not parameter and not df_stdf:
        print(HEAD, "No test found... ".ljust(150), end="\r", flush=True)
        return

    print(HEAD, "Start Report generation... ".ljust(150), end="\r", flush=True)
    run_report(parameter, df_stdf, report_path)


def gen_mainmenu(parameter: dict, path: str):
    """Generate main menu."""
    gen_menu(parameter=parameter, destinationfolder=path)


def main(DEBUG: bool):
    """Example usage."""
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
        "AUTHOR": "Matteo Terranova",
        "MAIL": "matteo.terranova@st.com",
        "SITE": "Catania",
        "GROUP": "MDRF - EP - GPAM",
    }

    composite = "PMU"
    run(path, parameter, composite, DEBUG)


if __name__ == "__main__":
    DEBUG = True
    main(DEBUG)
