import os
import sys
import json
import datetime 
import polars as pl
from jupiter.utility import get_personalization

sys.path.append(os.path.join(os.path.dirname(__file__), "jupiter"))
import jupiter.utility as uty

sys.path.pop()

debug = False
FILENAME = os.path.abspath("src/run.log")


def power_of_10(value):
    return 10**value if value >= 0 else 1 / (10 ** abs(value))


def find_value(value, calc_type):
    if value == 0:
        return 0.1 if calc_type == "min" else -0.1
    elif value < 0:
        adjustment = value * 0.1
        return value - adjustment if calc_type == "min" else value + adjustment
    else:
        adjustment = value * 0.1
        return value + adjustment if calc_type == "min" else value - adjustment


def rework_stdf(parameter, df_stdf):
    composite = parameter["COM"]
    flwtp = parameter["TYPE"]
    ptr_dict = {}
    ftr_dict = {}
    uty.write_log("Rework STDF START", FILENAME)

    XY_XL = get_personalization(parameter, "XY_XL")
    XY_XH = get_personalization(parameter, "XY_XH")
    XY_YL = get_personalization(parameter, "XY_YL")
    XY_YH = get_personalization(parameter, "XY_YH")
    XY_Waf = get_personalization(parameter, "XY_Waf")
    XY_Lot0 = get_personalization(parameter, "XY_Lot0")
    XY_Lot1 = get_personalization(parameter, "XY_Lot1")
    XY_Lot2 = get_personalization(parameter, "XY_Lot2")
    XY_Lot3 = get_personalization(parameter, "XY_Lot3")
    XY_Lot4 = get_personalization(parameter, "XY_Lot4")
    XY_Lot5 = get_personalization(parameter, "XY_Lot5")
    XY_Lot6 = get_personalization(parameter, "XY_Lot6")
    xwafer = get_personalization(parameter, "xwafer")
    ywafer = get_personalization(parameter, "ywafer")

    # Conversione DataFrame Polars
    mir = df_stdf["mir"].clone()
    prr = df_stdf["prr"].clone()
    pcr = df_stdf["pcr"].clone()
    hbr = df_stdf["hbr"].clone()
    sbr = df_stdf["sbr"].clone()

    test_nums = (
        parameter["TEST_NUM"]
        if isinstance(parameter["TEST_NUM"], list)
        else [parameter["TEST_NUM"]]
    )

    # Filtraggio PTR e FTR
    tmpptr = df_stdf["ptr"].filter(pl.col("TEST_NUM").is_in(test_nums))
    tmpftr = df_stdf["ftr"].filter(pl.col("TEST_NUM").is_in(test_nums))

    # Calcolo temperatura
    temp_val = mir.select("TST_TEMP").to_series()[0]

    # Calcola la temperatura arrotondata o usa 30 se null
    temperature = 30 if temp_val is None else int(round(float(temp_val) / 5.0) * 5.0)

    # Aggiunta colonne comuni
    tmpptr = tmpptr.with_columns(
        [
            pl.lit(temperature).alias("TEMPERATURE"),
            pl.lit(mir.select("SBLOT_ID").to_series().to_list()[0]).alias("WAFER"),
            pl.lit(mir.select("LOT_ID").to_series().to_list()[0]).alias("LOT_ID"),
        ]
    )

    tmpftr = tmpftr.with_columns(
        [
            pl.lit(temperature).alias("TEMPERATURE"),
            pl.lit(mir.select("SBLOT_ID").to_series().to_list()[0]).alias("WAFER"),
            pl.lit(mir.select("LOT_ID").to_series().to_list()[0]).alias("LOT_ID"),
        ]
    )

    # Calcolo statistiche popolazione
    gross = (
        pcr.filter(pl.col("HEAD_NUM").cast(pl.Int32) == 255)
        .select("PART_CNT")
        .cast(pl.Int32)
        .to_series()
        .to_list()[0]
    )
    good_part = (
        pcr.filter(pl.col("HEAD_NUM").cast(pl.Int32) == 255)
        .select("GOOD_CNT")
        .cast(pl.Int32)
        .to_series()
        .to_list()[0]
    )
    yield_pct = f"{round((good_part * 100) / gross, 2)} %"
    population = pl.DataFrame(
        {
            "temperature": [temperature],
            "good_part": [good_part],
            "gross": [gross],
            "yield": [yield_pct],
        }
    )

    # Calcolo coordinate X/Y
    if "EWS" not in str(parameter["FLOW"]).upper():
        try:
            # 1. Filter XY test data and immediately cast to enforce types
            xy_tests = (
                tmpptr.filter(
                    pl.col("TEST_NUM").is_in([XY_XH, XY_XL, XY_YH, XY_YL, XY_Waf])
                )
                .select([
                    pl.col("PartID").cast(pl.Int64),  # Ensure 64-bit integer for joins
                    pl.col("TEST_NUM").cast(pl.Utf8),  # String type for filtering
                    pl.col("RESULT").cast(pl.Int32)  # 32-bit for bitshift operations
                ])
            )

            # 2. Calculate X coordinates
            # Split XH (high byte) and XL (low byte), rename to avoid post-join ambiguity
            xh = xy_tests.filter(pl.col("TEST_NUM") == XY_XH).select(
                ["PartID", "RESULT"]
            )
            xl = xy_tests.filter(pl.col("TEST_NUM") == XY_XL).select(
                ["PartID", "RESULT"]
            )

            combined_X = (
                xh.with_columns(pl.col("RESULT").alias("RESULT_H"))  # Rename high byte
                .join(
                    xl.with_columns(pl.col("RESULT").alias("RESULT_L")), on="PartID"
                )  # Join with low byte
                .with_columns(
                    (
                        pl.col("RESULT_H").cast(pl.Int32) * 256 + pl.col("RESULT_L").cast(pl.Int32)
                    )  # High << 8 + Low
                    .cast(pl.Int64)  # Convert to 64-bit for storage
                    .alias("X_COORD")
                )
                .select(["PartID", "X_COORD"])
            )

            # 3. Calculate Y coordinates (same logic as X)
            yh = xy_tests.filter(pl.col("TEST_NUM") == XY_YH).select(
                ["PartID", "RESULT"]
            )
            yl = xy_tests.filter(pl.col("TEST_NUM") == XY_YL).select(
                ["PartID", "RESULT"]
            )

            combined_Y = (
                yh.with_columns(pl.col("RESULT").alias("RESULT_H"))  # Rename high byte
                .join(
                    yl.with_columns(pl.col("RESULT").alias("RESULT_L")), on="PartID"
                )  # Join with low byte
                .with_columns(
                    (
                        pl.col("RESULT_H").cast(pl.Int32) * 256 + pl.col("RESULT_L").cast(pl.Int32)
                    )  # High << 8 + Low
                    .cast(pl.Int64)  # Convert to 64-bit for storage
                    .alias("Y_COORD")
                )
                .select(["PartID", "Y_COORD"])
            )

            # 4. Join coordinates to PRR dataframe and validate ranges
            # Ensure PartID type matches (Int64) before joining
            prr = (
                prr.with_columns(
                    pl.col("PartID").cast(pl.Int64)
                )  # Standardize join key type
                .join(combined_X, on="PartID", how="left")  # Add X coordinates
                .join(combined_Y, on="PartID", how="left")  # Add Y coordinates
                .with_columns(
                    [
                        # Validate X coordinate is within wafer bounds, else set to None
                        pl.when(
                            pl.col("X_COORD")
                            .cast(pl.Int64)
                            .is_between(
                                pl.lit(xwafer[0], dtype=pl.Int64),  # Min X bound
                                pl.lit(xwafer[1], dtype=pl.Int64),  # Max X bound
                            )
                        )
                        .then(pl.col("X_COORD"))
                        .otherwise(None)
                        .cast(pl.Int64)
                        .alias("X_COORD"),
                        # Validate Y coordinate is within wafer bounds, else set to None
                        pl.when(
                            pl.col("Y_COORD")
                            .cast(pl.Int64)
                            .is_between(
                                pl.lit(ywafer[0], dtype=pl.Int64),  # Min Y bound
                                pl.lit(ywafer[1], dtype=pl.Int64),  # Max Y bound
                            )
                        )
                        .then(pl.col("Y_COORD"))
                        .otherwise(None)
                        .cast(pl.Int64)
                        .alias("Y_COORD"),
                    ]
                )
            )

            # 5. Extract wafer ID and lot info with explicit type casting
            lot_tests = [
                XY_Waf,
                XY_Lot0,
                XY_Lot1,
                XY_Lot2,
                XY_Lot3,
                XY_Lot4,
                XY_Lot5,
                XY_Lot6,
            ]
            lot_data = tmpptr.filter(pl.col("TEST_NUM").is_in(lot_tests)).select(
                [
                    pl.col("TEST_NUM").cast(pl.Utf8),  # String for filtering
                    pl.col("RESULT").cast(pl.Float64),  # Float for mode() function
                ]
            )

            # Extract most frequent wafer ID
            parameter["EWSWAFER"] = str(
                int(
                    lot_data.filter(pl.col("TEST_NUM") == XY_Waf)
                    .select("RESULT")
                    .to_series()
                    .mode()[0]
                )
            )

            # Build lot string from 7 character codes
            lot_chars = []
            for test_num in [
                XY_Lot0,
                XY_Lot1,
                XY_Lot2,
                XY_Lot3,
                XY_Lot4,
                XY_Lot5,
                XY_Lot6,
            ]:
                mode_val = (
                    lot_data.filter(pl.col("TEST_NUM") == test_num)
                    .select("RESULT")
                    .to_series()
                    .mode()[0]
                )
                lot_chars.append(
                    chr(int(mode_val))
                )  # Convert numeric ASCII to character

            parameter["EWSLOT"] = "".join(lot_chars) + f" (FT lot {parameter['LOT']})"

        except Exception as e:
            print(f"ERROR: UID Test number wrong ({e})")
    else:
        sb_lot_id = mir.select("SBLOT_ID").to_series()[0]
        parameter["EWSWAFER"] = (
            str(sb_lot_id).rjust(2, "0")
            if sb_lot_id is not None
            else str(parameter["WAFER"]).rjust(2, "0")
        )
        lot_id = mir.select("LOT_ID").to_series()[0]
        parameter["EWSLOT"] = (
            str(lot_id) if lot_id is not None else str(parameter["LOT"])
        )

    # Rimozione retest
    if str(parameter["TYPE"]).upper() != "LOOP":
        prr = prr.unique(subset=["X_COORD", "Y_COORD"], keep="last")
        prr = prr.with_columns(pl.col("PartID").cast(pl.Utf8))

        if not tmpptr.height == 0:
            tmpptr = tmpptr.join(
                prr.select(["PartID", "X_COORD", "Y_COORD", "SOFT_BIN", "HARD_BIN"]),
                on="PartID",
                how="inner",
            )

        if not tmpftr.height == 0:
            tmpftr = tmpftr.join(
                prr.select(["PartID", "X_COORD", "Y_COORD"]), on="PartID", how="inner"
            )

    # Elaborazione PTR
    if not tmpptr.height == 0:
        # Rework RESULT SCALE
        uty.write_log("Result Scale", FILENAME)

        # Conversione PARM_FLG e calcolo RES_SCAL
        tmpptr = tmpptr.with_columns(
            pl.when(pl.col("PARM_FLG").is_not_null())
            .then(
                pl.col("PARM_FLG").cast(pl.String).str.to_integer(base=2, strict=False)
            )
            .cast(pl.UInt16)
            .alias("PARM_FLG")
        )
        if "TTIME" not in composite:
            res_scal_df = (
                tmpptr.with_columns(RES_SCAL_int=pl.col("RES_SCAL").cast(pl.Int64))
                .filter(
                    (pl.col("RES_SCAL_int") != 0)  # Integer comparison
                    & (
                        pl.col("RES_SCAL_int").is_in([2, 3, 6, 9, 12, 15, 18, -6, -9])
                    )  # Integer check
                )
                .group_by("TEST_TXT")
                .agg(
                    min_val=pl.col("RES_SCAL_int").min(),
                    max_val=pl.col("RES_SCAL_int").max(),
                )
                .select(
                    pl.col("TEST_TXT"),
                    pl.when(pl.col("min_val") > 0)
                    .then(pl.col("max_val"))
                    .when(pl.col("max_val") < 0)
                    .then(pl.col("min_val"))
                    .otherwise(0)
                    .alias("NEW_RES_SCAL"),
                )
            )

            # Unione con il DataFrame originale (rimane invariata)
            tmpptr = (
                tmpptr.join(res_scal_df, on="TEST_TXT", how="left")
                .with_columns(
                    pl.col("NEW_RES_SCAL")
                    .fill_null(pl.col("RES_SCAL"))
                    .alias("RES_SCAL")
                )
                .drop("NEW_RES_SCAL")
            )

            # Aggiornamento unità di misura
            unit_mapping = {
                3: "m",
                6: "u",
                9: "n",
                12: "p",
                15: "f",
                18: "a",
                -3: "K",
                -6: "M",
                -9: "G",
            }

            for scale, prefix in unit_mapping.items():
                tmpptr = tmpptr.with_columns(
                    pl.when(pl.col("RES_SCAL").cast(pl.Int64) == scale)
                    .then(prefix + pl.col("UNITS").cast(pl.Utf8))
                    .otherwise(pl.col("UNITS"))
                    .alias("UNITS")
                )

            # Applicazione scaling
            tmpptr = tmpptr.with_columns(
                [
                    (
                        pl.col("RESULT").cast(pl.Float32)
                        * (10 ** pl.col("RES_SCAL").cast(pl.Float32))
                    ).alias("RESULT"),
                    (
                        pl.col("HI_LIMIT").cast(pl.Float32)
                        * (10 ** pl.col("RES_SCAL").cast(pl.Float32))
                    )
                    .round(3)
                    .alias("HI_LIMIT"),
                    (
                        pl.col("LO_LIMIT").cast(pl.Float32)
                        * (10 ** pl.col("RES_SCAL").cast(pl.Float32))
                    )
                    .round(3)
                    .alias("LO_LIMIT"),
                ]
            )

        test = pl.DataFrame()
        testvdd = pl.DataFrame()

        uty.write_log("Split", FILENAME)
        if "TTIME" not in composite:
            SPLIT = "vio|vbt|v11|v12|v33|FRC|frc"
            vdd_regex = rf"(?P<TestName>.+)_(?P<SplitName>{SPLIT})_(?P<Split>[^_]+)_(?P<COM>{composite})_(?P<tmpfunc>[^:]+)(?::(?P<TARGET>.+))?"
            testvdd = (
                tmpptr.filter(pl.col("TEST_TXT").str.contains(vdd_regex))
                .with_columns(
                    pl.col("TEST_TXT").str.extract_groups(vdd_regex).alias("extracted")
                )
                .unnest("extracted")
                .with_columns(pl.lit("BPLVDD").alias("pltype"))
            ).drop(["SplitName", "tmpfunc"])

            tmpptr = tmpptr.filter(~pl.col("TEST_TXT").str.contains(vdd_regex))

            # Regex per test standard
            std_regex = f"(?P<TestName>.*)_(?P<COM>{composite})_(?P<TARGET>.*)"
            test = (
                tmpptr.filter(pl.col("TEST_TXT").str.contains(std_regex))
                .with_columns(
                    pl.col("TEST_TXT").str.extract_groups(std_regex).alias("extracted")
                )
                .unnest("extracted")
                .with_columns(pl.lit("BPLTEMP").alias("pltype"))
                .with_columns(pl.lit("Standard").alias("Split"))
            )
            split_series = test.get_column("Split")
            test = test.drop("Split")
        else:
            # Regex per test TTIME
            ttime_regex = f"(?P<COM>log_ttime)__(?P<TestName>.*)::(?P<TARGET>.*)"
            test = (
                tmpptr.filter(pl.col("TEST_TXT").str.contains(ttime_regex))
                .with_columns(
                    pl.col("TEST_TXT")
                    .str.extract_groups(ttime_regex)
                    .alias("extracted")
                )
                .unnest("extracted")
                .with_columns(pl.lit("BPLTEMP").alias("pltype"))
                .with_columns(pl.lit("Standard").alias("Split"))
            )

        uty.write_log("PTR Split done", FILENAME)

        # Combinazione e pulizia dati
        if test.height == 0:
            test = pl.DataFrame(schema=testvdd.schema)
        elif testvdd.height == 0:
            testvdd = pl.DataFrame(schema=test.schema)

        clearptr = pl.concat([test, testvdd], how="align")

        if not clearptr.height == 0:
            # Estrazione tipo test
            clearptr = clearptr.with_columns(
                pl.when(pl.col("TARGET").str.contains("Trim"))
                .then(pl.lit("TRIM"))
                .otherwise(pl.col("pltype"))
                .alias("pltype")
            ).filter(~pl.col("TARGET").str.contains("TestTime|ttime"))

            clearptr = clearptr.with_columns(
                (
                    pl.col("TestName")
                    + ":"
                    + pl.col("TARGET").str.split(":").list.get(-1)
                ).alias("TestName")
            )
            # Rinomina colonne
            clearptr = clearptr.rename(
                {
                    "RESULT": "Value",
                    "LO_LIMIT": "Low Limit",
                    "HI_LIMIT": "High Limit",
                    "UNITS": "Unit",
                    "TEMPERATURE": "°C",
                    "TEST_NUM": "TestNumber",
                    "TestName": "TestName",
                }
            ).drop(
                [
                    "LOT_ID",
                    "TARGET",
                    "TestNumber",
                    "RES_SCAL",
                    "LLM_SCAL",
                    "HLM_SCAL",
                ]
            )

            ptr_dict[parameter["MAIN"]] = clearptr

    uty.write_log("END PTR", FILENAME)

    # Elaborazione FTR
    if not tmpftr.height == 0:
        # Regex per test VDD
        SPLIT = "vio|vbt|v11|v12|v33|FRC|frc"
        vdd_regex = rf"(?P<TestName>.+)_(?P<SplitName>{SPLIT})_(?P<Split>[^_]+)_(?P<COM>{composite})_(?P<tmpfunc>[^:]+)(?::(?P<TARGET>.+))?"
        testvdd = (
            tmpftr.filter(pl.col("TEST_TXT").str.contains(vdd_regex))
            .with_columns(
                pl.col("TEST_TXT").str.extract_groups(vdd_regex).alias("extracted")
            )
            .unnest("extracted")
            .with_columns(pl.lit("BPLVDD").alias("pltype"))
        ).drop(["SplitName", "tmpfunc"])

        tmpftr = tmpftr.filter(~pl.col("TEST_TXT").str.contains(vdd_regex))

        # Regex per test standard
        std_regex = f"(?P<TestName>.*)_(?P<COM>{composite})_(?P<TARGET>.*)"
        test = (
            tmpftr.filter(pl.col("TEST_TXT").str.contains(std_regex))
            .with_columns(
                pl.col("TEST_TXT").str.extract_groups(std_regex).alias("extracted")
            )
            .unnest("extracted")
            .with_columns(pl.lit("BPLTEMP").alias("pltype"))
            .with_columns(pl.lit("Standard").alias("Split"))
        )
        split_series = test.get_column("Split")
        test = test.drop("Split")

        # Combinazione e pulizia dati
        clearftr = pl.concat([test, testvdd], how="align")

        if not clearftr.height == 0:
            # Conversione flag test
            clearftr = clearftr.with_columns(
                pl.when(pl.col("TEST_FLG") == "00000000")
                .then(1)
                .when(pl.col("TEST_FLG") == "10000000")
                .then(0)
                .otherwise(None)
                .cast(pl.UInt8)
                .alias("RESULT")
            ).drop("TEST_FLG")

            # Rinomina colonne
            clearftr = clearftr.rename(
                {
                    "TEMPERATURE": "°C",
                    "TEST_NUM": "TestNumber",
                    "TestName": "TestName",
                }
            ).drop(["LOT_ID", "TARGET", "TestNumber"])

            ftr_dict[parameter["MAIN"]] = clearftr

    uty.write_log("Write csv for jupiter", FILENAME)

    # Combinazione risultati e salvataggio
    ptr = pl.concat(ptr_dict.values()) if ptr_dict else pl.DataFrame()
    ftr = pl.concat(ftr_dict.values()) if ftr_dict else pl.DataFrame()

    os.makedirs("./src/jupiter/tmp", exist_ok=True)
    ptr.write_parquet(os.path.abspath("./src/jupiter/tmp/ptr.parquet"))
    ftr.write_parquet(os.path.abspath("./src/jupiter/tmp/ftr.parquet"))

    return parameter


def main():
    try:
        with open("src/jupiter/cfg.json", "r") as file:
            content = file.read()
            parameter = (
                json.loads(content)
                if content.strip()
                else {
                    "TITLE": "MBIST",
                    "COM": "mbist",
                    "FLOW": "EWS",
                    "TYPE": "STD",
                    "PRODUCT": "Mosquito",
                    "CODE": "44E",
                    "LOT": "P6AX86",
                    "WAFER": "1",
                    "Author": "Matteo Terranova",
                    "Mail": "matteo.terranova@st.com",
                    "Cut": "2.1",
                    "Site": "Catania",
                    "stdf": "example.com",
                    "RUN": "1",
                    "TEST_NUM": ["80003000", "80004000"],
                    "MAIN": "r44exxxz_q443616_04_st44ez-t2kf1_e_ews1_tat2k06_20250301214005.std",
                }
            )
    except Exception as e:
        print(f"Error loading config: {e}")
        return

    # Simulazione df_stdf (da sostituire con dati reali)
    df_stdf = {
        "ptr": pl.DataFrame(),
        "ftr": pl.DataFrame(),
        "mir": pl.DataFrame(),
        "prr": pl.DataFrame(),
        "pcr": pl.DataFrame(),
        "hbr": pl.DataFrame(),
        "sbr": pl.DataFrame(),
    }

    rework_stdf(parameter, df_stdf)
    
    


if __name__ == "__main__":
    last_timestamp = datetime.datetime.now()
    main()
