import os
import sys
import json
import datetime
import numpy as np
import pandas as pd

sys.path.append(os.path.join(os.path.dirname(__file__), "jupiter"))
import jupiter.utility as uty

sys.path.pop()

debug = False

FILENAME = os.path.abspath("src/run.log")


def power_of_10(value):
    if value >= 0:
        return 10**value
    else:
        return 1 / (10 ** abs(value))


def find_value(value, calc_type):
    if value == 0:
        if calc_type == "min":
            min_value = -0.1
            # print(f"Valore attuale: {value} Minimo: {min_value}")
            return 0.1
        elif calc_type == "max":
            max_value = 0.1
            # print(f"Valore attuale: {value} Massimo: {max_value}")
            return -0.1
    elif value < 0:
        if calc_type == "min":
            min_value = value - (value * 0.1)
            # print(f"Valore attuale: {value} Minimo: {min_value}")
            return value - (value * 0.1)
        elif calc_type == "max":
            max_value = value + (value * 0.1)
            # print(f"Valore attuale: {value} Massimo: {max_value}")
            return value + (value * 0.1)
    else:
        if calc_type == "min":
            min_value = value + (value * 0.1)
            # print(f"Valore attuale: {value} Minimo: {min_value}")
            return value + (value * 0.1)
        elif calc_type == "max":
            max_value = value - (value * 0.1)
            # print(f"Valore attuale: {value} Massimo: {max_value}")
            return value - (value * 0.1)


def rework_stdf(parameter,df_stdf):
    # print(parameter)
    composite = parameter["COM"]
    flwtp = parameter["TYPE"]
    groosgood30 = 0
    p = os.listdir()
    ptr = pd.DataFrame()
    ftr = pd.DataFrame()
    population = pd.DataFrame()
    ptr_dict = {}
    ftr_dict = {}
    ptrtname = []
    ftrtname = []
    uty.write_log("Rework STDF START", FILENAME)
    pd.set_option("display.width", None)

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

    base_path=os.path.dirname(parameter['CSV'])
    
    # ----------==================================================---------- #
    # Read extracted file
    # ----------==================================================---------- #
    test_nums = (
        parameter["TEST_NUM"]
        if isinstance(parameter["TEST_NUM"], list)
        else [parameter["TEST_NUM"]]
    )
    # PTR Parametric Test Record
    tmpptr = df_stdf["ptr"][df_stdf["ptr"]["TEST_NUM"].isin(test_nums)].copy()

    # FTR Functional Test Record
    tmpftr = df_stdf["ftr"][df_stdf["ftr"]["TEST_NUM"].isin(test_nums)].copy()

    # MIR Master Information Record
    mir = df_stdf["mir"].copy()
    # PRR Part Results Record
    prr = df_stdf["prr"].copy()
    # PCR Part Count Record
    pcr = df_stdf["pcr"].copy()
    # HBR Hardware Bin Record
    hbr = df_stdf["hbr"].copy()
    # SBR Software Bin Record
    sbr = df_stdf["sbr"].copy()

    # ----------==================================================---------- #
    uty.write_log("Extract general inforamtion", FILENAME)
    # ----------==================================================---------- #
    # round to nearest 5 temperature
    # ----------==================================================---------- #

    if str(mir["TST_TEMP"].iloc[0]) == "nan":
        mir["TST_TEMP"] = "30"

    temperature = int(round(float(mir["TST_TEMP"].iloc[0]) / 5.0) * 5.0)
    tmpptr["TEMPERATURE"] = temperature
    tmpftr["TEMPERATURE"] = temperature

    # ----------==================================================---------- #
    tmpptr["WAFER"] = mir.SBLOT_ID[0]
    tmpftr["WAFER"] = mir.SBLOT_ID[0]
    tmpptr["LOT_ID"] = mir.LOT_ID[0]
    tmpftr["LOT_ID"] = mir.LOT_ID[0]
    # ----------==================================================---------- #

    # ----------==================================================---------- #
    gross = pcr[pcr["HEAD_NUM"] == 255]["PART_CNT"].values[0]
    goodPart = pcr[pcr["HEAD_NUM"] == 255]["GOOD_CNT"].values[0]
    ye = str(round(((goodPart * 100) / gross), 2)) + " %"
    population[temperature] = [goodPart, gross, ye]
    # print(ptr)

    # ----------==================================================---------- #
    # Recalculate X_COORD Y_COORD
    # ----------==================================================---------- #
    try:
        if "EWS" not in str(parameter["FLOW"]).upper():
            # Convert float values to integers before performing bitwise shift
            combined_X = (
                tmpptr[tmpptr["TEST_NUM"] == XY_XH]
                .set_index("PartID")["RESULT"]
                .astype(int)
                .apply(lambda x: x << 8)
            ) + tmpptr[tmpptr["TEST_NUM"] == XY_XL].set_index("PartID")["RESULT"].astype(
                int
            )

            combined_Y = (
                tmpptr[tmpptr["TEST_NUM"] == XY_YH]
                .set_index("PartID")["RESULT"]
                .astype(int)
                .apply(lambda x: x << 8)
            ) + tmpptr[tmpptr["TEST_NUM"] == XY_YL].set_index("PartID")["RESULT"].astype(
                int
            )

            # Map the combined results to the prr DataFrame
            prr["X_COORD"] = prr["PartID"].map(combined_X)
            prr["Y_COORD"] = prr["PartID"].map(combined_Y)

            # Apply the range check and set NaN if out of range
            prr["X_COORD"] = prr["X_COORD"].apply(
                lambda x: x if xwafer[0] <= x <= xwafer[1] else np.nan
            )
            prr["Y_COORD"] = prr["Y_COORD"].apply(
                lambda y: y if ywafer[0] <= y <= ywafer[1] else np.nan
            )

            parameter["EWSWAFER"] = str(
                int(tmpptr[tmpptr["TEST_NUM"] == XY_Waf]["RESULT"].mode().iloc[0])
            )

            value = "".join(
                chr(int(tmpptr[tmpptr["TEST_NUM"] == var]["RESULT"].mode().iloc[0]))
                for var in [XY_Lot0, XY_Lot1, XY_Lot2, XY_Lot3, XY_Lot4, XY_Lot5, XY_Lot6]
            )
            parameter["EWSLOT"] = value + " (FT lot " + parameter["LOT"] + ")"
        else:
            parameter["EWSWAFER"] = str(mir.SBLOT_ID[0]).rjust(2, '0') if not pd.isna(mir.SBLOT_ID[0]) else str(parameter["WAFER"]).rjust(2, '0')
            parameter["EWSLOT"] = str(mir.LOT_ID[0]) if not pd.isna(mir.LOT_ID[0]) else str(parameter["LOT"])


    except Exception as e:
        print(f"ERROR: UID Test number wrong ({e})")

    # ----------==================================================---------- #

    # ----------==================================================---------- #
    # Remove retest
    # ----------==================================================---------- #
    if str(parameter["TYPE"]).upper() != "X30":
        prr = prr.drop_duplicates(subset=["X_COORD", "Y_COORD"], keep="last")
        # ----------==================================================---------- #
        if not tmpptr.empty:
            tmpptr = tmpptr.merge(
                prr[["PartID", "X_COORD", "Y_COORD", "SOFT_BIN", "HARD_BIN"]],
                how="inner",
                on="PartID",
            )
        if not tmpftr.empty:
            tmpftr = tmpftr.merge(
                prr[["PartID", "X_COORD", "Y_COORD"]],
                how="inner",
                on="PartID",
            )
        # ----------==================================================---------- #
        # Print retest
        # print(corner,str(mir.SBLOT_ID[0]),str(int(
        #     round(float(mir["TST_TEMP"].iloc[0]) / 5.0) * 5.0
        # )), tmpptr['PartID'].max()-prr.shape[0])

    # ----------==================================================---------- #
    # Remove ususefull Test
    # ----------==================================================---------- #
    # if not tmpptr.empty:
    #     tmpptr = tmpptr[~tmpptr["TEST_TXT"].str.startswith(("R_", "log"))]
    #     tmpptr = tmpptr[
    #         ~tmpptr["TEST_TXT"].str.contains("OTPWord|TestTime|XId|YId|WaferId")
    #     ]
    # if not tmpftr.empty:
    #     tmpftr = tmpftr[~tmpftr["TEST_TXT"].str.startswith(("R_", "log"))]
    #     tmpftr = tmpftr[
    #         ~tmpftr["TEST_TXT"].str.contains("OTPWord|TestTime|XId|YId|WaferId")
    #     ]

    # ----------==================================================---------- #
    # FIX ERROR
    # ----------==================================================---------- #

    # if composite == "scan":
    #     tmpptr = tmpptr[~tmpptr["TEST_TXT"].str.contains("Stress")]
    # if composite == "RB":
    #     tmpptr = tmpptr[~tmpptr["TEST_TXT"].str.contains("diff", case=False)]
    # if composite == "RNG":
    #     tmpptr = tmpptr[~tmpptr["TEST_TXT"].str.contains("div", case=False)]
    # if composite == "Register_Test":
    #     tmpptr.loc[tmpptr["TEST_NUM"] == 90020000, "HI_LIMIT"] = 1.5
    #     tmpptr.loc[tmpptr["TEST_NUM"] == 90020000, "LO_LIMIT"] = 0.5

    # ----------==================================================---------- #
    # ----------==================================================---------- #
    # Data Paipeline cleaning
    # ----------==================================================---------- #

    # ----------==================================================---------- #
    # Inizialize variable
    # testv33 = pd.DataFrame()
    testvdd = pd.DataFrame()
    testv11 = pd.DataFrame()
    test = pd.DataFrame()
    # ----------==================================================---------- #

    # ----------==================================================---------- #
    # START CLEANING PARAMETRIC TEST RECORD
    if not tmpptr.empty:

        # ----------==================================================---------- #
        # Rework RESULT SCALE
        if not tmpptr.empty:

            uty.write_log("Result Scale", FILENAME)

            tmpptr["PARM_FLG"] = (
                tmpptr["PARM_FLG"].astype(str).apply(lambda x: int(x, 2))
            )

            tesnames = tmpptr["TEST_TXT"].unique()

            def custom_res_scal(group):
                # Combina i valori delle tre colonne in una Serie.
                combined = pd.concat(
                    [group["RES_SCAL"], group["LLM_SCAL"], group["HLM_SCAL"]]
                )
                combined = combined[combined != 0]
                valid_values = [2, 3, 6, 9, 12, 15, 18, -6, -9]
                combined = combined[combined.isin(valid_values)]

                if combined.empty:
                    return 0

                if all(combined > 0):
                    return combined.max()
                elif all(combined < 0):
                    return combined.min()
                else:
                    return 0

            for tesname in tesnames:
                mask = tmpptr["TEST_TXT"] == tesname
                tmpptr.loc[mask, "RES_SCAL"] = custom_res_scal(tmpptr[mask])

            # Cast to string before concatenation
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
            tmpptr.loc[tmpptr["RES_SCAL"] == 15, "UNITS"] = (
                "f" + tmpptr.loc[tmpptr["RES_SCAL"] == 15, "UNITS"]
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

            tmpptr["RESULT"] = tmpptr["RESULT"].astype(float)
            tmpptr["RESULT"] = tmpptr["RESULT"] * tmpptr["RES_SCAL"].apply(power_of_10).astype(float)

            tmpptr["HI_LIMIT"] = tmpptr["HI_LIMIT"].astype(float)
            tmpptr["HI_LIMIT"] = round(
                tmpptr["HI_LIMIT"] * tmpptr["RES_SCAL"].apply(power_of_10), 3
            ).astype(float)

            tmpptr["LO_LIMIT"] = tmpptr["LO_LIMIT"].astype(float)
            tmpptr["LO_LIMIT"] = round(
                tmpptr["LO_LIMIT"] * tmpptr["RES_SCAL"].apply(power_of_10), 3
            ).astype(float)

        # ----------==================================================---------- #

        uty.write_log("Split VDD", FILENAME)

        if "TTIME" not in composite:

            # ----------==================================================---------- #
            # SPLIT VDD TESTS
            regex = f"(?P<TestName>.*)(_vio_|_vbt_|_v11_)(?P<VDD>[^_]+)_(?P<COM>{composite})_(?P<TARGET>.*)"
            # work in a copy dataframe
            testvdd = tmpptr.loc[tmpptr["TEST_TXT"].str.match(regex)].copy()
            # execute split test name
            testvdd[["TestName", "tmpvdd", "Volt", "COM", "TARGET"]] = testvdd[
                "TEST_TXT"
            ].str.extract(regex, expand=True)
            # remove all unusefull test
            testvdd = testvdd.dropna(subset=["TestName"])
            testvdd["pltype"] = "BPLVDD"
            # remove in test all
            if not testvdd.empty:
                tmpptr = tmpptr.loc[~tmpptr["TEST_TXT"].str.match(regex)]
            # print(testvdd)
            # ----------==================================================---------- #

            # ----------==================================================---------- #
            # SPLIT STANDARD TEST
            regex = f"(?P<TestName>.*)_(?P<COM>{composite})_(?P<TARGET>.*)"
            test = tmpptr.copy()
            test[["TestName", "COM", "TARGET"]] = test["TEST_TXT"].str.extract(
                regex, expand=True
            )
            test["pltype"] = "BPLTEMP"
            test = test[test["COM"].notna()]
            # uty.write_log("Rework FTR all test done", FILENAME)
            # ----------==================================================---------- #

        else:
            # ----------==================================================---------- #
            # SPLIT STANDARD TEST
            regex = f"(?P<COM>log_ttime)__(?P<TestName>.*)::(?P<TARGET>.*)"
            test = tmpptr.copy()
            test[["COM", "TestName", "TARGET"]] = test["TEST_TXT"].str.extract(
                regex, expand=True
            )
            test["pltype"] = "BPLTEMP"
            test = test[test["COM"].notna()]
            # uty.write_log("Rework FTR all test done", FILENAME)
            # ----------==================================================---------- #

        uty.write_log("PTR Split VDD done", FILENAME)

        # ----------==================================================---------- #
        # Choosing the Chart Type
        clearptr = pd.concat([test, testvdd])

        if not clearptr.empty:
            regex = "(.*(:.*):.*)|(.*(:.*|DELTA.*))|(.*)"
            # clearptr[["tmp", "TARGET", "FTYPE"]] = clearptr["TARGET"].str.extract(
            #     regex, expand=True
            # )
            extracted = clearptr["TARGET"].str.extract(regex, expand=True)

            # Combina i risultati in una singola colonna temporanea
            clearptr["tmp"] = (
                extracted[0].combine_first(extracted[2]).combine_first(extracted[4])
            )

            # Estrazione di TARGET e FTYPE
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
            clearptr.fillna({"Volt": "Standard"}, inplace=True)
            ptr_dict[parameter["CSV"]] = clearptr
            # ptrtname = clearptr["TestName"].unique()
            # uty.write_log("PTR all done", FILENAME)
        # ----------==================================================---------- #

        # ----------==================================================---------- #
        # CLEANING SHEREDE VAR
        testvdd = pd.DataFrame()
        test = pd.DataFrame()
        # ----------==================================================---------- #

    uty.write_log("END PTR", FILENAME)

    # ----------==================================================---------- #
    if not tmpftr.empty:

        # ----------==================================================---------- #
        # SPLIT VDD TESTS
        regex = f"(?P<TestName>.*)(_vio_|_vbt_|_v11_)(?P<VDD>[^_]+)_(?P<COM>{composite})_(?P<TARGET>.*)"
        testvdd = tmpftr.loc[tmpftr["TEST_TXT"].str.match(regex)].copy()
        testvdd[["TestName", "tmpvdd", "Volt", "COM", "TARGET"]] = testvdd[
            "TEST_TXT"
        ].str.extract(regex, expand=True)
        testvdd["pltype"] = "BPLVDD"
        if not testvdd.empty:
            tmpftr = tmpftr.loc[~tmpftr["TEST_TXT"].str.match(regex)]
        # uty.write_log("Rework ftr Vdd done", FILENAME)
        # ----------==================================================---------- #

        # ----------==================================================---------- #
        # SPLIT STANDARD TEST
        regex = f"(?P<TestName>.*)_(?P<COM>{composite})_(?P<TARGET>.*)"
        test = tmpftr.copy()
        test[["TestName", "COM", "TARGET"]] = test["TEST_TXT"].str.extract(
            regex, expand=True
        )
        test["pltype"] = "BPLTEMP"
        test = test[test["COM"].notna()]
        # uty.write_log("Rework FTR all test done", FILENAME)
        # ----------==================================================---------- #

        # ----------==================================================---------- #

        # clearftr = pd.concat(
        #     [testvdd]  # ONLY SPECIAL VDD ARE COMPUTED, STD TEST ARE IGNORED
        # )
        clearftr = pd.concat([test, testvdd])

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
            clearftr = clearftr.drop(["LOT_ID", "TARGET", "tmpvdd"], axis=1)
            clearftr.fillna({"Volt": "Standard"}, inplace=True)
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
            # uty.write_log("FTR all done", FILENAME)
        # ----------==================================================---------- #

    uty.write_log("Write csv for jupiter", FILENAME)

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

    ptr.to_csv(os.path.abspath("./src/jupiter/tmp/ptr.csv"), index=False)
    ftr.to_csv(os.path.abspath("./src/jupiter/tmp/ftr.csv"), index=False)

    return parameter
    # uty.write_log("Rework STDF DONE", FILENAME)


def main():
    import json

    parameter = {
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
        "CSV": "r44exxxz_q443616_04_st44ez-t2kf1_e_ews1_tat2k06_20250301214005.std",
    }

    with open("src/jupiter/cfg.json", "r") as file:
        content = file.read()
        if not content.strip():
            pass
        parameter = json.loads(content)

    rework_stdf(parameter)


if __name__ == "__main__":
    last_timestamp = datetime.datetime.now()
    main()
