import os
import json
import fileinput
import subprocess
import numpy as np
import pandas as pd

from bs4 import BeautifulSoup
from io import StringIO

file_path = ".\\STDF\\anaflow_VAL_ST44EZ_T2KF1_0007.html"


def detect_separator(file_path, max_lines=10):
    with open(file_path, "r", encoding="utf-8") as file:
        for line_number in range(max_lines):
            line = file.readline()
            if not line:
                break  # Fine del file raggiunta
            if "," in line and ";" in line:
                raise ValueError("Both ',' and ';' found in the line.")
            elif "," in line:
                return ",", line_number - 1
            elif ";" in line:
                return ";", line_number - 1
        raise ValueError(
            "No valid separator found in the first {} lines.".format(max_lines)
        )


def read_csv_to_dataframe(file_path):
    separator, header_line = detect_separator(file_path)
    df = pd.read_csv(file_path, sep=separator, header=header_line)
    return df


# Esempio di utilizzo


def condition_rework(parameter, directory_path):
    if not directory_path.endswith(".csv"):
        for filename in os.listdir(directory_path):
            if filename.endswith(".csv"):
                file_path = os.path.join(directory_path, filename)
                break
    else:
        file_path = directory_path

    df = read_csv_to_dataframe(file_path)
    df.columns = df.columns.str.upper().str.replace(" ", "")

    df = df[df["TAG"].replace("", np.nan).fillna(0).astype(int) == 1]
    df = df[df["BYP"].replace("", np.nan).fillna(0).astype(int) == 0]

    def is_all_nan_or_empty(row):
        return row[1:].isnull().all() or (row[1:] == "").all()

    df = df[~df.apply(is_all_nan_or_empty, axis=1)]
    df[["HB", "TESTNR"]] = (
        df[["HB", "TESTNR"]].replace("", np.nan).fillna(0).astype(int)
    )
    df["TESTSUITE"] = df["TESTSUITE"].str.upper()
    df["HBNAME"] = df["HBNAME"].str.replace("_", "")
    df = df[~df["TESTSUITE"].str.contains("TTIME")]

    df["TESTSUITE"] = df.apply(
        lambda row: row["TESTSUITE"].replace("_"+row["COMP"].upper(), "_").split("__")[0],
        axis=1,
    )
    df = df.drop(
        columns=[
            "Ext",
            "BYP",
            "TAG",
            "SB",
            "SBNAME",
            "GROUPBIN",
            "GROUPNAME",
        ],
        errors="ignore",
    )
    df.reset_index(drop=True, inplace=True)

    df_comp = df[df["COMP"] == parameter["COM"]]
    filename = f"./src/tmp/condition.csv"
    df_comp.to_csv(filename, index=False)

    return filename


def main():

    parameter = {
        "TITLE": "MBIST",
        "COM": "mbist",
        "FLOW": "EWS1",
        "TYPE": "STD",
        "PRODUCT": "Mosquito512K",
        "CODE": "44E",
        "LOT": "P6AX86",
        "WAFER": "1",
        "AUTHOR": "Matteo Terranova",
        "MAIL": "matteo.terranova@st.com",
        "GROUP": "MDRF - EP - GPAM",
        "Cut": "2.1",
        "SITE": "Catania",
        "REVISION": "0.1",
        "stdf": "example.com",
        "RUN": "1",
        "TEST_NUM": ["80003000", "80004000"],
        "CSV": "r44exxxz_q443616_04_st44ez-t2kf1_e_ews1_tat2k06_20250301214005.std",
    }

    for comp_value in df["Comp"].unique():
        df_comp = df[df["Comp"] == comp_value]
        filename = f"./src/tmp/condition.csv"
        df_comp.to_csv(filename, index=False)
        parameter["TITLE"] = comp_value
        parameter["COM"] = comp_value
        parameter["TYPE"] = "CONDITION"
        parameter["FLOW"] = "EWS1"
        print(comp_value)
        cfgfile = f"./src/jupiter/cfg.json"
        try:
            # Convert any Series objects in the parameter dictionary to lists
            parameter = {
                k: v.tolist() if isinstance(v, pd.Series) else v
                for k, v in parameter.items()
            }

            with open(cfgfile, mode="wt", encoding="utf-8") as file:
                json.dump(parameter, file, indent=4)
        except Exception as e:
            print(f"Error writing the configuration file: {e}")

        str_output = (
            parameter["TITLE"]
            + " "
            + parameter["FLOW"]
            + "_"
            + parameter["TYPE"].lower()
        )
        dir_output = os.path.abspath(
            os.path.join(
                "Report",
                f"{parameter['PRODUCT']}",
                parameter["FLOW"],
                parameter["TYPE"].upper(),
            )
        )
        if not os.path.exists(dir_output):
            os.makedirs(dir_output)

        cmd = f'jupyter nbconvert --execute --no-input --to html --output "{dir_output}/{str_output}" ./src/jupiter/{str(parameter["TYPE"]).upper()}.ipynb'
        if (
            subprocess.call(
                args=cmd,
                shell=True,
                stdout=subprocess.DEVNULL,
            )
            == 0
        ):
            pass
        else:
            print(f"ERROR: execution failed {cmd}")
