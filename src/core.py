import os
import json
import warnings
import datetime
import subprocess
import pandas as pd
import jupiter.utility as uty
from rework_stdf import rework_stdf
from condition import condition_rework

warnings.filterwarnings("ignore", category=RuntimeWarning)

FILENAME = os.path.abspath("src/run.log")

def process_composite(parameter, csv_name,df_stdf):
    """
    Process the composite data from a CSV file and execute the report generation.

    Args:
        parameter (dict): Parameters for processing.
        csv_name (str): CSV file name to process.
    """
    try:
        tsr = pd.read_csv(os.path.abspath(f"{csv_name}.tsr.csv"))

        if str(parameter["COM"]).upper() == "TTIME":
            process_ttime(
                parameter, tsr, parameter["COM"], csv_name,df_stdf)
        elif str(parameter["COM"]).upper() == "YIELD":
            process_yield(
                parameter, tsr, parameter["COM"], csv_name,df_stdf)
        elif str(parameter["COM"]).upper() == "CONDITION":
            process_condition(
                parameter, tsr, parameter["COM"], csv_name,df_stdf)
        else:
            process_single_composite(
                parameter, tsr, parameter["COM"], csv_name,df_stdf)
    except Exception as e:
        print(f"Error processing composite: {e}")


def process_condition(parameter, stdf_folder,df_stdf):
    """
    Process a FAKE composite for condition report and execute generation.

    Args:
        parameter (dict): Parameters for processing.
        tsr (DataFrame): DataFrame containing test results.
        composite (str): Composite name to process.
        csv_file (str): CSV file name to process.
    """

    uty.write_log(f"Converting ANAFLOW by COM", FILENAME)
    with open("src/jupiter/personalization.json", "r") as file:
        data = json.load(file)

    product_data = data.get(parameter["CODE"], {})
    product_name = product_data.get("product_name", {})
    
    parameter["PRODUCT"] = product_name
    csv_file = condition_rework(parameter, stdf_folder)
    if len(csv_file) == 0:
        uty.write_log(f"No Extaction good : {parameter["COM"]}", FILENAME)
        return
    parameter["TEST_NUM"] = ""
    parameter["CSV"] = csv_file

    exec(parameter,df_stdf)


def process_yield(parameter, tsr, composite, csv_file, df_stdf):
    """
    Process a FAKE composite for yeald analysis and execute the report generation.

    Args:
        parameter (dict): Parameters for processing.
        tsr (DataFrame): DataFrame containing test results.
        composite (str): Composite name to process.
        csv_file (str): CSV file name to process.
    """
    if parameter["TYPE"].upper() == "X30":
        return
    
    uty.write_log(f"Converting tests by test list", FILENAME)
    with open("src/jupiter/personalization.json", "r") as file:
            data = json.load(file)

    product_data = data.get(parameter["CODE"], {})
    product_name = product_data.get("product_name", {})
    
    parameter["PRODUCT"] = product_name

    parameter["COM"] = composite
    parameter["TEST_NUM"] = ""
    parameter["CSV"] = csv_file
    parameter["TYPE"] = "YIELD"

    exec(parameter,df_stdf)


def process_ttime(parameter, tsr, composite, csv_file,df_stdf):
    """
    Process a FAKE composite for test time analysis and execute the report generation.

    Args:
        parameter (dict): Parameters for processing.
        tsr (DataFrame): DataFrame containing test results.
        composite (str): Composite name to process.
        csv_file (str): CSV file name to process.
    """
    match_group = tsr["TEST_NAM"].str.extract(r"(log_ttime.*)".format(composite))
    tsr["match_group"] = match_group[0]

    test_numbers = tsr.loc[tsr["match_group"].notnull(), "TEST_NUM"].unique().tolist()

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
        product_name = product_data.get("product_name", {})
    else:
        with open("src/jupiter/personalization.json", "r") as file:
            data = json.load(file)
        product_data = data.get(parameter["CODE"], {})
        product_name = product_data.get("product_name", {})
    
    parameter["PRODUCT"] = product_name

    parameter["COM"] = composite
    test_numbers = list(set(filter(None, test_numbers)))
    parameter["TEST_NUM"] = test_numbers
    parameter["CSV"] = csv_file
    parameter["TYPE"] = "TTIME"

    exec(parameter,df_stdf)


def process_single_composite(
    parameter, tsr, composite, csv_file,df_stdf):
    """
    Process a single composite and execute the report generation.

    Args:
        parameter (dict): Parameters for processing.
        tsr (DataFrame): DataFrame containing test results.
        composite (str): Composite name to process.
        csv_file (str): CSV file name to process.
    """
    match_group = tsr["TEST_NAM"].str.extract(
        r"(.*_{0}_.*:.*|.*_{0}_..$|.*_{0}_.*_DELTA_.*)".format(composite)
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

    parameter["COM"] = composite
    test_numbers = list(set(filter(None, test_numbers)))
    parameter["TEST_NUM"] = test_numbers
    parameter["CSV"] = csv_file

    exec(parameter,df_stdf)


def write_config_file(parameter):
    """
    Write the configuration parameters to a JSON file.

    Args:
        parameter (dict): Parameters for processing.
    """
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


def convert_notebook_to_html(parameter):
    """
    Convert the Jupyter notebook to HTML format.

    Args:
        parameter (dict): Parameters for processing.
    """
    uty.write_log("Start Jupyter conversion", FILENAME)
    timestartsub = datetime.datetime.now()
    str_output = (
        parameter["TITLE"]
    )
    if parameter["TYPE"] == "YIELD" or parameter["TYPE"]=="TTIME": 
        dir_output = os.path.abspath(
            os.path.join(
                os.path.dirname(parameter["FILE"][parameter["WAFER"]]["path"]).split(parameter["LOT"]+"_"+parameter["WAFER"])[0],
                (parameter["LOT"]+"_"+parameter["WAFER"]),
                "VOLUME",
                "Report",
            )
        )
    elif parameter["TYPE"] == "CONDITION" : 
        dir_output = os.path.abspath(
            os.path.join(
                os.path.dirname(parameter["FILE"][parameter["WAFER"]]["path"]),
                "Report",
            )
        )
    else:
        dir_output = os.path.abspath(
            os.path.join(
                os.path.dirname(parameter["FILE"][parameter["WAFER"]]["path"]).split(parameter["LOT"]+"_"+parameter["WAFER"])[0],
                (parameter["LOT"]+"_"+parameter["WAFER"]),
                parameter["TYPE"],
                "Report",
                parameter["TYPE"].upper(),
            )
        )
    # else:
    #     dir_output = os.path.abspath(
    #         os.path.join(
    #             "\\\\gpm-pe-data.gnb.st.com\\ENGI_MCD_STDF",
    #             parameter["CODE"],
    #             parameter["FLOW"],
    #             parameter["TYPE"].upper(),
    #         )
    #     )
    if not os.path.exists(dir_output):
        os.makedirs(dir_output)
    
    jupiter_path = os.path.abspath(f"./src/jupiter/{str(parameter['TYPE']).upper()}.ipynb")

    cmd = f'"C:\\Program Files\\Python\\python.exe" "C:\\Program Files\\Python\\Scripts\\jupyter-nbconvert.exe" ./src/jupiter/{str(parameter["TYPE"]).upper()}.ipynb --execute --no-input --to html --output "{dir_output}/{str_output}" '
    print("[NbConvertApp]",dir_output,str_output)
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
        cmd = f'jupyter nbconvert --execute --no-input --to html --output "{dir_output}/{str_output}" "{jupiter_path}"'
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

    uty.write_log("DONE Jupyter conversion", FILENAME)

    return timestartsub, dir_output, str_output


def rework_report(parameter, dir_output, str_output):
    """
    Substitute HTML title with TITLE name and add CSS for better report aspect.

    Args:
        parameter (dict): Parameters for processing.
    """
    report_path = os.path.abspath(f"{dir_output}/{str_output}.html")

    title = parameter["TITLE"]
    new_str = (
        "<title>"
        + title
        + "</title>"
        + '<link rel="icon" href="https://www.st.com/etc/clientlibs/st-site/media/app/images/favicon.ico">'
        + '<script src="https://cdnjs.cloudflare.com/ajax/libs/require.js/2.1.10/require.min.js"></script>'
    )

    try:
        # Utilizza PowerShell per sostituire la riga 6 del file HTML
        ps_command = f"""
        $filePath = "{report_path}"
        $newContent = "{new_str.replace('"', '`"')}"
        $lines = Get-Content $filePath
        $lines[5] = $newContent
        $lines | Set-Content $filePath
        """
        subprocess.run(
            ["powershell", "-Command", ps_command], check=True, text=True, shell=True
        )

        import webbrowser

        webbrowser.open(f"file://{report_path}")
    except Exception as e:
        uty.write_log(f"ERROR: rework_report {e}", FILENAME)
        print(f"ERROR: rework_report {e}")

def exec(parameter,df_stdf):
    """
    Execute the report generation steps.

    Args:
        parameter (dict): Parameters for processing.
    """
    try:
        if (
            str(parameter["COM"].upper()) == "YIELD"
            or str(parameter["TYPE"]).upper() == "CONDITION"
        ):
            pass
        else:
            parameter = rework_stdf(parameter,df_stdf)
            pass
        write_config_file(parameter)
        timestartsub, dir_output, str_output = convert_notebook_to_html(parameter)
        post_exec(parameter, timestartsub, dir_output, str_output)
    except Exception as e:
        print(f"ERROR execution: {e}")


def post_exec(parameter, timestartsub, dir_output, str_output):
    """
    Post-execution function to handle final steps.

    Args:
        parameter (dict): Parameters for processing.
    """
    uty.write_log("postexec START", FILENAME)
    # rework_report(parameter, dir_output, str_output)
    uty.write_log("postexec DONE", FILENAME)
    timeendsub = datetime.datetime.now()
    timeexecsub = timeendsub - timestartsub
    print("[NbConvertApp] Conversion time:", timeexecsub)


def resetpost(file_path):
    print("Reset post.json")
    with open(file_path, "r") as file:
        data = json.load(file)

    for item in data["data"]:
        item["Run"] = "0"

    with open(file_path, "w") as file:
        json.dump(data, file, indent=4)

def get_parameter(path):
    product, productcut, flow, lot_pkg, waf_badge, mytype, stdname = path.split("\\",10)[4:]
    # product,productcut, flow, lot_pkg, waf_badge, mytype, stdname = path.split("/")[3:]
    lot_pkg, waf_badge, corner = (waf_badge + "_TTTT").split("_", 2)

    parameter = {
        "TITLE": "",
        "COM": "",
        "FLOW": flow.upper(),
        "TYPE": mytype.upper(),
        "PRODUCT": "",
        "CODE": product.upper(),
        "LOT": lot_pkg.upper(),
        "WAFER": waf_badge,
        "CUT": productcut.upper(),
        "REVISION": "0.1",
        "FILE": {
            waf_badge: {
                "corner": corner,
                "path": path,
            }
        },
        "AUTHOR": "Matteo Terranova",
        "MAIL": "matteo.terranova@st.com",
        "SITE": "Catania",
        "GROUP": "MDRF - EP - GPAM",
        "TEST_NUM": "",
        "CSV": stdname,
    }

    return parameter

if __name__ == "__main__":
    print("\n\n--- REPORT GENERATOR ---")
    path="\\\\gpm-pe-data.gnb.st.com\\ENGI_MCD_STDF\\44E\\44EZ\\EWS1\\Q443616\\Q443616_04\\VOLUME\\r44exxxz_q443616_04_st44ez-t2kf1_e_ews1_tat2k06_20250301214005.std"
    parameter = get_parameter(path)
    composite = "INIT"
    parameter["COM"] = composite
    
    parameter["TITLE"] = f"{composite.upper().replace('_',' ')} {parameter['FLOW'].upper()} {parameter['TYPE'].lower()}"
    
    path=os.path.join("csv",os.path.basename(parameter['FILE'][parameter['WAFER']]['path']))
    process_composite(parameter,path)
    print("|-->END\n")
