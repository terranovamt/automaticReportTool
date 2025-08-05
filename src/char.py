import os
import pandas as pd
import glob
import json

HEAD = "[CHAR]"

def power_of_10(value):
    return 10**value

def process_single_file(csv_file, corner_name, parameter, df_stdf):
    """Process a single CSV file and return processed dataframes"""
    
    # Extract temperature from MIR
    mir = df_stdf["mir"].copy()
    if str(mir["TST_TEMP"].iloc[0]) == "nan":
        mir["TST_TEMP"] = "30"
    temperature = int(round(float(mir["TST_TEMP"].iloc[0]) / 5.0) * 5.0)
    
    # Process PTR and FTR data similar to original function
    test_nums = (
        parameter["TEST_NUM"]
        if isinstance(parameter["TEST_NUM"], list)
        else [parameter["TEST_NUM"]]
    )
    
    tmpptr = df_stdf["ptr"][df_stdf["ptr"]["TEST_NUM"].isin(test_nums)].copy()
    tmpftr = df_stdf["ftr"][df_stdf["ftr"]["TEST_NUM"].isin(test_nums)].copy()
    prr = df_stdf["prr"].copy()
    pcr = df_stdf["pcr"].copy()
    hbr = df_stdf["hbr"].copy()
    sbr = df_stdf["sbr"].copy()
    
    # Add corner and temperature info
    tmpptr["CORNER"] = corner_name
    tmpptr["TEMPERATURE"] = temperature
    tmpftr["CORNER"] = corner_name
    tmpftr["TEMPERATURE"] = temperature
    
    # Add wafer and lot info
    tmpptr["WAFER"] = mir.SBLOT_ID[0]
    tmpftr["WAFER"] = mir.SBLOT_ID[0]
    tmpptr["LOT_ID"] = mir.LOT_ID[0]
    tmpftr["LOT_ID"] = mir.LOT_ID[0]
    
    return {
        'ptr': tmpptr,
        'ftr': tmpftr,
        'mir': mir,
        'prr': prr,
        'pcr': pcr,
        'hbr': hbr,
        'sbr': sbr,
        'temperature': temperature,
        'corner': corner_name,
        'csv_file': csv_file
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
        'ptr': [],
        'ftr': [],
        'mir': [],
        'prr': [],
        'pcr': [],
        'hbr': [],
        'sbr': []
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
        std_files = glob.glob(os.path.join(corner_folder,"*.std"))

        for std_file in std_files:

            csv_file = os.path.join(os.path.dirname(std_file),"csv",os.path.basename(std_file))
            try:
                df_stdf = read_csv_to_dataframe(parameter=parameter, csv_path= csv_file)  # You need to implement this
                
                file_data = process_single_file(csv_file, corner_name, parameter, df_stdf)
                
                # Append to master collections
                for key in all_data.keys():
                    if not file_data[key].empty if hasattr(file_data[key], 'empty') else len(file_data[key]) > 0:
                        all_data[key].append(file_data[key])
                        
            except Exception as e:
                continue
    
    # Consolidate all data
    consolidated_data = {}
    for key, data_list in all_data.items():
        if data_list:
            if key in ['ptr', 'ftr', 'mir', 'prr', 'pcr', 'hbr', 'sbr']:
                consolidated_data[key] = pd.concat(data_list, ignore_index=True)
            else:
                consolidated_data[key] = data_list
        else:
            consolidated_data[key] = pd.DataFrame()
    
    # Now process the consolidated data using the original logic
    return process_consolidated_data(parameter, consolidated_data, XY_XL, XY_XH, XY_YL, XY_YH, 
                                   XY_Waf, XY_Lot0, XY_Lot1, XY_Lot2, XY_Lot3, XY_Lot4, XY_Lot5, XY_Lot6, 
                                   xwafer, ywafer)

def read_csv_to_dataframe(parameter, csv_path):
    """
    Legge i file CSV e restituisce un dizionario di DataFrame.
    CORREZIONE: Rimossa la definizione di funzione annidata e corretti i parametri.
    
    Args:
        parameter: Dizionario dei parametri contenente le informazioni del file
        csv_path: Percorso base del file CSV
        
    Returns:
        Dict: Dizionario contenente i DataFrame dei file CSV
    """
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

    def read_csv_file(file_path: str, usecols=None) -> pd.DataFrame:
        """
        Funzione helper per leggere un file CSV.
        
        Args:
            file_path: Percorso del file CSV
            usecols: Colonne da leggere (opzionale)
            
        Returns:
            DataFrame o DataFrame vuoto se il file non esiste
        """
        if os.path.exists(file_path):
            try:
                # print(f"[ERROR] reading {file_path}")
                return pd.read_csv(file_path, usecols=usecols, low_memory=False)
            except Exception as e:
                print(f"[ERROR] Error reading {file_path}: {e}")
                return pd.DataFrame()
        else:
            print(f"[WARNING] File not found: {file_path}")
            return pd.DataFrame()

    # Legge tutti i file CSV necessari
    ptr = read_csv_file(f"{csv_path}.ptr.csv", usecols=[0, 1, 5, 6, 7, 10, 11, 12, 13, 14, 15])
    ftr = read_csv_file(f"{csv_path}.ftr.csv", usecols=[0, 1, 4, 23])
    mir = read_csv_file(f"{csv_path}.mir.csv")
    prr = read_csv_file(f"{csv_path}.prr.csv")
    pcr = read_csv_file(f"{csv_path}.pcr.csv")
    hbr = read_csv_file(f"{csv_path}.hbr.csv")
    sbr = read_csv_file(f"{csv_path}.sbr.csv")
    tsr = read_csv_file(f"{csv_path}.tsr.csv")
    
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

    test_numbers = list(set(filter(None, test_numbers)))
    parameter["TEST_NUM"] = test_numbers

    # Crea un dizionario per accedere ai DataFrame
    df_stdf = {
        "ptr": ptr,
        "ftr": ftr,
        "mir": mir,
        "prr": prr,
        "pcr": pcr,
        "hbr": hbr,
        "sbr": sbr
    }

    return df_stdf

def process_consolidated_data(parameter, df_stdf, XY_XL, XY_XH, XY_YL, XY_YH, XY_Waf, 
                            XY_Lot0, XY_Lot1, XY_Lot2, XY_Lot3, XY_Lot4, XY_Lot5, XY_Lot6, 
                            xwafer, ywafer):
    """
    Process the consolidated data using the original logic
    """
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
                for (corner, temp), group in tmpptr.groupby(['CORNER', 'TEMPERATURE']):
                    mask = (tmpptr['CORNER'] == corner) & (tmpptr['TEMPERATURE'] == temp)
                    
                    # Get corresponding PRR data
                    prr_mask = prr['CORNER'] == corner if 'CORNER' in prr.columns else slice(None)
                    prr_group = prr[prr_mask] if prr_mask is not slice(None) else prr
                    
                    # Recalculate coordinates for this group
                    if not group[group["TEST_NUM"] == XY_XH].empty and not group[group["TEST_NUM"] == XY_XL].empty:
                        combined_X = (
                            group[group["TEST_NUM"] == XY_XH]
                            .set_index("PartID")["RESULT"]
                            .astype(int)
                            .apply(lambda x: x << 8)
                        ) + group[group["TEST_NUM"] == XY_XL].set_index("PartID")["RESULT"].astype(int)
                        
                        combined_Y = (
                            group[group["TEST_NUM"] == XY_YH]
                            .set_index("PartID")["RESULT"]
                            .astype(int)
                            .apply(lambda x: x << 8)
                        ) + group[group["TEST_NUM"] == XY_YL].set_index("PartID")["RESULT"].astype(int)
                        
                        # Update PRR coordinates for this group
                        prr.loc[prr_mask, "X_COORD"] = prr.loc[prr_mask, "PartID"].map(combined_X)
                        prr.loc[prr_mask, "Y_COORD"] = prr.loc[prr_mask, "PartID"].map(combined_Y)
                        
                        # Apply range check
                        prr.loc[prr_mask, "X_COORD"] = prr.loc[prr_mask, "X_COORD"].apply(
                            lambda x: x if xwafer[0] <= x <= xwafer[1] else np.nan
                        )
                        prr.loc[prr_mask, "Y_COORD"] = prr.loc[prr_mask, "Y_COORD"].apply(
                            lambda y: y if ywafer[0] <= y <= ywafer[1] else np.nan
                        )
                        
        except Exception as e:
            print(f"ERROR: UID Test number wrong ({e})")
    
    # Continue with the rest of the original processing logic...
    # Remove retest, process PTR and FTR data, etc.
    
    # Remove retest for each corner/temperature combination
    if str(parameter["TYPE"]).upper() != "X30":
        prr = prr.drop_duplicates(subset=["X_COORD", "Y_COORD", "CORNER"], keep="last")
        
        if not tmpptr.empty:
            tmpptr = tmpptr.merge(
                prr[["PartID", "X_COORD", "Y_COORD", "SOFT_BIN", "HARD_BIN", "CORNER"]],
                how="inner",
                on=["PartID", "CORNER"],
            )
        if not tmpftr.empty:
            tmpftr = tmpftr.merge(
                prr[["PartID", "X_COORD", "Y_COORD", "CORNER"]],
                how="inner",
                on=["PartID", "CORNER"],
            )
    
    # Process PTR data
    ptr_dict = {}
    ftr_dict = {}
    
    if not tmpptr.empty:
        # Result scaling logic (same as original)
        
        tmpptr["PARM_FLG"] = (
            tmpptr["PARM_FLG"].astype(str).apply(lambda x: int(x, 2))
        )
        
        tesnames = tmpptr["TEST_TXT"].unique()
        
        def custom_res_scal(group):
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
        
        # Apply scaling and unit conversion
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
        
        # Continue with test splitting logic...
        
        if "TTIME" not in composite:
            # SPLIT VDD TESTS
            regex = f"(?P<TestName>.*)(_vio_|_vbt_|_v11_)(?P<VDD>[^_]+)_(?P<COM>{composite})_(?P<TARGET>.*)"
            testvdd = tmpptr.loc[tmpptr["TEST_TXT"].str.match(regex)].copy()
            testvdd[["TestName", "tmpvdd", "Volt", "COM", "TARGET"]] = testvdd[
                "TEST_TXT"
            ].str.extract(regex, expand=True)
            testvdd = testvdd.dropna(subset=["TestName"])
            testvdd["pltype"] = "BPLVDD"
            if not testvdd.empty:
                tmpptr = tmpptr.loc[~tmpptr["TEST_TXT"].str.match(regex)]
            
            # SPLIT STANDARD TEST
            regex = f"(?P<TestName>.*)_(?P<COM>{composite})_(?P<TARGET>.*)"
            test = tmpptr.copy()
            test[["TestName", "COM", "TARGET"]] = test["TEST_TXT"].str.extract(
                regex, expand=True
            )
            test["pltype"] = "BPLTEMP"
            test = test[test["COM"].notna()]
        else:
            # SPLIT STANDARD TEST for TTIME
            regex = f"(?P<COM>log_ttime)__(?P<TestName>.*)::(?P<TARGET>.*)"
            test = tmpptr.copy()
            test[["COM", "TestName", "TARGET"]] = test["TEST_TXT"].str.extract(
                regex, expand=True
            )
            test["pltype"] = "BPLTEMP"
            test = test[test["COM"].notna()]
        
        # Continue with the rest of PTR processing...
        clearptr = pd.concat([test, testvdd]) if 'testvdd' in locals() else test
        
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
            clearptr.fillna({"Volt": "Standard"}, inplace=True)
            ptr_dict["consolidated"] = clearptr
    
    # Process FTR data similarly
    if not tmpftr.empty:
        # Similar processing for FTR data...
        # (Include the FTR processing logic from original function)
        pass

    
    if len(ptr_dict) != 0:
        ptr = pd.concat(ptr_dict.values(), ignore_index=True)
        ptr.drop(
            ["TestNumber", "RES_SCAL", "LLM_SCAL", "HLM_SCAL", "FTYPE"],
            axis="columns",
            inplace=True,
            errors="ignore",
        )
        os.makedirs("./src/jupiter/tmp", exist_ok=True)
        ptr.to_csv(os.path.abspath("./src/jupiter/tmp/ptr.csv"), index=False)
    
    if len(ftr_dict) != 0:
        ftr = pd.concat(ftr_dict.values(), ignore_index=True)
        ftr.drop(["TestNumber"], axis="columns", inplace=True, errors="ignore")
        ftr.to_csv(os.path.abspath("./src/jupiter/tmp/ftr.csv"), index=False)
    
    return parameter

def process_csv_folder(folder_path, parameter, composite):
    """Process CSV files in a csv folder - groups files by base name"""
    csv_folder = os.path.join(folder_path, "csv")
    
    if not os.path.exists(csv_folder):
        print(f"No 'csv' folder found in {folder_path}")
        return
    
    # Get all files in csv folder
    all_files = os.listdir(csv_folder)
    
    # Group files by base name (everything before first dot)
    file_groups = {}
    for file in all_files:
        if '.' in file:
            base_name = file.split('.')[0]
            if base_name not in file_groups:
                file_groups[base_name] = []
            file_groups[base_name].append(file)
    
    if not file_groups:
        print(f"No grouped files found in {csv_folder}")
        return
    
    print(f"Found {len(file_groups)} file groups in {csv_folder}")
    
    # Process each group
    for base_name, files in file_groups.items():
        print(f"{HEAD} Processing group: {base_name}")
        
        # Find specific file types
        ptr_file = next((f for f in files if '.ptr.csv' in f), None)
        ftr_file = next((f for f in files if '.ftr.csv' in f), None)
        mir_file = next((f for f in files if '.mir.csv' in f), None)
        prr_file = next((f for f in files if '.prr.csv' in f), None)
        pcr_file = next((f for f in files if '.pcr.csv' in f), None)
        hbr_file = next((f for f in files if '.hbr.csv' in f), None)
        sbr_file = next((f for f in files if '.sbr.csv' in f), None)
        
        if not mir_file:
            print(f"No MIR file found for {base_name}, skipping")
            continue
            
        # Read the files
        try:
            mir = pd.read_csv(os.path.join(csv_folder, mir_file))
            temperature = int(round(float(mir["TST_TEMP"].iloc[0]) / 5.0) * 5.0)
            
            print(f"Processing {base_name} at {temperature}°C")
            
            # Process PTR if exists
            if ptr_file:
                tmpptr = pd.read_csv(os.path.join(csv_folder, ptr_file), 
                                   usecols=[0, 1, 3, 6, 7, 10, 13, 14, 15])
                tmpptr["TEMPERATURE"] = temperature
                tmpptr["WAFER"] = mir.SBLOT_ID[0]
                tmpptr["LOT_ID"] = mir.LOT_ID[0]
                
            # Process FTR if exists  
            if ftr_file:
                tmpftr = pd.read_csv(os.path.join(csv_folder, ftr_file), 
                                   usecols=[0, 1, 4, 23])
                tmpftr["TEMPERATURE"] = temperature
                tmpftr["WAFER"] = mir.SBLOT_ID[0]
                tmpftr["LOT_ID"] = mir.LOT_ID[0]
            
            # Process other files
            if prr_file:
                prr = pd.read_csv(os.path.join(csv_folder, prr_file))
            if pcr_file:
                pcr = pd.read_csv(os.path.join(csv_folder, pcr_file))
            if hbr_file:
                hbr = pd.read_csv(os.path.join(csv_folder, hbr_file))
            if sbr_file:
                sbr = pd.read_csv(os.path.join(csv_folder, sbr_file))
                
            print(f"Successfully processed {base_name}")
            
        except Exception as e:
            print(f"Error processing {base_name}: {e}")

def run(path, parameter, composite, DEBUG=False):
    """Main processing function"""
    # Clean the path
    if "CHAR" in path:
        path = path.split("CHAR")[0]
    
    print(f"{HEAD} Start {parameter['CUT']} {composite}")
    
    mainfolder = path.split("CHAR")[0] + "CHAR"
    
    if not os.path.exists(mainfolder):
        print(f"Main folder {mainfolder} does not exist")
        return
    
    # Get all directories in main folder
    corner_folders = [os.path.join(mainfolder, f) for f in os.listdir(mainfolder) if os.path.isdir(os.path.join(mainfolder, f))]
    
    DEBUG and print(f"Found {len(corner_folders)} folders to process")
    
    rework_stdf_multiple(parameter, corner_folders)


def main(DEBUG):
    # Example usage
    path = "\\\\gpm-pe-data.gnb.st.com\\ENGI_MCD_STDF\\44E\\44EZ\\EWSCHAR\\"
    parameter = {'TITLE' :'PMU EWSCHAR char',
                'COM' :'PMU',
                'FLOW' :'EWSCHAR',
                'TYPE' :'CHAR',
                'PRODUCT' :'',
                'CODE' :'44E',
                'LOT' :'Q445172',
                'WAFER' :'05',
                'CUT' :'44EZ',
                'REVISION' :'0.1',
                'FILE' :{'05': {'corner': 'SSTT', 'path': '\\\\gpm-pe-data.gnb.st.com\\ENGI_MCD_STDF\\44E\\44EZ\\EWSCHAR\\Q445172_05_SSTT'}},
                'AUTHOR' :'Matteo Terranova',
                'MAIL' :'matteo.terranova@st.com',
                'SITE' :'Catania',
                'GROUP' :'MDRF - EP - GPAM',
                'TEST_NUM' :'',
                'CSV' :'\\\\gpm-pe-data.gnb.st.com\\ENGI_MCD_STDF\\44E\\44EZ\\EWSCHAR\\Q445172_05_SSTT'}

    composite = "PMU"
    
    run(path, parameter, composite,DEBUG)
    print(f"{HEAD} Hello World")

if __name__ == "__main__":
    DEBUG = False
    main(DEBUG)