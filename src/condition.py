import os
import json
import fileinput
import subprocess
import numpy as np
import pandas as pd

from bs4 import BeautifulSoup
from io import StringIO

def detect_file_type(file_path):
    """Rileva il tipo di file basandosi sull'estensione"""
    if file_path.lower().endswith('.html'):
        return 'html'
    elif file_path.lower().endswith('.csv'):
        return 'csv'
    else:
        raise ValueError(f"Tipo di file non supportato: {file_path}")

def detect_separator(file_path, max_lines=10):
    """Rileva il separatore CSV (, o ;)"""
    with open(file_path, "r", encoding="utf-8") as file:
        for line_number in range(max_lines):
            line = file.readline()
            if not line:
                break
            # Conta le occorrenze di virgole e punti e virgola
            comma_count = line.count(',')
            semicolon_count = line.count(';')
            
            # Se entrambi sono presenti, usa quello più frequente
            if comma_count > 0 and semicolon_count > 0:
                if comma_count > semicolon_count:
                    return ",", line_number
                else:
                    return ";", line_number
            elif comma_count > 0:
                return ",", line_number
            elif semicolon_count > 0:
                return ";", line_number
                
        # Se non trova separatori, prova con virgola come default
        return ",", 0

def read_html_to_dataframe(file_path):
    """Legge un file HTML e converte le tabelle in DataFrame"""
    try:
        # Prova prima con pandas read_html
        tables = pd.read_html(file_path, encoding='utf-8')
        if tables:
            # Prendi la prima tabella trovata
            df = tables[0]
            return df
    except Exception as e:
        print(f"Errore con pd.read_html: {e}")
        
    # Se pd.read_html fallisce, usa BeautifulSoup
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            content = file.read()
        
        soup = BeautifulSoup(content, 'html.parser')
        tables = soup.find_all('table')
        
        if not tables:
            raise ValueError("Nessuna tabella trovata nel file HTML")
        
        # Prendi la prima tabella
        table = tables[0]
        
        # Estrai i dati dalla tabella
        rows = []
        for tr in table.find_all('tr'):
            row = []
            for td in tr.find_all(['td', 'th']):
                row.append(td.get_text(strip=True))
            if row:  # Solo se la riga non è vuota
                rows.append(row)
        
        if not rows:
            raise ValueError("Nessun dato trovato nella tabella HTML")
        
        # Crea il DataFrame
        if len(rows) > 1:
            df = pd.DataFrame(rows[1:], columns=rows[0])
        else:
            df = pd.DataFrame(rows)
            
        return df
        
    except Exception as e:
        raise ValueError(f"Errore nella lettura del file HTML: {e}")

def read_csv_to_dataframe(file_path):
    """Legge un file CSV con rilevamento automatico del separatore"""
    separator, header_line = detect_separator(file_path)
    try:
        df = pd.read_csv(file_path, sep=separator, header=header_line, encoding='utf-8')
    except UnicodeDecodeError:
        # Prova con encoding alternativo
        df = pd.read_csv(file_path, sep=separator, header=header_line, encoding='latin-1')
    return df

def read_file_to_dataframe(file_path):
    """Funzione principale per leggere qualsiasi tipo di file supportato"""
    file_type = detect_file_type(file_path)
    
    if file_type == 'html':
        return read_html_to_dataframe(file_path)
    elif file_type == 'csv':
        return read_csv_to_dataframe(file_path)
    else:
        raise ValueError(f"Tipo di file non supportato: {file_type}")

def condition_rework(parameter, directory_path):
    """Elabora i file e crea condition.csv"""
    
    # Determina il file da processare
    if os.path.isdir(directory_path):
        # Se è una directory, cerca file CSV o HTML
        file_path = None
        for filename in os.listdir(directory_path):
            if filename.lower().endswith(('.csv', '.html')):
                file_path = os.path.join(directory_path, filename)
                break
        if not file_path:
            raise ValueError("Nessun file CSV o HTML trovato nella directory")
    else:
        file_path = directory_path

    # Leggi il file (CSV o HTML)
    df = read_file_to_dataframe(file_path)
    
    # Normalizza i nomi delle colonne
    df.columns = df.columns.str.upper().str.replace(" ", "").str.strip()
    
    # Verifica che le colonne necessarie esistano
    required_columns = ['TAG', 'BYP']
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        print(f"Attenzione: colonne mancanti {missing_columns}. Continuo senza filtrarle.")
    
    # Applica i filtri solo se le colonne esistono
    if 'TAG' in df.columns:
        df = df[df["TAG"].replace("", np.nan).fillna(0).astype(str).str.strip().isin(['1', '1.0'])]
    
    if 'BYP' in df.columns:
        df = df[df["BYP"].replace("", np.nan).fillna(0).astype(str).str.strip().isin(['0', '0.0', ''])]

    # Rimuovi righe completamente vuote (esclusa la prima colonna)
    def is_all_nan_or_empty(row):
        if len(row) <= 1:
            return False
        return row[1:].isnull().all() or (row[1:].astype(str).str.strip() == "").all()

    df = df[~df.apply(is_all_nan_or_empty, axis=1)]
    
    # Converti colonne numeriche se esistono
    numeric_columns = ['HB', 'TESTNR']
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col].replace("", np.nan).fillna(0), errors='coerce').fillna(0).astype(int)
    
    # Processa colonne stringa se esistono
    if 'TESTSUITE' in df.columns:
        df["TESTSUITE"] = df["TESTSUITE"].str.upper()
    
    if 'HBNAME' in df.columns:
        df["HBNAME"] = df["HBNAME"].str.replace("_", "")
    
    if 'TESTSUITE' in df.columns:
        df = df[~df["TESTSUITE"].str.contains("TTIME", na=False)]

    # Processa TESTSUITE con COMP se entrambe le colonne esistono
    if 'TESTSUITE' in df.columns and 'COMP' in df.columns:
        df["TESTSUITE"] = df.apply(
            lambda row: row["TESTSUITE"].replace("_"+str(row["COMP"]).upper(), "_").split("__")[0] if pd.notna(row["TESTSUITE"]) and pd.notna(row["COMP"]) else row["TESTSUITE"],
            axis=1,
        )
    
    # Rimuovi colonne non necessarie se esistono
    columns_to_drop = [
        "Ext", "BYP", "TAG", "SB", "SBNAME", "GROUPBIN", "GROUPNAME"
    ]
    df = df.drop(columns=[col for col in columns_to_drop if col in df.columns], errors="ignore")
    
    df.reset_index(drop=True, inplace=True)

    # Filtra per componente se la colonna COMP esiste
    if 'COMP' in df.columns:
        df_comp = df[df["COMP"] == parameter["COM"]]
    else:
        print("Attenzione: colonna COMP non trovata, uso tutto il DataFrame")
        df_comp = df
        
    if df_comp.empty:
        return "" 
    
    # Crea la directory se non esiste
    os.makedirs("./src/tmp", exist_ok=True)
    
    filename = f"./src/tmp/condition.csv"
    df_comp.to_csv(filename, index=False, sep=',')  # Usa sempre virgola come separatore in output

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

    # Esempio di utilizzo con un file (CSV o HTML)
    file_path = ".\\STDF\\anaflow_VAL_ST44EZ_T2KF1_0007.html"  # o un file .csv
    
    try:
        # Leggi il file
        df = read_file_to_dataframe(file_path)
        print(f"File letto con successo. Shape: {df.shape}")
        print(f"Colonne: {list(df.columns)}")
        
        # Se esiste la colonna Comp, processa per ogni valore unico
        if 'COMP' in df.columns or 'Comp' in df.columns:
            comp_column = 'COMP' if 'COMP' in df.columns else 'Comp'
            for comp_value in df[comp_column].unique():
                if pd.notna(comp_value):  # Salta valori NaN
                    print(f"Processando componente: {comp_value}")
                    parameter["TITLE"] = str(comp_value)
                    parameter["COM"] = str(comp_value)
                    parameter["TYPE"] = "CONDITION"
                    parameter["FLOW"] = "EWS1"
                    
                    # Crea condition.csv per questo componente
                    condition_file = condition_rework(parameter, file_path)
                    print(f"Creato: {condition_file}")
                    
                    # Crea il file di configurazione
                    cfgfile = f"./src/jupiter/cfg.json"
                    os.makedirs("./src/jupiter", exist_ok=True)
                    
                    try:
                        # Convert any Series objects in the parameter dictionary to lists
                        parameter_clean = {
                            k: v.tolist() if isinstance(v, pd.Series) else v
                            for k, v in parameter.items()
                        }

                        with open(cfgfile, mode="wt", encoding="utf-8") as file:
                            json.dump(parameter_clean, file, indent=4)
                    except Exception as e:
                        print(f"Error writing the configuration file: {e}")

                    # Genera il report
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
                        print(f"Report generato con successo per {comp_value}")
                    else:
                        print(f"ERROR: execution failed {cmd}")
        else:
            print("Colonna COMP/Comp non trovata, processo il file intero")
            condition_file = condition_rework(parameter, file_path)
            print(f"Creato: {condition_file}")
            
    except Exception as e:
        print(f"Errore durante l'elaborazione: {e}")

if __name__ == "__main__":
    main()