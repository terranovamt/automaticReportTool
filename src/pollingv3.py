import multiprocessing.managers
import os
import re
import time
import core
import copy
import logging
import stdf2csv
import subprocess
import multiprocessing 

# --==================================================-- #
# Logging
# --==================================================-- #
from logging.handlers import BaseRotatingHandler

class LineCountRotatingFileHandler(BaseRotatingHandler):
    def __init__(self, filename, max_lines=1000, backup_count=1, **kwargs):
        super().__init__(filename, 'a', **kwargs)
        self.max_lines = max_lines
        self.backup_count = backup_count
        self.line_count = 0
        self._open()

    def _open(self):
        self.stream = open(self.baseFilename, self.mode)
        self.line_count = sum(1 for _ in open(self.baseFilename))
        self.stream.seek(0, 2)  # Move to the end of the file

    def shouldRollover(self, record):
        if self.line_count >= self.max_lines:
            return True
        return False

    def doRollover(self):
        if self.stream:
            self.stream.close()
        for i in range(self.backup_count - 1, 0, -1):
            sfn = f"{self.baseFilename}.{i}"
            dfn = f"{self.baseFilename}.{i + 1}"
            if os.path.exists(sfn):
                if os.path.exists(dfn):
                    os.remove(dfn)
                os.rename(sfn, dfn)
        dfn = self.baseFilename + ".1"
        if os.path.exists(dfn):
            os.remove(dfn)
        self.rotate(self.baseFilename, dfn)
        self._open()

    def emit(self, record):
        if self.shouldRollover(record):
            self.doRollover()
        super().emit(record)
        self.line_count += 1

def setup_logger(name, log_file, level=logging.INFO):
    formatter = logging.Formatter('%(asctime)s - %(message)s')
    handler = LineCountRotatingFileHandler(log_file, max_lines=1000, backup_count=1)
    handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)

    return logger

# --==================================================-- #
# GENERAL FUNCTION
# --==================================================-- #

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


def get_composite(logger,svn_url):
    try:
        username = os.getlogin()
        # username = "terranom"
    except OSError:
        username = input("Insert your username SVN: ")

    password = username

    command = [
        "svn", "cat", svn_url, "--username", username, "--password", password,
        "--non-interactive", "--trust-server-cert"
    ]

    try:
        composite_list = ["YIELD","TTIME"]
        result = subprocess.run(command, capture_output=True, text=True, check=True)
        content = result.stdout
        composite_list += re.findall(r"Composite\s*:=\s*(\w+)", content)
        return composite_list
    except subprocess.CalledProcessError as e:
        logger.error(f"[SVN] ERROR: repo not foud {svn_url}")
        return []

# --==================================================-- #
# POLLING PROCESS
# --==================================================-- #

def polling(directory, logger):
    product_regrx = re.compile(r"^[A-F0-9]{3}$")
    allowed_flow = {
        "EWS1", "EWS2", "EWS3", "EWSDIE", "FT", "FT1", "FT2", "FIAB", "QC", "FA"
    }
    allowed_package = {"QFP", "QFN", "DIP", "WLCSP", "CSP"}

    # Use a set to track already added paths
    seen_paths = set()
    list_stdf2csv = []
    list_csv2report = []
    list_condition = []

    def check_csv_folder(path,list_stdf2csv):
        report_done_path = os.path.join(path, "REPORT DONE.txt")
        if os.path.isfile(report_done_path):
            print(f"[Polling] REPORT DONE {os.path.dirname(path).replace("\\\\gpm-pe-data.gnb.st.com\\ENGI_MCD_STDF\\","").replace("\\"," ")}")
            return False
        std_files = [f for f in os.listdir(path) if f.endswith(".std") or f.endswith(".stdf")]
        if len(std_files) == 1:
            csv_folder_path = os.path.join(path, "csv")
            if os.path.isdir(csv_folder_path):
                csv_files = [f for f in os.listdir(csv_folder_path) if f.endswith(".csv")]
                if len(csv_files) > 8:
                    return True
            std_file_path = os.path.join(path, std_files[0])
            if std_file_path not in seen_paths:
                print(f"[Polling] New STDF found: {std_files[0]}")
                list_stdf2csv.append(std_file_path)
                seen_paths.add(std_file_path)
            return False
        return False

    def check_report_folder(path,list_csv2report):
        std_files = [f for f in os.listdir(path) if f.endswith(".std") or f.endswith(".stdf")]
        if len(std_files) == 1:
            report_folder_path = os.path.join(path, "Report")
            std_file_path = os.path.join(path, std_files[0])

            if not os.path.isdir(report_folder_path):
                print(f"[Polling] New CSV found: {std_files[0]}")
                list_csv2report.append(std_file_path)
            else:
                parameter = get_parameter(std_file_path)
                svn_url = f"svn://mcd-pe-svn.gnb.st.com/prj/ENGI_MCD_SVN/TPI_REPO/trunk/{parameter['CUT']}/{parameter['FLOW']}/cnf/composites.cnf"
                composite_list = get_composite(logger=logger,svn_url=svn_url)

                # Collect all .html files in the "Report" directory and its subdirectories
                html_files = []
                for root, dirs, files in os.walk(report_folder_path):
                    for file in files:
                        if file.endswith(".html"):
                            html_files.append(file)

                # Check if any composite is missing in the list of HTML files
                missing_composites = []
                for comp in composite_list:
                    if not any(comp in html_file for html_file in html_files):
                        missing_composites.append(comp)

                        if missing_composites:
                            print(f"[Polling] New CSV found: {std_files[0]}")
                            list_csv2report.append(std_file_path)
                            break

    while True:
        print("[Polling] Search valid paths...")
        for root, dirs, files in os.walk(directory):
            matching_dirs = [d for d in dirs if product_regrx.match(d)]

            max_iterations = len(matching_dirs)
            for index, product in enumerate(matching_dirs, start=1):
                print(f"{product}: {index}/{max_iterations}")
                product_path = os.path.join(root, product)
                if os.path.isdir(product_path):
                    productcut_regrx = re.compile(rf"^{product}[A-Z]$")
                    for productcut in os.listdir(product_path):
                        if productcut_regrx.match(productcut):
                            productcut_path = os.path.join(product_path, productcut)
                            if os.path.isdir(productcut_path):
                                for flow in os.listdir(productcut_path):
                                    flow_path = os.path.join(productcut_path, flow)
                                    if flow in allowed_flow and os.path.isdir(flow_path):
                                        if flow.startswith("EWS"):
                                            for lot in os.listdir(flow_path):
                                                lot_path = os.path.join(flow_path, lot)
                                                if os.path.isdir(lot_path):
                                                    lot_wafer_regex = re.compile(
                                                        rf"^{lot}_([0][1-9]|1[0-9]|2[0-5])$"
                                                    )
                                                    for wafer in os.listdir(lot_path):
                                                        wafer_path = os.path.join(lot_path, wafer)
                                                        if lot_wafer_regex.match(wafer) and os.path.isdir(wafer_path):
                                                            for subfolder in ["x30", "VOLUME"]:
                                                                subfolder_path = os.path.join(wafer_path, subfolder)
                                                                if os.path.isdir(subfolder_path):
                                                                    if check_csv_folder(subfolder_path, list_stdf2csv):
                                                                        check_report_folder(subfolder_path, list_csv2report)
                                        else:
                                            for package in os.listdir(flow_path):
                                                package_path = os.path.join(flow_path, package)
                                                if os.path.isdir(package_path) and any(pkg in package for pkg in allowed_package):
                                                    for badge in os.listdir(package_path):
                                                        badge_path = os.path.join(package_path, badge)
                                                        if os.path.isdir(badge_path):
                                                            for subfolder in ["x30", "VOLUME"]:
                                                                subfolder_path = os.path.join(badge_path, subfolder)
                                                                if os.path.isdir(subfolder_path):
                                                                    if check_csv_folder(subfolder_path,list_stdf2csv):
                                                                        check_report_folder(subfolder_path,list_csv2report)
            break

        return list_stdf2csv, list_csv2report

# --==================================================-- #
# CSV2REPORT PROCESS
# --==================================================-- #
def csv2report_worker(path,logger):
        # print(f"[CSV2REPORT] Generating report for: {path}")
        parameter = get_parameter(path)
        svn_url = f"svn://mcd-pe-svn.gnb.st.com/prj/ENGI_MCD_SVN/TPI_REPO/trunk/{parameter['CUT']}/{parameter['FLOW']}/cnf/composites.cnf"
        composite_list = get_composite(logger=logger,svn_url=svn_url)
        for composite in composite_list:
            # print(f"[CSV2REPORT] Processing composite: {composite}")
            parameter["COM"] = composite
            parameter["TITLE"] = f"{composite.upper().replace('_',' ')} {parameter['FLOW'].upper()} {parameter['TYPE'].lower()}"
            if \
            ("X30" in parameter["TYPE"].upper() and "TTIME" in parameter["COM"]) or \
            ("X30" in parameter["TYPE"].upper() and "YIELD" in parameter["COM"]) or \
            (parameter["COM"] == "INIT") or \
            (parameter["COM"] == "FLH_TOOLS"):
                continue
            else:
                if "TTIME" in parameter["COM"]:
                    report = os.path.join(os.path.dirname(path),"Report",parameter["TITLE"]+".html")
                elif "YIELD" in parameter["COM"]:
                    report = os.path.join(os.path.dirname(path),"Report",parameter["TITLE"]+".html")
                else:
                    report = os.path.join(os.path.dirname(path),"Report",parameter["TYPE"].upper(),parameter["TITLE"]+".html")
                    
                if not os.path.isfile(report):
                    print(f"[CSV2REPORT] Start Report {parameter['CODE']} {parameter['FLOW']} {parameter['LOT']} {parameter['WAFER']} {parameter['TYPE'].lower()} {parameter['COM']}")
                    try:
                        run_report(parameter,logger)
                    except Exception as e:
                        print(f"[CSV2REPORT] Error in {composite}: {e}")
                    # print(f"[CSV2REPORT] Composite list:{(composite_list)} now {composite}")
                else:
                    print(f"[CSV2REPORT] Report done {os.path.basename(report)}")
        # Nome del file
        file_name = "REPORT DONE.txt"
        # Contenuto da scrivere nel file
        content = "IF YOU READ THIS ALL REPORT HAVE BEEN GENERATED \nTHIS FOLDER WILL BE SKIPPED\n IN CASE DELETE THIS FILE END REPORT YOU WANT TO REGENERATE AND WAIT"

        # Apri il file in modalità scrittura e scrivi il contenuto
        with open(os.path.join(os.path.dirname(path),file_name), "w") as file:
            file.write(content)

                    

def run_report(parameter, logger):
    path=os.path.join(os.path.dirname(parameter['FILE'][parameter['WAFER']]['path']),"csv",os.path.basename(parameter['FILE'][parameter['WAFER']]['path']))
    local_parameter = copy.deepcopy(parameter)
    core.process_composite(local_parameter,path)
    print(f"[CSV2REPORT] End Report {parameter['CODE']} {parameter['FLOW']} {parameter['LOT']} {parameter['WAFER']} {parameter['TYPE'].lower()} {parameter['COM']}")

# --==================================================-- #
# STDF2CSV PROCESS
# --==================================================-- #

def stdf2csv_worker(path, logger):
    # print(f"[STDF2CSV] Processing: {path}")
    stdf_to_csv(path,logger)


def stdf_to_csv(path, logger):
    parameter = get_parameter(path)
    print(f"[STDF2CSV] Start stdf2csv {parameter['CODE']} {parameter['FLOW']} {parameter['LOT']} {parameter['WAFER']} {parameter['TYPE']}")
    process_stdf_to_csv(path, logger)
    print(f"[STDF2CSV] End stdf2csv {parameter['CODE']} {parameter['FLOW']} {parameter['LOT']} {parameter['WAFER']} {parameter['TYPE']}")

def process_stdf_to_csv(path, logger):
    base_path = os.path.dirname(path)
    csv_folder = os.path.join(base_path, "csv")
    os.makedirs(csv_folder, exist_ok=True)
    csv_path = os.path.join(csv_folder, os.path.basename(path))
    stdf2csv.stdf2csv_converter(os.path.join(path), csv_path)
    # print(f"[STDF2CSV] Finished processing: {path}")
    

# --==================================================-- #
# MAIN PROCESS
# --==================================================-- #

def main():

    # Initialize loggers within the main function
    polling_logger = setup_logger('polling', 'polling.log')
    stdf2csv_logger = setup_logger('stdf2csv', 'stdf2csv.log')
    csv2report_logger = setup_logger('csv2report', 'csv2report.log')

    watch_path = r"\\gpm-pe-data.gnb.st.com\ENGI_MCD_STDF"
    # watch_path = "/prj/ENGI_MCD_STDF"
    
    while True:

        stdf_list, csv_list = polling(watch_path, polling_logger)
        
        # if not stdf_list and not csv_list:
        #     print("[Polling] Sleeping for 10 minutes.")
        #     time.sleep(600)  # Aspetta 10 minuti
        #     continue

        # Step 2: STDF to CSV
        for stdf_file in stdf_list:
            try:
                stdf2csv_worker(stdf_file, stdf2csv_logger)
            except Exception as e:
                pass
        stdf_list = []

        # Step 3: CSV to Report
        for csv_file in csv_list:
            try:
                csv2report_worker(csv_file, csv2report_logger)
            except Exception as e:
                pass
        csv_list=[]


if __name__ == "__main__":
    main()