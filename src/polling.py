import os
import re
import time
import logging
import subprocess
import core
from stdf2csvnew import *
from rework_stdf import *
from multiprocessing import Process, Manager

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
        username = input("Impossibile ottenere il nome utente automaticamente. Inserisci il tuo nome utente SVN: ")

    password = username

    command = [
        "svn", "cat", svn_url, "--username", username, "--password", password,
        "--non-interactive", "--trust-server-cert"
    ]

    try:
        composite_list = ["YIELD","TTIME"]
        result = subprocess.run(command, capture_output=True, text=True, check=True)
        content = result.stdout
        # composite_list += re.findall(r"Composite\s*:=\s*(\w+)", content)
        return composite_list
    except subprocess.CalledProcessError as e:
        logger.error(f"Errore durante l'accesso al repository SVN: {e}")
        return []

# --==================================================-- #
# POLLING PROCESS
# --==================================================-- #

def polling(directory, stdf2csv_pipeline, csv2report_pipeline, logger):
    product_regrx = re.compile(r"^[A-F0-9]{3}$")
    allowed_flow = {
        "EWS1", "EWS2", "EWS3", "EWSDIE", "FT", "FT1", "FT2", "FIAB", "QC", "FA"
    }
    allowed_package = {"QFP", "QFN", "DIP", "WLCSP", "CSP"}

    # Use a set to track already added paths
    seen_paths = set()

    def check_csv_folder(path):
        std_files = [f for f in os.listdir(path) if f.endswith(".std") or f.endswith(".stdf")]
        if len(std_files) == 1:
            csv_folder_path = os.path.join(path, "csv")
            if os.path.isdir(csv_folder_path):
                csv_files = [f for f in os.listdir(csv_folder_path) if f.endswith(".csv")]
                if len(csv_files) > 3:
                    return True
            std_file_path = os.path.join(path, std_files[0])
            if std_file_path not in seen_paths:
                stdf2csv_pipeline.append(std_file_path)
                seen_paths.add(std_file_path)
                print(f"Added valid path to stdf2csv_pipeline: {std_file_path}")
            return False
        return False

    def check_report_folder(path):
        std_files = [f for f in os.listdir(path) if f.endswith(".std") or f.endswith(".stdf")]
        if len(std_files) == 1:
            report_folder_path = os.path.join(path, "Report")
            std_file_path = os.path.join(path, std_files[0])

            if not os.path.isdir(report_folder_path):
                csv2report_pipeline.append(std_file_path)
                print(f"Added valid path to csv2report_pipeline: {std_file_path}")
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
                            csv2report_pipeline.append(std_file_path)
                            print(f"Added valid path to csv2report_pipeline: {std_file_path}")
                            break

    while True:
        print("Polling for valid paths...")
        for root, dirs, files in os.walk(directory):
            matching_dirs = [d for d in dirs if product_regrx.match(d)]

            max_iterations = len(matching_dirs)
            for index, product in enumerate(matching_dirs, start=1):
                print(f"{product} : {index}/{max_iterations}")
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
                                                            for subfolder in ["VOLUME", "x30"]:
                                                                subfolder_path = os.path.join(wafer_path, subfolder)
                                                                if os.path.isdir(subfolder_path):
                                                                    if check_csv_folder(subfolder_path):
                                                                        check_report_folder(subfolder_path)
                                        else:
                                            for package in os.listdir(flow_path):
                                                package_path = os.path.join(flow_path, package)
                                                if os.path.isdir(package_path) and any(pkg in package for pkg in allowed_package):
                                                    for badge in os.listdir(package_path):
                                                        badge_path = os.path.join(package_path, badge)
                                                        if os.path.isdir(badge_path):
                                                            for subfolder in ["VOLUME", "x30"]:
                                                                subfolder_path = os.path.join(badge_path, subfolder)
                                                                if os.path.isdir(subfolder_path):
                                                                    if check_csv_folder(subfolder_path):
                                                                        check_report_folder(subfolder_path)
            break
        print("Polling cycle complete. Sleeping for 5 minutes.")
        time.sleep(300)

# --==================================================-- #
# CSV2REPORT PROCESS
# --==================================================-- #

def csv2report(csv2report_pipeline, logger):
    while True:
        if csv2report_pipeline:
            path = csv2report_pipeline.pop(0)
            base_path = os.path.dirname(path)
            report_folder = os.path.join(base_path, "Report")
            os.makedirs(report_folder, exist_ok=True)
            parameter = get_parameter(path)
            svn_url = f"svn://mcd-pe-svn.gnb.st.com/prj/ENGI_MCD_SVN/TPI_REPO/trunk/{parameter['CUT']}/{parameter['FLOW']}/cnf/composites.cnf"
            composite_list = get_composite(logger=logger,svn_url=svn_url)
            thread_composites(composite_list, path, logger)
        else:
            time.sleep(25)
            print("No CSV to report paths available. Waiting...")
        time.sleep(5)

def thread_composites(composite_list, path, logger):
    threads = []

    for composite in composite_list:
        # Create a copy of the parameter dictionary for each thread
        thread_parameter = get_parameter(path)
        thread_parameter["COM"] = composite
        thread_parameter["TITLE"] = f"{composite.upper().replace('_',' ')} {thread_parameter['FLOW'].upper()} {thread_parameter['TYPE'].lower()}"
        
        print(f"Start Report {thread_parameter['CODE']} {thread_parameter['LOT']} {thread_parameter['WAFER']} {thread_parameter['TYPE'].lower()} {thread_parameter['COM']}")
        
        # thread = threading.Thread(target=run_report, args=(thread_parameter, logger))
        # thread.start()
        # threads.append((thread, composite))
        run_report(thread_parameter,logger)
        

    # for t, composite in threads:
    #     t.join()
    #     remaining_threads = len([thread for thread, _ in threads if thread.is_alive()])
    #     print(f"Threads ongoing: {remaining_threads}")

def run_report(parameter, logger):
    path=os.path.join(os.path.dirname(parameter['FILE'][parameter['WAFER']]['path']),"csv",os.path.basename(parameter['FILE'][parameter['WAFER']]['path']))
    core.process_composite(parameter,path)
    print(f"End report {parameter['CODE']} {parameter['LOT']} {parameter['WAFER']} {parameter['TYPE'].lower()} {parameter['COM']}")

# --==================================================-- #
# STDF2CSV PROCESS
# --==================================================-- #

def stdf2csv(stdf2csv_pipeline, logger):
    processes = []

    while True:
        if stdf2csv_pipeline:
            path = stdf2csv_pipeline.pop(0)
            process = Process(target=stdf_to_csv, args=(path, logger))
            process.start()
            processes.append(process)
        else:
            active_processes = [p for p in processes if p.is_alive()]
            processes = active_processes
            if not stdf2csv_pipeline and not processes:
                time.sleep(25)
                print("No STDF to CSV paths available. Waiting...")
            else:
                time.sleep(25)
                print(f"Process ongoing: {len(active_processes)}")
            time.sleep(5)

def stdf_to_csv(path, logger):
    parameter = get_parameter(path)
    print(f"Start stdf2csv {parameter['CODE']} {parameter['LOT']} {parameter['WAFER']} {parameter['TYPE']}")
    process_stdf_to_csv(path, logger)
    print(f"End stdf2csv {parameter['CODE']} {parameter['LOT']} {parameter['WAFER']} {parameter['TYPE']}")

def process_stdf_to_csv(path, logger):
    base_path = os.path.dirname(path)
    csv_folder = os.path.join(base_path, "csv")
    os.makedirs(csv_folder, exist_ok=True)
    csv_path = os.path.join(csv_folder, os.path.basename(path))
    stdf2csv_converter(fin=os.path.join(path), fout=csv_path)
    print(f"Finished processing: {path}")

# --==================================================-- #
# MAIN PROCESS
# --==================================================-- #

def main():
    
    # Initialize loggers within the main function
    polling_logger = setup_logger('polling', 'polling.log')
    csv2report_logger = setup_logger('csv2report', 'csv2report.log')
    stdf2csv_logger = setup_logger('stdf2csv', 'stdf2csv.log')

    manager = Manager()
    stdf2csv_pipeline = manager.list()
    csv2report_pipeline = manager.list()

    directory = r"\\gpm-pe-data.gnb.st.com\ENGI_MCD_STDF"
    # directory = "/prj/ENGI_MCD_STDF"

    polling_process = Process(target=polling, args=(directory, stdf2csv_pipeline, csv2report_pipeline, polling_logger))
    stdf2csv_process = Process(target=stdf2csv, args=(stdf2csv_pipeline, stdf2csv_logger))
    csv2report_process = Process(target=csv2report, args=(csv2report_pipeline, csv2report_logger))

    polling_process.start()
    stdf2csv_process.start()
    csv2report_process.start()

    polling_process.join()
    stdf2csv_process.join()
    csv2report_process.join()
    
if __name__ == "__main__":
    main()