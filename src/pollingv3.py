"""
STDF File Processing and Report Generation System

This module handles the automatic processing of STDF (Standard Test Data Format) files,
converting them to CSV format and generating reports based on composite configurations.
"""

import copy
import logging
import multiprocessing
import multiprocessing.managers
import os
import re
import subprocess
import time

import core
import stdf2csv

# ==================================================
# Logging Configuration
# ==================================================

from logging.handlers import BaseRotatingHandler


class LineCountRotatingFileHandler(BaseRotatingHandler):
    """
    Custom rotating file handler that rotates log files based on line count
    instead of file size.
    """
    
    def __init__(self, filename, max_lines=1000, backup_count=1, **kwargs):
        """
        Initialize the handler.
        
        Args:
            filename (str): Path to the log file
            max_lines (int): Maximum number of lines before rotation
            backup_count (int): Number of backup files to keep
            **kwargs: Additional keyword arguments for BaseRotatingHandler
        """
        super().__init__(filename, 'a', **kwargs)
        self.max_lines = max_lines
        self.backup_count = backup_count
        self.line_count = 0
        self._open()

    def _open(self):
        """Open the log file and count existing lines."""
        self.stream = open(self.baseFilename, self.mode)
        self.line_count = sum(1 for _ in open(self.baseFilename))
        self.stream.seek(0, 2)  # Move to the end of the file

    def shouldRollover(self, record):
        """
        Determine if log file should be rotated.
        
        Args:
            record: Log record
            
        Returns:
            bool: True if rotation should occur
        """
        return self.line_count >= self.max_lines

    def doRollover(self):
        """Perform the log file rotation."""
        if self.stream:
            self.stream.close()
            
        # Rotate existing backup files
        for i in range(self.backup_count - 1, 0, -1):
            sfn = f"{self.baseFilename}.{i}"
            dfn = f"{self.baseFilename}.{i + 1}"
            if os.path.exists(sfn):
                if os.path.exists(dfn):
                    os.remove(dfn)
                os.rename(sfn, dfn)
                
        # Move current log to backup
        dfn = self.baseFilename + ".1"
        if os.path.exists(dfn):
            os.remove(dfn)
        self.rotate(self.baseFilename, dfn)
        self._open()

    def emit(self, record):
        """
        Emit a log record and increment line count.
        
        Args:
            record: Log record to emit
        """
        if self.shouldRollover(record):
            self.doRollover()
        super().emit(record)
        self.line_count += 1


def setup_logger(name, log_file, level=logging.INFO):
    """
    Set up a logger with custom rotating file handler.
    
    Args:
        name (str): Logger name
        log_file (str): Path to log file
        level (int): Logging level
        
    Returns:
        logging.Logger: Configured logger instance
    """
    formatter = logging.Formatter('%(asctime)s - %(message)s')
    handler = LineCountRotatingFileHandler(log_file, max_lines=1000, backup_count=1)
    handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)

    return logger


# ==================================================
# General Functions
# ==================================================

def get_parameter(path):
    """
    Extract parameters from file path.
    
    Args:
        path (str): File path to parse
        
    Returns:
        dict: Dictionary containing extracted parameters
    """
    # Split path and extract components
    product, productcut, flow, lot_pkg, waf_badge, mytype, stdname = path.split("\\", 10)[4:]
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


def get_composite(logger, svn_url):
    """
    Retrieve composite list from SVN repository.
    
    Args:
        logger (logging.Logger): Logger instance
        svn_url (str): SVN repository URL
        
    Returns:
        list: List of composite names
    """
    try:
        username = os.getlogin()
    except OSError:
        username = input("Insert your username SVN: ")

    password = username

    command = [
        "svn", "cat", svn_url, "--username", username, "--password", password,
        "--non-interactive", "--trust-server-cert"
    ]

    try:
        composite_list = ["YIELD", "TTIME"]
        result = subprocess.run(command, capture_output=True, text=True, check=True)
        content = result.stdout
        composite_list += re.findall(r"Composite\s*:=\s*(\w+)", content)
        return composite_list
    except subprocess.CalledProcessError:
        logger.error(f"[SVN] ERROR: repo not found {svn_url}")
        return []


# ==================================================
# Polling Process
# ==================================================

def check_condition(folder_path, list_condition):
    """
    Check if folder contains anaflow files and add them to condition list.
    
    Args:
        folder_path (str): Path to folder to check
        list_condition (list): List to append found file paths
        
    Returns:
        bool: True if at least one file was found, False otherwise
    """
    found = False
    
    if os.path.isdir(folder_path):
        for file in os.listdir(folder_path):
            file_path = os.path.join(folder_path, file)
            # Check for anaflow files (case insensitive) with .csv or .html extension
            if (not os.path.isdir(file_path) and 
                file.lower().startswith("anaflow") and 
                file.lower().endswith(('.csv', '.html'))):
                print(f"[Polling] New CONDITION found: {file_path}")
                list_condition.append(file_path)
                found = True
    
    return found


def check_csv_folder(path, list_stdf2csv, seen_paths):
    """
    Check if CSV folder needs processing and add STDF files to processing list.
    
    Args:
        path (str): Path to check
        list_stdf2csv (list): List to append STDF file paths
        seen_paths (set): Set of already processed paths
        
    Returns:
        bool: True if CSV folder is ready for report generation
    """
    # Skip if report is already done
    report_done_path = os.path.join(path, "REPORT DONE.txt")
    if os.path.isfile(report_done_path):
        clean_path = os.path.dirname(path).replace(
            "\\\\gpm-pe-data.gnb.st.com\\ENGI_MCD_STDF\\", ""
        ).replace("\\", " ")
        print(f"[Polling] REPORT DONE {clean_path}")
        return False
        
    # Look for STDF files
    std_files = [f for f in os.listdir(path) if f.endswith((".std", ".stdf"))]
    
    if len(std_files) == 1:
        csv_folder_path = os.path.join(path, "csv")
        
        # Check if CSV files already exist
        if os.path.isdir(csv_folder_path):
            csv_files = [f for f in os.listdir(csv_folder_path) if f.endswith(".csv")]
            if len(csv_files) > 8:
                return True
                
        # Add STDF file to processing list if not already processed
        std_file_path = os.path.join(path, std_files[0])
        if std_file_path not in seen_paths:
            print(f"[Polling] New STDF found: {std_files[0]}")
            list_stdf2csv.append(std_file_path)
            seen_paths.add(std_file_path)
            
    return False


def check_report_folder(path, list_csv2report, logger):
    """
    Check if report folder needs processing and add to report generation list.
    
    Args:
        path (str): Path to check
        list_csv2report (list): List to append file paths for report generation
        logger (logging.Logger): Logger instance
    """
    std_files = [f for f in os.listdir(path) if f.endswith((".std", ".stdf"))]
    
    if len(std_files) == 1:
        report_folder_path = os.path.join(path, "Report")
        std_file_path = os.path.join(path, std_files[0])

        if not os.path.isdir(report_folder_path):
            print(f"[Polling] New CSV found: {std_files[0]}")
            list_csv2report.append(std_file_path)
        else:
            # Check for missing composite reports
            parameter = get_parameter(std_file_path)
            svn_url = (f"svn://mcd-pe-svn.gnb.st.com/prj/ENGI_MCD_SVN/TPI_REPO/trunk/{parameter['CUT']}/{parameter['FLOW']}/cnf/composites.cnf")
            composite_list = get_composite(logger=logger, svn_url=svn_url)

            # Collect all HTML files in Report directory
            html_files = []
            for root, dirs, files in os.walk(report_folder_path):
                for file in files:
                    if file.endswith(".html"):
                        html_files.append(file)

            # Check for missing composites
            missing_composites = []
            for comp in composite_list:
                if not any(comp in html_file for html_file in html_files):
                    missing_composites.append(comp)

            if missing_composites:
                print(f"[Polling] New CSV found: {std_files[0]}")
                list_csv2report.append(std_file_path)


def polling(directory, logger):
    """
    Poll directory for new files to process.
    
    Args:
        directory (str): Root directory to poll
        logger (logging.Logger): Logger instance
        
    Returns:
        tuple: Lists of files for STDF2CSV, CSV2Report, and condition processing
    """
    # Regex patterns and allowed values
    product_regex = re.compile(r"^[A-F0-9]{3}$")
    allowed_flow = {"EWS1", "EWS2", "EWS3", "EWSDIE", "FT", "FT1", "FT2", "FIAB", "QC", "FA"}
    allowed_package = {"QFP", "QFN", "DIP", "WLCSP", "CSP"}

    # Initialize lists and tracking sets
    seen_paths = set()
    list_stdf2csv = []
    list_csv2report = []
    list_condition = []

    print("[Polling] Search valid paths...")
    
    # Walk through directory structure
    for root, dirs, files in os.walk(directory):
        matching_dirs = [d for d in dirs if product_regex.match(d)]

        max_iterations = len(matching_dirs)
        for index, product in enumerate(matching_dirs, start=1):
            print(f"{product}: {index}/{max_iterations}")
            product_path = os.path.join(root, product)
            
            if os.path.isdir(product_path):
                productcut_regex = re.compile(rf"^{product}[A-Z]$")
                
                for productcut in os.listdir(product_path):
                    if productcut_regex.match(productcut):
                        productcut_path = os.path.join(product_path, productcut)
                        
                        if os.path.isdir(productcut_path):
                            for flow in os.listdir(productcut_path):
                                flow_path = os.path.join(productcut_path, flow)
                                
                                if flow in allowed_flow and os.path.isdir(flow_path):
                                    # Process EWS flows
                                    if flow.startswith("EWS"):
                                        check_condition(flow_path, list_condition)
                                        
                                        for lot in os.listdir(flow_path):
                                            lot_path = os.path.join(flow_path, lot)
                                            
                                            if os.path.isdir(lot_path):
                                                lot_wafer_regex = re.compile(
                                                    rf"^{lot}_([0][1-9]|1[0-9]|2[0-5])$"
                                                )
                                                
                                                for wafer in os.listdir(lot_path):
                                                    wafer_path = os.path.join(lot_path, wafer)
                                                    
                                                    if (lot_wafer_regex.match(wafer) and 
                                                        os.path.isdir(wafer_path)):
                                                        
                                                        for subfolder in ["x30", "VOLUME"]:
                                                            subfolder_path = os.path.join(
                                                                wafer_path, subfolder
                                                            )
                                                            
                                                            if os.path.isdir(subfolder_path):
                                                                if check_csv_folder(
                                                                    subfolder_path, 
                                                                    list_stdf2csv, 
                                                                    seen_paths
                                                                ):
                                                                    check_report_folder(
                                                                        subfolder_path, 
                                                                        list_csv2report, 
                                                                        logger
                                                                    )
                                    
                                    # Process non-EWS flows
                                    else:
                                        check_condition(flow_path, list_condition)
                                        
                                        for package in os.listdir(flow_path):
                                            package_path = os.path.join(flow_path, package)
                                            
                                            if (os.path.isdir(package_path) and 
                                                any(pkg in package for pkg in allowed_package)):
                                                
                                                for badge in os.listdir(package_path):
                                                    badge_path = os.path.join(package_path, badge)
                                                    
                                                    if os.path.isdir(badge_path):
                                                        for subfolder in ["x30", "VOLUME"]:
                                                            subfolder_path = os.path.join(
                                                                badge_path, subfolder
                                                            )
                                                            
                                                            if os.path.isdir(subfolder_path):
                                                                if check_csv_folder(
                                                                    subfolder_path,
                                                                    list_stdf2csv,
                                                                    seen_paths
                                                                ):
                                                                    check_report_folder(
                                                                        subfolder_path,
                                                                        list_csv2report,
                                                                        logger
                                                                    )
        break

    return list_stdf2csv, list_csv2report, list_condition


# ==================================================
# CSV2Report Process
# ==================================================

def csv2report_worker(path, logger):
    """
    Worker function for CSV to report conversion.
    
    Args:
        path (str): Path to STDF file
        logger (logging.Logger): Logger instance
    """
    parameter = get_parameter(path)
    svn_url = (f"svn://mcd-pe-svn.gnb.st.com/prj/ENGI_MCD_SVN/TPI_REPO/trunk/{parameter['CUT']}/{parameter['FLOW']}/cnf/composites.cnf")
    composite_list = get_composite(logger=logger, svn_url=svn_url)
    
    for composite in composite_list:
        parameter["COM"] = composite
        parameter["TITLE"] = (f"{composite.upper().replace('_', ' ')} {parameter['FLOW'].upper()} {parameter['TYPE'].lower()}")
        
        # Skip certain composite/type combinations
        if (_should_skip_composite(parameter)):
            continue
            
        report_path = _get_report_path(path, parameter)
        
        if not os.path.isfile(report_path):
            print(f"[CSV2REPORT] Start Report {parameter['CODE']} {parameter['FLOW']} {parameter['LOT']} {parameter['WAFER']} {parameter['TYPE'].lower()} {parameter['COM']}")
            try:
                run_report(parameter, logger)
            except Exception as e:
                print(f"[CSV2REPORT] Error in {composite}: {e}")
        else:
            print(f"[CSV2REPORT] Report done {os.path.basename(report_path)}")
    
    # Create completion marker file
    _create_report_done_file(path)


def _should_skip_composite(parameter):
    """
    Determine if composite should be skipped based on type and composite name.
    
    Args:
        parameter (dict): Parameter dictionary
        
    Returns:
        bool: True if composite should be skipped
    """
    return (
        ("X30" in parameter["TYPE"].upper() and "TTIME" in parameter["COM"]) or
        ("X30" in parameter["TYPE"].upper() and "YIELD" in parameter["COM"]) or
        (parameter["COM"] == "INIT") or
        (parameter["COM"] == "FLH_TOOLS")
    )


def _get_report_path(path, parameter):
    """
    Generate report file path based on parameters.
    
    Args:
        path (str): Base path
        parameter (dict): Parameter dictionary
        
    Returns:
        str: Report file path
    """
    base_report_path = os.path.join(os.path.dirname(path), "Report")
    
    if "TTIME" in parameter["COM"] or "YIELD" in parameter["COM"]:
        return os.path.join(base_report_path, parameter["TITLE"] + ".html")
    else:
        return os.path.join(
            base_report_path, 
            parameter["TYPE"].upper(), 
            parameter["TITLE"] + ".html"
        )


def _create_report_done_file(path):
    """
    Create marker file indicating all reports have been generated.
    
    Args:
        path (str): Base path for marker file
    """
    file_name = "REPORT DONE.txt"
    content = ( "IF YOU READ THIS ALL REPORT HAVE BEEN GENERATED \n"
                "THIS FOLDER WILL BE SKIPPED\n"
                "IN CASE DELETE THIS FILE END REPORT YOU WANT TO REGENERATE AND WAIT")

    marker_path = os.path.join(os.path.dirname(path), file_name)
    with open(marker_path, "w") as file:
        file.write(content)


def run_report(parameter, logger):
    """
    Execute report generation for given parameters.
    
    Args:
        parameter (dict): Parameter dictionary
        logger (logging.Logger): Logger instance
    """
    csv_path = os.path.join(
        os.path.dirname(parameter['FILE'][parameter['WAFER']]['path']),
        "csv",
        os.path.basename(parameter['FILE'][parameter['WAFER']]['path'])
    )
    
    local_parameter = copy.deepcopy(parameter)
    core.process_composite(local_parameter, csv_path)
    
    print(f"[CSV2REPORT] End Report {parameter['CODE']} {parameter['FLOW']} {parameter['LOT']} {parameter['WAFER']} {parameter['TYPE'].lower()} {parameter['COM']}")


# ==================================================
# STDF2CSV Process
# ==================================================

def stdf2csv_worker(path, logger):
    """
    Worker function for STDF to CSV conversion.
    
    Args:
        path (str): Path to STDF file
        logger (logging.Logger): Logger instance
    """
    stdf_to_csv(path, logger)


def stdf_to_csv(path, logger):
    """
    Convert STDF file to CSV format.
    
    Args:
        path (str): Path to STDF file
        logger (logging.Logger): Logger instance
    """
    parameter = get_parameter(path)
    print(f"[STDF2CSV] Start stdf2csv {parameter['CODE']} {parameter['FLOW']} {parameter['LOT']} {parameter['WAFER']} {parameter['TYPE']}")
    process_stdf_to_csv(path, logger)
    print(f"[STDF2CSV] End stdf2csv {parameter['CODE']} {parameter['FLOW']} {parameter['LOT']} {parameter['WAFER']} {parameter['TYPE']}")


def process_stdf_to_csv(path, logger):
    """
    Process STDF file conversion to CSV.
    
    Args:
        path (str): Path to STDF file
        logger (logging.Logger): Logger instance
    """
    base_path = os.path.dirname(path)
    csv_folder = os.path.join(base_path, "csv")
    os.makedirs(csv_folder, exist_ok=True)
    csv_path = os.path.join(csv_folder, os.path.basename(path))
    stdf2csv.stdf2csv_converter(path, csv_path)


# ==================================================
# Main Process
# ==================================================

def main():
    """Main execution function."""
    # Initialize loggers
    polling_logger = setup_logger('polling', 'polling.log')
    stdf2csv_logger = setup_logger('stdf2csv', 'stdf2csv.log')
    csv2report_logger = setup_logger('csv2report', 'csv2report.log')

    # Set watch path
    watch_path = r"\\gpm-pe-data.gnb.st.com\ENGI_MCD_STDF"
    # Alternative path for Unix systems: watch_path = "/prj/ENGI_MCD_STDF"
    
    while True:
        # Poll for new files
        stdf_list, csv_list, condition_list = polling(watch_path, polling_logger)
        
        # Process STDF to CSV conversion
        for stdf_file in stdf_list:
            try:
                stdf2csv_worker(stdf_file, stdf2csv_logger)
            except Exception as e:
                stdf2csv_logger.error(f"Error processing STDF file {stdf_file}: {e}")
        
        # Clear processed files
        stdf_list.clear()

        # Process CSV to Report generation
        for csv_file in csv_list:
            try:
                csv2report_worker(csv_file, csv2report_logger)
            except Exception as e:
                csv2report_logger.error(f"Error generating report for {csv_file}: {e}")
        
        # Clear processed files
        csv_list.clear()
        
        # TODO: Process condition_list if needed
        condition_list.clear()


if __name__ == "__main__":
    main()
