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
import json
import pandas as pd
from enum import Enum
from dataclasses import dataclass
from typing import List, Dict, Set, Tuple, Optional

import core
import stdf2csv

# ==================================================
# Constants and Configuration
# ==================================================

class ProcessType(Enum):
    """Enumeration for different processing types."""
    STDF2CSV = "stdf2csv"
    CSV2REPORT = "csv2report"
    CONDITION2REPORT = "condition2report"

class FileType(Enum):
    """Enumeration for file types."""
    STDF = "stdf"
    CSV = "csv"
    CONDITION = "condition"

@dataclass
class ProcessingConfig:
    """Configuration for processing operations."""
    allowed_flow = {"EWS1", "EWS2", "EWS3", "EWSDIE", "FT", "FT1", "FT2", "FIAB", "QC", "FA"}
    allowed_package = {"QFP", "QFN", "DIP", "WLCSP", "CSP"}
    product_regex = re.compile(r"^[A-F0-9]{3}$")
    max_lines_per_log = 1000
    backup_count = 1

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

def setup_logger(name: str, log_file: str, level: int = logging.INFO) -> logging.Logger:
    """
    Set up a logger with custom rotating file handler.
    
    Args:
        name: Logger name
        log_file: Path to log file
        level: Logging level
        
    Returns:
        Configured logger instance
    """
    config = ProcessingConfig()
    formatter = logging.Formatter('%(asctime)s - %(message)s')
    handler = LineCountRotatingFileHandler(
        log_file, 
        max_lines=config.max_lines_per_log, 
        backup_count=config.backup_count
    )
    handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)

    return logger

# ==================================================
# Parameter Extraction and Processing
# ==================================================

class ParameterExtractor:
    """Handles parameter extraction from file paths."""
    
    @staticmethod
    def get_parameter_from_stdf_path(path: str) -> Dict:
        """
        Extract parameters from STDF file path.
        
        Args:
            path: STDF file path to parse
            
        Returns:
            Dictionary containing extracted parameters
        """
        # Split path and extract components
        product, productcut, flow, lot_pkg, waf_badge, mytype, stdname = path.split("\\", 10)[4:]
        lot_pkg, waf_badge, corner = (waf_badge + "_TTTT").split("_", 2)

        return ParameterExtractor._create_parameter_dict(
            product=product,
            productcut=productcut,
            flow=flow,
            mytype=mytype,
            lot_pkg=lot_pkg,
            waf_badge=waf_badge,
            corner=corner,
            stdname=stdname,
            path=path
        )

    @staticmethod
    def get_parameter_from_condition_path(path: str) -> Dict:
        """
        Extract parameters from condition file path.
        Updated to handle the CONDITION subdirectory structure.
        
        Args:
            path: Condition file path to parse (e.g., .../EWS1/CONDITION/anaflow.csv)
            
        Returns:
            Dictionary containing extracted parameters
        """
        path_parts = path.split("\\")
        
        # Find the relevant parts - look for PRODUCTCUT pattern and get flow from parent of CONDITION
        product, productcut, flow = "UNK", "UNKA", "UNKNOWN"
        for i, part in enumerate(path_parts):
            if re.match(r"^[A-F0-9]{3}[A-Z]$", part):  # PRODUCTCUT pattern
                productcut = part
                product = part[:-1]  # Remove last character
                # Look for flow directory before CONDITION
                for j in range(i + 1, len(path_parts)):
                    if path_parts[j] == "CONDITION" and j > i + 1:
                        flow = path_parts[j - 1]  # Flow is the directory before CONDITION
                        break
                break
        
        filename = os.path.basename(path)
        
        return ParameterExtractor._create_parameter_dict(
            product=product,
            productcut=productcut,
            flow=flow,
            mytype="CONDITION",
            lot_pkg="CONDITION",
            waf_badge="CONDITION",
            corner="CONDITION",
            stdname=filename,
            path=path
        )
    
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

    @staticmethod
    def _create_parameter_dict(product: str, productcut: str, flow: str, mytype: str,
                             lot_pkg: str, waf_badge: str, corner: str, stdname: str, path: str) -> Dict:
        """
        Create standardized parameter dictionary.
        
        Args:
            Various parameter components
            
        Returns:
            Standardized parameter dictionary
        """
        return {
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

# ==================================================
# SVN and Composite Management
# ==================================================

class CompositeManager:
    """Handles composite list retrieval and validation."""
    
    @staticmethod
    def get_composite_list(logger: logging.Logger, svn_url: str) -> List[str]:
        """
        Retrieve composite list from SVN repository.
        
        Args:
            logger: Logger instance
            svn_url: SVN repository URL
            
        Returns:
            List of composite names
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

    @staticmethod
    def should_skip_composite(parameter: Dict, process_type: ProcessType) -> bool:
        """
        Determine if composite should be skipped based on type and process.
        
        Args:
            parameter: Parameter dictionary
            process_type: Type of processing
            
        Returns:
            True if composite should be skipped
        """
        composite = parameter.get("COM", "")
        param_type = parameter.get("TYPE", "").upper()
        
        # Common skips
        if composite in ["INIT", "FLH_TOOLS"]:
            return True
            
        # Process-specific skips
        if process_type == ProcessType.CSV2REPORT:
            return (
                ("X30" in param_type and "TTIME" in composite) or
                ("X30" in param_type and "YIELD" in composite)
            )
        elif process_type == ProcessType.CONDITION2REPORT:
            return composite in ["TTIME", "YIELD"]
            
        return False

# ==================================================
# File Processing Utilities
# ==================================================

class FileProcessor:
    """Handles file processing operations."""
    
    @staticmethod
    def check_completion_marker(path: str, marker_name: str) -> bool:
        """
        Check if completion marker file exists.
        
        Args:
            path: Directory path to check
            marker_name: Name of marker file
            
        Returns:
            True if marker exists
        """
        marker_path = os.path.join(path, marker_name)
        return os.path.isfile(marker_path)

    @staticmethod
    def create_completion_marker(path: str, marker_name: str, content: str):
        """
        Create completion marker file.
        Updated to handle CONDITION subdirectory properly.
        
        Args:
            path: Directory path (for condition files, this should be the CONDITION directory)
            marker_name: Name of marker file
            content: Content to write to marker file
        """
        marker_path = os.path.join(path, marker_name)
        # Ensure the directory exists
        os.makedirs(path, exist_ok=True)
        with open(marker_path, "w") as file:
            file.write(content)

    @staticmethod
    def get_report_path(base_path: str, parameter: Dict, process_type: ProcessType) -> str:
        """
        Generate report file path based on parameters and process type.
        
        Args:
            base_path: Base directory path
            parameter: Parameter dictionary
            process_type: Type of processing
            
        Returns:
            Report file path
        """
        if process_type == ProcessType.CSV2REPORT:
            base_report_path = os.path.join(os.path.dirname(base_path), "Report")
            
            if "TTIME" in parameter["COM"] or "YIELD" in parameter["COM"]:
                return os.path.join(base_report_path, parameter["TITLE"] + ".html")
            else:
                return os.path.join(
                    base_report_path, 
                    parameter["TYPE"].upper(), 
                    parameter["TITLE"] + ".html"
                )
        elif process_type == ProcessType.CONDITION2REPORT:
            base_report_path = os.path.join(os.path.dirname(base_path), "Report")
            return os.path.join(base_report_path, parameter["TITLE"] + ".html")
        
        return ""

# ==================================================
# Polling System
# ==================================================

class DirectoryPoller:
    """Handles directory polling for new files."""
    
    def __init__(self, config: ProcessingConfig):
        self.config = config

    def check_condition_files(self, folder_path: str, condition_list: List[str]) -> bool:
        """
        Check if folder contains a CONDITION subdirectory with anaflow files and add them to condition list.
        
        Args:
            folder_path: Path to flow folder (e.g., EWS1, EWS2, FT, etc.)
            condition_list: List to append found file paths
            
        Returns:
            True if at least one file was found
        """
        found = False
        
        # Look for CONDITION subdirectory within the flow folder
        condition_folder_path = os.path.join(folder_path, "CONDITION")
        
        if os.path.isdir(condition_folder_path):
            for file in os.listdir(condition_folder_path):
                file_path = os.path.join(condition_folder_path, file)
                # Check for anaflow files (case insensitive) with .csv or .html extension
                if (not os.path.isdir(file_path) and 
                    file.lower().startswith("anaflow") and 
                    file.lower().endswith(('.csv', '.html'))):
                    
                    # Skip if already processed (check marker in CONDITION folder, not flow folder)
                    if not FileProcessor.check_completion_marker(condition_folder_path, "CONDITION_REPORT_DONE.txt"):
                        clean_path = file_path.replace(
                            "\\\\gpm-pe-data.gnb.st.com\\ENGI_MCD_STDF\\", ""
                        ).replace("\\", " ")
                        print(f"[Polling] New CONDITION found: {clean_path}")
                        clean_path
                        condition_list.append(file_path)
                        found = True
                    else :
                        clean_path = condition_folder_path.replace(
                            "\\\\gpm-pe-data.gnb.st.com\\ENGI_MCD_STDF\\", ""
                        ).replace("\\", " ")
                        False and print(f"[Polling] REPORT DONE {clean_path}")
        
        return found

    def check_csv_folder(self, path: str, stdf_list: List[str], seen_paths: Set[str]) -> bool:
        """
        Check if CSV folder needs processing and add STDF files to processing list.
        
        Args:
            path: Path to check
            stdf_list: List to append STDF file paths
            seen_paths: Set of already processed paths
            
        Returns:
            True if CSV folder is ready for report generation
        """
        # Skip if report is already done
        if FileProcessor.check_completion_marker(path, "REPORT DONE.txt"):
            clean_path = path.replace(
                "\\\\gpm-pe-data.gnb.st.com\\ENGI_MCD_STDF\\", ""
            ).replace("\\", " ")
            False and print(f"[Polling] REPORT DONE {clean_path}")
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
                stdf_list.append(std_file_path)
                seen_paths.add(std_file_path)
                
        return False

    def check_report_folder(self, path: str, csv_list: List[str], logger: logging.Logger):
        """
        Check if report folder needs processing and add to report generation list.
        
        Args:
            path: Path to check
            csv_list: List to append file paths for report generation
            logger: Logger instance
        """
        std_files = [f for f in os.listdir(path) if f.endswith((".std", ".stdf"))]
        
        if len(std_files) == 1:
            report_folder_path = os.path.join(path, "Report")
            std_file_path = os.path.join(path, std_files[0])

            if not os.path.isdir(report_folder_path):
                print(f"[Polling] New CSV found: {std_files[0]}")
                csv_list.append(std_file_path)
            else:
                # Check for missing composite reports
                parameter = ParameterExtractor.get_parameter_from_stdf_path(std_file_path)
                svn_url = (f"svn://mcd-pe-svn.gnb.st.com/prj/ENGI_MCD_SVN/TPI_REPO/trunk/"
                          f"{parameter['CUT']}/{parameter['FLOW']}/cnf/composites.cnf")
                composite_list = CompositeManager.get_composite_list(logger=logger, svn_url=svn_url)

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
                    csv_list.append(std_file_path)

    def poll_directory(self, directory: str, logger: logging.Logger) -> Tuple[List[str], List[str], List[str]]:
        """
        Poll directory for new files to process with advanced progress display.
        
        Args:
            directory: Root directory to poll
            logger: Logger instance
            
        Returns:
            Tuple of lists for STDF2CSV, CSV2Report, and condition processing
        """
        # Initialize lists and tracking sets
        seen_paths = set()
        stdf_list = []
        csv_list = []
        condition_list = []
        False and print("[Polling] Search valid paths...")
        
        # Walk through directory structure
        for root, dirs, files in os.walk(directory):
            matching_dirs = [d for d in dirs if self.config.product_regex.match(d)]
            max_iterations = len(matching_dirs)
            
            for index, product in enumerate(matching_dirs, start=1):
                # Calcola la percentuale
                percentage = int((index / max_iterations) * 100)
                
                # Stampa percentuale e prodotto
                print(f"{percentage}%")
                print(f"{product}")
                
                product_path = os.path.join(root, product)
                printed_something = False
                
                if os.path.isdir(product_path):
                    # Cattura stdout per verificare se vengono stampate righe
                    import sys
                    from io import StringIO
                    
                    # Salva lo stdout originale
                    original_stdout = sys.stdout
                    
                    # Crea un buffer per catturare l'output
                    captured_output = StringIO()
                    sys.stdout = captured_output
                    
                    try:
                        # Elabora la directory del prodotto
                        self._process_product_directory(
                            product_path, product, stdf_list, csv_list, 
                            condition_list, seen_paths, logger
                        )
                    finally:
                        # Ripristina stdout originale
                        sys.stdout = original_stdout
                    
                    # Ottieni l'output catturato
                    output_content = captured_output.getvalue()
                    
                    # Se c'è output, stampalo e segnala che è stato stampato qualcosa
                    if output_content.strip():
                        print("\033[A\033[A", end='', flush=True)
                        print(output_content, end='')
                        printed_something = True
                
                # Se non è l'ultimo elemento e non sono state stampate righe, sovrascrive
                if index < max_iterations and not printed_something:
                    # Sposta il cursore su di 2 righe per sovrascrivere percentuale e prodotto
                    print("\033[A\033[A", end='', flush=True)
                    
            # Se non è l'ultimo elemento e non sono state stampate righe, sovrascrive
            if index == max_iterations and not printed_something:
                # Sposta il cursore su di 2 righe per sovrascrivere percentuale e prodotto
                print("\033[A\033[A\n\n\033[A\033[A", end='', flush=True)
        
            # Stampa finale
            False and print("[Polling] Completed")
            break
            
        return stdf_list, csv_list, condition_list

    def _process_product_directory(self, product_path: str, product: str, 
                                 stdf_list: List[str], csv_list: List[str], 
                                 condition_list: List[str], seen_paths: Set[str], 
                                 logger: logging.Logger):
        """Process a single product directory."""
        productcut_regex = re.compile(rf"^{product}[A-Z]$")
        
        for productcut in os.listdir(product_path):
            if productcut_regex.match(productcut):
                productcut_path = os.path.join(product_path, productcut)
                
                if os.path.isdir(productcut_path):
                    self._process_productcut_directory(
                        productcut_path, stdf_list, csv_list, 
                        condition_list, seen_paths, logger
                    )

    def _process_productcut_directory(self, productcut_path: str, stdf_list: List[str], 
                                    csv_list: List[str], condition_list: List[str], 
                                    seen_paths: Set[str], logger: logging.Logger):
        """Process a single productcut directory."""
        for flow in os.listdir(productcut_path):
            flow_path = os.path.join(productcut_path, flow)
            
            if flow in self.config.allowed_flow and os.path.isdir(flow_path):
                # Process EWS flows
                if flow.startswith("EWS"):
                    self._process_ews_flow(flow_path, stdf_list, csv_list, condition_list, seen_paths, logger)
                # Process non-EWS flows
                else:
                    self._process_standard_flow(flow_path, stdf_list, csv_list, condition_list, seen_paths, logger)

    def _process_ews_flow(self, flow_path: str, stdf_list: List[str], csv_list: List[str], 
                     condition_list: List[str], seen_paths: Set[str], logger: logging.Logger):
        """
        Process EWS flow directory.
        Updated to check for CONDITION subdirectory.
        """
        # Check for condition files in CONDITION subdirectory
        self.check_condition_files(flow_path, condition_list)
        
        for lot in os.listdir(flow_path):
            lot_path = os.path.join(flow_path, lot)
            
            # Skip the CONDITION directory when processing lots
            if lot == "CONDITION":
                continue
                
            if os.path.isdir(lot_path):
                lot_wafer_regex = re.compile(rf"^{lot}_([0][1-9]|1[0-9]|2[0-5])$")
                
                for wafer in os.listdir(lot_path):
                    wafer_path = os.path.join(lot_path, wafer)
                    
                    if (lot_wafer_regex.match(wafer) and os.path.isdir(wafer_path)):
                        self._process_wafer_subfolders(wafer_path, stdf_list, csv_list, seen_paths, logger)


    def _process_standard_flow(self, flow_path: str, stdf_list: List[str], csv_list: List[str], 
                          condition_list: List[str], seen_paths: Set[str], logger: logging.Logger):
        """
        Process standard (non-EWS) flow directory.
        Updated to check for CONDITION subdirectory.
        """
        # Check for condition files in CONDITION subdirectory
        self.check_condition_files(flow_path, condition_list)
        
        for package in os.listdir(flow_path):
            package_path = os.path.join(flow_path, package)
            
            # Skip the CONDITION directory when processing packages
            if package == "CONDITION":
                continue
            
            if (os.path.isdir(package_path) and 
                any(pkg in package for pkg in self.config.allowed_package)):
                
                for badge in os.listdir(package_path):
                    badge_path = os.path.join(package_path, badge)
                    
                    if os.path.isdir(badge_path):
                        self._process_wafer_subfolders(badge_path, stdf_list, csv_list, seen_paths, logger)


    def _process_wafer_subfolders(self, base_path: str, stdf_list: List[str], csv_list: List[str], 
                                 seen_paths: Set[str], logger: logging.Logger):
        """Process wafer subfolders (x30, VOLUME)."""
        for subfolder in ["x30", "VOLUME"]:
            subfolder_path = os.path.join(base_path, subfolder)
            
            if os.path.isdir(subfolder_path):
                if self.check_csv_folder(subfolder_path, stdf_list, seen_paths):
                    self.check_report_folder(subfolder_path, csv_list, logger)

# ==================================================
# Processing Workers
# ==================================================

class ProcessingWorker:
    """Base class for processing workers."""
    
    def __init__(self, process_type: ProcessType):
        self.process_type = process_type

    def create_title(self, parameter: Dict, composite: str) -> str:
        """Create title based on process type and parameters."""
        if self.process_type == ProcessType.CONDITION2REPORT:
            return f"{composite.upper().replace('_', ' ')} {parameter['FLOW'].upper()} condition"
        else:
            return f"{composite.upper().replace('_', ' ')} {parameter['FLOW'].upper()} {parameter['TYPE'].lower()}"

    def get_completion_marker_info(self) -> Tuple[str, str]:
        """Get completion marker file name and content."""
        if self.process_type == ProcessType.CONDITION2REPORT:
            return (
                "CONDITION_REPORT_DONE.txt",
                "IF YOU READ THIS ALL CONDITION REPORTS HAVE BEEN GENERATED \n"
                "THIS FOLDER WILL BE SKIPPED FOR CONDITION PROCESSING\n"
                "IN CASE DELETE THIS FILE IF YOU WANT TO REGENERATE CONDITION REPORTS AND WAIT"
            )
        else:
            return (
                "REPORT DONE.txt",
                "IF YOU READ THIS ALL REPORT HAVE BEEN GENERATED \n"
                "THIS FOLDER WILL BE SKIPPED\n"
                "IN CASE DELETE THIS FILE END REPORT YOU WANT TO REGENERATE AND WAIT"
            )

class ReportWorker(ProcessingWorker):
    """Worker for report generation (CSV2REPORT and CONDITION2REPORT)."""
    
    def process_file(self, path: str, logger: logging.Logger):
        """
        Main processing function for condition report generation.
        Updated to handle CONDITION subdirectory structure.
        
        Args:
            path: Path to condition file to process
            logger: Logger instance
        """
        if self.process_type == ProcessType.CSV2REPORT:
            parameter = ParameterExtractor.get_parameter(path)
        else:
            parameter = ParameterExtractor.get_parameter_from_condition_path(path)
        
        # Get composite list
        svn_url = (f"svn://mcd-pe-svn.gnb.st.com/prj/ENGI_MCD_SVN/TPI_REPO/trunk/"
                f"{parameter['CUT']}/{parameter['FLOW']}/cnf/composites.cnf")
        composite_list = CompositeManager.get_composite_list(logger=logger, svn_url=svn_url)
        
        # SPOSTATO QUI: Leggi il CSV una sola volta prima del ciclo
        df_stdf = None
        csv_path = None
        if self.process_type == ProcessType.CSV2REPORT:
            csv_path = os.path.join(
                os.path.dirname(parameter['FILE'][parameter['WAFER']]['path']),
                "csv",
                os.path.basename(parameter['FILE'][parameter['WAFER']]['path'])
            )
            df_stdf = self._read_csv_to_dataframe(parameter, csv_path)
        
        # Process each composite
        for composite in composite_list:
            parameter["COM"] = composite
            parameter["TITLE"] = self.create_title(parameter, composite)
            
            # Skip if composite should be skipped
            if CompositeManager.should_skip_composite(parameter, self.process_type):
                continue
                
            report_path = FileProcessor.get_report_path(path, parameter, self.process_type)
            
            if not os.path.isfile(report_path):
                self._log_start_message(parameter)
                try:
                    # Passa df_stdf e csv_path già preparati
                    self._run_report_generation(parameter, path, logger, df_stdf, csv_path)
                except Exception as e:
                    print(f"[{self.process_type.value.upper()}] Error in {composite}: {e}")
            else:
                print(f"[{self.process_type.value.upper()}] Report done {os.path.basename(report_path)}")
        
        # Create completion marker in the CONDITION directory (parent of the file)
        if self.process_type == ProcessType.CONDITION2REPORT:
            condition_directory = os.path.dirname(path)
        else:
            condition_directory = os.path.dirname(path)
        marker_name, marker_content = self.get_completion_marker_info()
        FileProcessor.create_completion_marker(condition_directory, marker_name, marker_content)
    
    def _read_csv_to_dataframe(self, parameter: Dict, csv_path: str) -> Dict:
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
                    print(f"[ERROR] reading {file_path}")
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

    def _log_start_message(self, parameter: Dict):
        """Log start message for report generation."""
        if self.process_type == ProcessType.CSV2REPORT:
            print(f"[CSV2REPORT] Start Report {parameter['CODE']} {parameter['FLOW']} "
                 f"{parameter['LOT']} {parameter['WAFER']} {parameter['TYPE'].lower()} {parameter['COM']}")
        else:
            print(f"[CONDITION2REPORT] Start Report {parameter['CODE']} {parameter['FLOW']} "
                 f"{parameter['COM']} condition")

    def _run_report_generation(self, parameter: Dict, path: str, logger: logging.Logger, df_stdf: Dict = None, csv_path: str = None):
        """
        Run the actual report generation.
        
        Args:
            parameter: Parameter dictionary
            path: File path
            logger: Logger instance
            df_stdf: Pre-loaded DataFrame dictionary (for CSV2REPORT)
            csv_path: CSV file path (for CSV2REPORT)
        """
        local_parameter = copy.deepcopy(parameter)
        
        if self.process_type == ProcessType.CSV2REPORT:
            # Usa i dati già caricati invece di rileggerli
            core.process_composite(local_parameter, csv_path, df_stdf)
            print(f"[CSV2REPORT] End Report {parameter['CODE']} {parameter['FLOW']} "
                 f"{parameter['LOT']} {parameter['WAFER']} {parameter['TYPE'].lower()} {parameter['COM']}")
        else:
            core.process_condition(local_parameter, path)
            print(f"[CONDITION2REPORT] End Report {parameter['CODE']} {parameter['FLOW']} "
                 f"{parameter['COM']} condition")

class STDFWorker(ProcessingWorker):
    """Worker for STDF to CSV conversion."""
    
    def __init__(self):
        super().__init__(ProcessType.STDF2CSV)

    def process_file(self, path: str, logger: logging.Logger):
        """
        Convert STDF file to CSV format.
        
        Args:
            path: Path to STDF file
            logger: Logger instance
        """
        parameter = ParameterExtractor.get_parameter_from_stdf_path(path)
        print(f"[STDF2CSV] Start stdf2csv {parameter['CODE']} {parameter['FLOW']} "
             f"{parameter['LOT']} {parameter['WAFER']} {parameter['TYPE']}")
        
        #self._convert_stdf_to_csv(path, logger)
        
        print(f"[STDF2CSV] End stdf2csv {parameter['CODE']} {parameter['FLOW']} "
             f"{parameter['LOT']} {parameter['WAFER']} {parameter['TYPE']}")

    def _convert_stdf_to_csv(self, path: str, logger: logging.Logger):
        """
        Process STDF file conversion to CSV.
        
        Args:
            path: Path to STDF file
            logger: Logger instance
        """
        base_path = os.path.dirname(path)
        csv_folder = os.path.join(base_path, "csv")
        os.makedirs(csv_folder, exist_ok=True)
        csv_path = os.path.join(csv_folder, os.path.basename(path))
        stdf2csv.stdf2csv_converter(path, csv_path)
# ==================================================
# Main Processing System
# ==================================================

class STDFProcessingSystem:
    """Main processing system that coordinates all operations."""
    
    def __init__(self, watch_path: str):
        """
        Initialize the STDF processing system.
        
        Args:
            watch_path: Root directory to monitor for files
        """
        self.watch_path = watch_path
        self.config = ProcessingConfig()
        self.poller = DirectoryPoller(self.config)
        
        # Initialize workers
        self.stdf_worker = STDFWorker()
        self.csv_worker = ReportWorker(ProcessType.CSV2REPORT)
        self.condition_worker = ReportWorker(ProcessType.CONDITION2REPORT)
        
        # Initialize loggers
        self.polling_logger = setup_logger('polling', 'polling.log')
        self.stdf2csv_logger = setup_logger('stdf2csv', 'stdf2csv.log')
        self.csv2report_logger = setup_logger('csv2report', 'csv2report.log')
        self.condition2report_logger = setup_logger('condition2report', 'condition2report.log')
    
    def process_stdf_files(self, stdf_list: List[str]):
        """
        Process STDF files for conversion to CSV.
        
        Args:
            stdf_list: List of STDF file paths to process
        """
        for stdf_file in stdf_list:
            try:
                self.stdf_worker.process_file(stdf_file, self.stdf2csv_logger)
            except Exception as e:
                self.stdf2csv_logger.error(f"Error processing STDF file {stdf_file}: {e}")
                print(f"[ERROR] STDF processing failed for {stdf_file}: {e}")
    
    def process_csv_files(self, csv_list: List[str]):
        """
        Process CSV files for report generation.
        
        Args:
            csv_list: List of CSV file paths to process
        """
        for csv_file in csv_list:
            try:
                self.csv_worker.process_file(csv_file, self.csv2report_logger)
            except Exception as e:
                self.csv2report_logger.error(f"Error generating report for {csv_file}: {e}")
                print(f"[ERROR] CSV report generation failed for {csv_file}: {e}")
    
    def process_condition_files(self, condition_list: List[str]):
        """
        Process condition files for report generation.
        
        Args:
            condition_list: List of condition file paths to process
        """
        for condition_file in condition_list:
            try:
                self.condition_worker.process_file(condition_file, self.condition2report_logger)
            except Exception as e:
                self.condition2report_logger.error(f"Error generating condition report for {condition_file}: {e}")
                print(f"[ERROR] Condition report generation failed for {condition_file}: {e}")
    
    def run_single_cycle(self):
        """
        Execute a single processing cycle.
        
        Returns:
            Tuple of (stdf_count, csv_count, condition_count) processed
        """
        # Poll for new files
        stdf_list, csv_list, condition_list = self.poller.poll_directory(
            self.watch_path, self.polling_logger
        )
        
        # Process files
        self.process_stdf_files(stdf_list)
        self.process_csv_files(csv_list)
        self.process_condition_files(condition_list)
        
        return len(stdf_list), len(csv_list), len(condition_list)
    
    def run_continuous(self, sleep_interval: int = 60):
        """
        Run the processing system continuously.
        
        Args:
            sleep_interval: Time to sleep between polling cycles (seconds)
        """
        print(f"[SYSTEM] Starting STDF Processing System")
        print(f"[SYSTEM] Monitoring directory: {self.watch_path}")
        False and print(f"[SYSTEM] Polling interval: {sleep_interval} seconds")
        
        cycle_count = 0
        
        while True:
            try:
                cycle_count += 1
                False and print(f"\n[SYSTEM] Starting cycle {cycle_count}")
                
                # Run processing cycle
                stdf_count, csv_count, condition_count = self.run_single_cycle()
                
                # Report cycle results
                total_processed = stdf_count + csv_count + condition_count
                if total_processed > 0:
                    print(f"[SYSTEM] Cycle {cycle_count} completed: "
                         f"STDF={stdf_count}, CSV={csv_count}, Condition={condition_count}")
                else:
                    False and print(f"[SYSTEM] Cycle {cycle_count} completed: No files to process")
                
                # Sleep before next cycle
                False and print(f"[SYSTEM] Sleeping for {sleep_interval} seconds...")
                time.sleep(sleep_interval)
                
            except KeyboardInterrupt:
                print(f"\n[SYSTEM] Shutdown requested by user")
                break
            except Exception as e:
                print(f"[SYSTEM] Error in processing cycle {cycle_count}: {e}")
                self.polling_logger.error(f"System error in cycle {cycle_count}: {e}")
                print(f"[SYSTEM] Continuing after error...")
                time.sleep(sleep_interval)

# ==================================================
# Main Process
# ==================================================

def main():
    """Main execution function."""
    # Set watch path
    watch_path = r"\\gpm-pe-data.gnb.st.com\ENGI_MCD_STDF"
    # Alternative path for Unix systems: watch_path = "/prj/ENGI_MCD_STDF"
    
    # Create and run the processing system
    processing_system = STDFProcessingSystem(watch_path)
    
    try:
        processing_system.run_continuous()
    except Exception as e:
        print(f"[MAIN] Fatal error: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())