# ART.stdf - Usage Examples

This document provides practical examples for using the ART.stdf system.

## Table of Contents

1. [Basic Usage](#basic-usage)
2. [Processing Different Report Types](#processing-different-report-types)
3. [Configuration Examples](#configuration-examples)
4. [Advanced Scenarios](#advanced-scenarios)
5. [Programmatic Usage](#programmatic-usage)
6. [Troubleshooting Examples](#troubleshooting-examples)

---

## Basic Usage

### Example 1: Run with Default Settings

```bash
# Start the polling system with default STDF directory
python main.py
```

**What it does:**
- Monitors `.\STDF` directory
- Automatically detects new STDF files
- Converts them to Parquet format
- Generates reports when data is ready

### Example 2: Monitor Custom Directory

```bash
# Monitor a specific directory
python main.py "\\gpm-pe-data.gnb.st.com\ENGI_MCD_STDF"
```

### Example 3: Monitor Local Test Directory

```bash
# For testing with local files
python main.py "C:\TestData\STDF"
```

---

## Processing Different Report Types

### Volume Report (Single STDF)

**Directory Structure:**
```
STDF/
└── 44E/
    ├── ART.jsonc          # Product configuration
    └── 44EY/
        └── EWS1/
            └── Q443616/
                └── Q443616_01/
                    └── VOLUME/
                        └── test.std.gz    # Your STDF file
```

**Expected Output:**
```
STDF/44E/44EY/EWS1/Q443616/Q443616_01/VOLUME/
├── test.std.gz
├── parquet/              # Auto-generated
│   ├── test.std.gz.ptr.parquet
│   ├── test.std.gz.ftr.parquet
│   ├── test.std.gz.mir.parquet
│   ├── test.std.gz.prr.parquet
│   ├── test.std.gz.pcr.parquet
│   ├── test.std.gz.hbr.parquet
│   └── test.std.gz.sbr.parquet
├── Report/               # Generated reports
│   ├── VOLUME/
│   │   ├── IP_COMPOSITE1_EWS1_volume.html
│   │   ├── IP_COMPOSITE2_EWS1_volume.html
│   │   └── ...
│   ├── TTIME_EWS1_volume.html
│   └── YIELD_EWS1_volume.html
└── REPORT DONE.txt      # Completion marker
```

### Stability Report (30 Loops)

**Directory Structure:**
```
STDF/44E/44EY/EWS1/Q443616/Q443616_01/
└── LOOP/
    └── loops_x30.std.gz   # STDF with 30 repetitions of same part
```

**Expected Output:**
```
Report/LOOP/
├── LOOP_COMPOSITE1_EWS1_loop.html
├── LOOP_COMPOSITE2_EWS1_loop.html
└── ...
```

### Condition Report

**Directory Structure:**
```
STDF/44E/44EY/
└── EWS1/
    └── CONDITION/
        └── anaflow.html   # Exported from anaflow
```

**Expected Output:**
```
STDF/44E/44EY/EWS1/
├── CONDITION/
│   ├── anaflow.html
│   └── REPORT DONE.txt
└── Report/
    ├── COMPOSITE1_EWS1_condition.html
    ├── COMPOSITE2_EWS1_condition.html
    └── ...
```

### Characterization Report

**Directory Structure:**
```
STDF/44E/44EY/
└── EWSCHAR/
    ├── LOT001_01_TTTT/
    │   ├── 01_test_-40C.std.gz
    │   ├── 02_test_-10C.std.gz
    │   ├── 03_test_30C.std.gz
    │   ├── 04_test_60C.std.gz
    │   ├── 05_test_90C.std.gz
    │   └── 06_test_130C.std.gz
    ├── LOT001_02_FFTT/
    │   └── ...
    └── LOT001_03_SSTT/
        └── ...
```

**Expected Output:**
```
STDF/44E/44EY/EWSCHAR/Report/
├── COMPOSITE1/
│   ├── index.html
│   ├── TTTT_analysis.html
│   ├── FFTT_analysis.html
│   └── SSTT_analysis.html
├── COMPOSITE2/
│   └── ...
└── mainmenu.html         # Main navigation page
```

### Shmoo Report

**Directory Structure:**
```
STDF/44E/44EY/
└── EWS1/
    └── SHMOO/
        ├── test1.shm
        ├── test2.shm
        └── test3.shm
```

**Expected Output:**
```
STDF/44E/44EY/EWS1/SHMOO/
├── test1.shm
├── test1_shmoo.html      # Interactive shmoo plot
├── test2.shm
├── test2_shmoo.html
└── ...
```

---

## Configuration Examples

### Example 1: Basic Product Configuration

**File:** `STDF/44E/ART.jsonc`

```jsonc
{
  // Product identification
  "product_name": "Mosquito512K",

  // Wafer map dimensions
  "xwafer": [0, 30],      // X-axis: 0 to 30
  "ywafer": [0, 30],      // Y-axis: 0 to 30

  // Touchdown count for wafer maps
  "touch_down": 150,

  // Test numbers for wafer map reconstruction (FT only)
  "XY_XL": "4500001",     // X Low coordinate
  "XY_XH": "4500002",     // X High coordinate
  "XY_YL": "4500003",     // Y Low coordinate
  "XY_YH": "4500004",     // Y High coordinate
  "XY_Waf": "4500005",    // Wafer number
  "XY_Lot0": "4500006",   // Lot ID byte 0
  "XY_Lot1": "4500007",   // Lot ID byte 1
  "XY_Lot2": "4500008",   // Lot ID byte 2
  "XY_Lot3": "4500009",   // Lot ID byte 3
  "XY_Lot4": "4500010",   // Lot ID byte 4
  "XY_Lot5": "4500011",   // Lot ID byte 5
  "XY_Lot6": "4500012"    // Lot ID byte 6
}
```

### Example 2: Configuration with Characterization Colors

```jsonc
{
  "product_name": "Mosquito512K",
  "xwafer": [0, 30],
  "ywafer": [0, 30],
  "touch_down": 150,

  // Temperature to color mapping for CHAR reports
  "STPaletteChar": {
    "-40": "#03234B",     // ST Blue - Cold
    "-10": "#3CB4E6",     // Light Blue
    "30": "#49B170",      // Green - Room temp
    "60": "#A4C238",      // Yellow-Green
    "90": "#FFD200",      // ST Yellow - Warm
    "130": "#F3693F"      // Orange - Hot
  },

  // Optional: Custom color overrides
  "STblue": "#03234B",
  "STcyan": "#3CB4E6",
  "STgreen": "#49B170",
  "STyellow": "#FFD200",
  "STpink": "#E6007E"
}
```

### Example 3: Product List Configuration

**File:** `STDF/ARTstdf_Product.cnf`

```
[44E, 44F, 449, 550]
```

This tells ART to only process these product codes, ignoring all others.

---

## Advanced Scenarios

### Scenario 1: Re-generate Reports for Existing Data

If you want to regenerate reports without re-converting STDF:

1. Delete the completion marker:
   ```bash
   del "STDF\44E\44EY\EWS1\Q443616\Q443616_01\VOLUME\REPORT DONE.txt"
   ```

2. Optionally, delete specific reports you want to regenerate:
   ```bash
   del "STDF\44E\44EY\EWS1\Q443616\Q443616_01\VOLUME\Report\LOOP\*.html"
   ```

3. The system will detect and regenerate on next polling cycle

### Scenario 2: Save Multiple Analysis Versions

Rename old folders to keep history:

```bash
# Before running new analysis
ren VOLUME VOLUME_VAL1
ren LOOP LOOP_VAL1

# Now place new STDF in VOLUME or LOOP folder
# Old analysis preserved in VOLUME_VAL1, LOOP_VAL1
```

### Scenario 3: Batch Processing Multiple Products

Create a batch script:

```batch
@echo off
echo Processing Product 44E...
python main.py "\\server\STDF\44E"

echo Processing Product 44F...
python main.py "\\server\STDF\44F"

echo Processing Product 449...
python main.py "\\server\STDF\449"

echo All done!
pause
```

### Scenario 4: Monitoring Only Specific Products

Edit `ARTstdf_Product.cnf` to list only products you want:

```
[44E]
```

Now only 44E will be processed, all others ignored.

---

## Programmatic Usage

### Example 1: Use ART as a Library

```python
import sys
import os

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), "src"))

from polling import STDFProcessingSystem

# Create processing system
system = STDFProcessingSystem(watch_path="./test_data/STDF")

# Run single cycle (no loop)
stdf_count, data_count, condition_count, shmoo_count, char_count = system.run_single_cycle()

print(f"Processed: {stdf_count} STDF files, {data_count} reports")
```

### Example 2: Custom Processing

```python
import sys
import os
sys.path.append("src")

from polling import ParameterExtractor
import stdf2data

# Convert specific STDF file
stdf_path = "path/to/test.std.gz"
output_path = "path/to/output/test.std.gz"

# Extract metadata
params = ParameterExtractor.get_parameter_from_stdf_path(stdf_path)
print(f"Product: {params['CODE']}, Flow: {params['FLOW']}")

# Convert to parquet
stdf2data.stdf2data_converter(stdf_path, output_path)
print("Conversion complete!")
```

### Example 3: Generate Single Report

```python
import sys
sys.path.append("src")

import polars as pl
from polling import ProcessingWorker, ProcessType

# Create worker
worker = ProcessingWorker(ProcessType.DATA2REPORT)

# Prepare parameters
parameter = {
    'CODE': '44E',
    'CUT': '44EY',
    'FLOW': 'EWS1',
    'LOT': 'Q443616',
    'WAFER': '01',
    'TYPE': 'VOLUME',
    'COM': 'HSI',
    'TITLE': 'HSI EWS1 volume',
    'PRODUCT': 'Mosquito512K',
    # ... other fields
}

# Process
worker.process_file("path/to/parquet/files", logger)
```

---

## Troubleshooting Examples

### Example 1: Check if File is Ready for Processing

```python
import os

def check_parquet_files(data_path):
    """Check if all required parquet files exist"""
    required = ['.ptr.parquet', '.ftr.parquet', '.mir.parquet',
                '.prr.parquet', '.pcr.parquet', '.hbr.parquet', '.sbr.parquet']

    missing = []
    for suffix in required:
        if not os.path.exists(data_path + suffix):
            missing.append(suffix)

    if missing:
        print(f"Missing files: {missing}")
        return False
    else:
        print("All parquet files present!")
        return True

# Usage
check_parquet_files("STDF/44E/44EY/EWS1/Q443616/Q443616_01/VOLUME/parquet/test.std.gz")
```

### Example 2: Verify Product Configuration

```python
import json

def validate_art_config(config_path):
    """Validate ART.jsonc configuration"""
    with open(config_path, 'r') as f:
        # Remove comments for JSON parsing
        content = '\n'.join(line for line in f if not line.strip().startswith('//'))
        config = json.loads(content)

    required_fields = ['product_name', 'xwafer', 'ywafer', 'touch_down']

    missing = [field for field in required_fields if field not in config]

    if missing:
        print(f"Missing required fields: {missing}")
        return False
    else:
        print(f"Configuration valid for product: {config['product_name']}")
        return True

# Usage
validate_art_config("STDF/44E/ART.jsonc")
```

### Example 3: Check Processing Status

```python
import os
import polars as pl

def check_processing_status(base_path):
    """Check what has been processed"""

    # Check parquet conversion
    parquet_exists = os.path.exists(os.path.join(base_path, "parquet"))

    # Check report generation
    report_exists = os.path.exists(os.path.join(base_path, "Report"))

    # Check completion marker
    done_marker = os.path.exists(os.path.join(base_path, "REPORT DONE.txt"))

    print(f"Parquet files: {'✓' if parquet_exists else '✗'}")
    print(f"Reports: {'✓' if report_exists else '✗'}")
    print(f"Complete: {'✓' if done_marker else '✗'}")

    if report_exists:
        report_path = os.path.join(base_path, "Report")
        html_files = [f for f in os.listdir(report_path) if f.endswith('.html')]
        print(f"Generated reports: {len(html_files)}")
        for report in html_files:
            print(f"  - {report}")

# Usage
check_processing_status("STDF/44E/44EY/EWS1/Q443616/Q443616_01/VOLUME")
```

### Example 4: Manually Trigger Report Generation

If automatic processing missed a file:

```python
import sys
sys.path.append("src")

from polling import STDFProcessingSystem
import logging

# Setup logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("manual")

# Create system
system = STDFProcessingSystem("./STDF")

# Manually process specific file
file_path = "STDF/44E/44EY/EWS1/Q443616/Q443616_01/VOLUME"

# Trigger data report processing
system.rawdata_worker.process_file(file_path, logger)
```

---

## Performance Tips

### Tip 1: Use Compressed STDF Files

```bash
# Compress existing .std file
gzip test.std
# Result: test.std.gz (50-80% smaller!)
```

### Tip 2: Pre-convert Large Batches

```python
import os
import sys
sys.path.append("src")

import stdf2data

# Convert all STDF files in directory
directory = "STDF/44E/44EY/EWS1"

for root, dirs, files in os.walk(directory):
    for file in files:
        if file.endswith('.std') or file.endswith('.std.gz'):
            stdf_path = os.path.join(root, file)
            output_path = os.path.join(root, "parquet", file)

            print(f"Converting {file}...")
            stdf2data.stdf2data_converter(stdf_path, output_path)
```

### Tip 3: Cleanup Old Files

```python
import os
import time

def cleanup_old_reports(base_path, days=30):
    """Delete reports older than specified days"""
    cutoff = time.time() - (days * 86400)

    for root, dirs, files in os.walk(base_path):
        if "Report" in root:
            for file in files:
                file_path = os.path.join(root, file)
                if os.path.getmtime(file_path) < cutoff:
                    print(f"Deleting old report: {file}")
                    os.remove(file_path)

# Usage
cleanup_old_reports("STDF", days=30)
```

---

## Integration Examples

### Example 1: Email Notification on Completion

```python
import smtplib
from email.mime.text import MIMEText

def send_completion_email(report_path, recipient):
    """Send email when report is complete"""
    subject = f"ART Report Complete: {os.path.basename(report_path)}"
    body = f"Report generated successfully at:\n{report_path}"

    msg = MIMEText(body)
    msg['Subject'] = subject
    msg['From'] = 'art@st.com'
    msg['To'] = recipient

    # Configure your SMTP server
    with smtplib.SMTP('smtp.st.com') as server:
        server.send_message(msg)

# Integrate into worker.process_file()
```

### Example 2: Upload Reports to SharePoint

```python
from office365.sharepoint.client_context import ClientContext

def upload_report_to_sharepoint(report_path, site_url, folder):
    """Upload generated report to SharePoint"""
    ctx = ClientContext(site_url).with_credentials(username, password)

    with open(report_path, 'rb') as content_file:
        file_content = content_file.read()

    target_folder = ctx.web.get_folder_by_server_relative_url(folder)
    target_folder.upload_file(os.path.basename(report_path), file_content)
    ctx.execute_query()
```

---

For more examples and detailed API documentation, see:
- [Developer Guide](doc/DEVELOPER_GUIDE.md)
- [User Guide](doc/ART.html)
- [API Reference](doc/DEVELOPER_GUIDE.md#api-reference)
