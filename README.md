# ART.stdf - Automatic Report Tool

<div align="center">

![ART Logo](doc/banner.jpg)

**Advanced STDF Processing and Report Generation System**

[![Python](https://img.shields.io/badge/Python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/License-STMicroelectronics-green.svg)](LICENSE)
[![Status](https://img.shields.io/badge/Status-Active-success.svg)](https://github.com/terranovamt/automaticReportTool)

</div>

## Overview

**ART.stdf** (Automatic Report Tool) is a high-performance software designed to automatically process MDRF STDF (Standard Test Data Format) files in engineering environments. The tool streamlines daily work by automating the generation of comprehensive test reports with interactive visualizations.

### Key Features

- **Automatic Processing**: Monitors directories and automatically processes STDF files
- **Multiple Report Types**: Condition, Stability, Volume, Test Time, Yield, and Characterization reports
- **Interactive Visualizations**: Uses Plotly for rich, interactive charts and graphs
- **High Performance**: Built with Polars for ultra-fast data processing
- **Efficient Storage**: Stores data in Apache Parquet format for optimal performance

## Quick Start

### Prerequisites

1. **Network Access**: Ensure access to the shared drive:
   ```
   \\gpm-pe-data.gnb.st.com\ENGI_MCD_STDF
   ```

2. **Python Environment**: Python 3.8 or higher

3. **STDF Files**: Preferably in compressed `.std.gz` format

### Installation

```bash
# Clone the repository
git clone https://github.com/terranovamt/automaticReportTool.git
cd automaticReportTool

# Install dependencies
pip install -r requirements.txt
```

### Basic Usage

```bash
# Run the main polling system (monitors default directory)
python main.py

# Monitor a specific directory
python main.py "path/to/your/STDF/directory"
```

## Directory Structure

```
automaticReportTool/
├── main.py                 # Entry point
├── src/
│   ├── polling.py         # Main processing system
│   ├── core.py            # Report generation core
│   ├── stdf2data.py       # STDF conversion utilities
│   ├── charv3.py          # Characterization reports
│   ├── shmoo.py           # Shmoo plot processing
│   ├── condition.py       # Condition report logic
│   ├── pystdf/            # STDF parsing library
│   ├── jupiter/           # Customization utilities
│   ├── script/            # HTML generation and analytics
│   └── web/               # Web templates
├── doc/                   # Documentation
│   ├── ART.html          # User guide (current)
│   ├── USER_GUIDE.html   # Enhanced user guide (NEW)
│   └── DEVELOPER_GUIDE.html  # Developer documentation (NEW)
└── README.md              # This file
```

## Report Types

| Report Type | Purpose | Input | Key Metrics |
|-------------|---------|-------|-------------|
| **Condition** | Test condition analysis | anaflow.html | Per-test conditions |
| **Stability (LOOP)** | Process consistency | 30 test loops | Cp > 10 for parametric |
| **Volume IP** | Volume validation | 1 wafer/100 parts | CpK > 1.6 |
| **Test Time** | Time analysis | Any volume file | Average/Max times |
| **Yield** | Production yield | Volume data | Hardware/Software bins |
| **CHAR** | Characterization | Multi-corner STDF | Temperature variations |
| **Shmoo** | Parameter sweeps | .shm files | Interactive shmoo plots |

## Configuration

### Product Configuration (ART.jsonc)

Create an `ART.jsonc` file in your product directory:

```jsonc
{
  "product_name": "Mosquito512K",  // Product name
  "xwafer": [0, 30],               // Wafer X-axis range
  "ywafer": [0, 30],               // Wafer Y-axis range
  "touch_down": 150,               // Touchdown count

  // Wafer map reconstruction test numbers
  "XY_XL": "4500001",
  "XY_XH": "4500002",
  "XY_YL": "4500003",
  // ... additional XY parameters

  // Optional: Temperature-to-color mapping for CHAR
  "STPaletteChar": {
    "-40": "#03234B",
    "-10": "#3CB4E6",
    "30": "#49B170",
    "60": "#A4C238",
    "90": "#FFD200",
    "130": "#F3693F"
  }
}
```

### Product List (ARTstdf_Product.cnf)

Create a configuration file to specify which products to process:

```
[44E, 44F, 449]
```

### Parallel Processing Configuration

ART.stdf supports parallel processing of STDF files to significantly reduce conversion time. By default, 2 parallel workers are used.

**Configuration**: Edit `src/polling.py` and modify the `ProcessingConfig` class:

```python
@dataclass
class ProcessingConfig:
    # ...
    parallel_stdf_workers = 2  # Default: 2 workers
```

**Performance Guidelines**:
- **2 workers** (default): ~50% faster, uses 2 CPU cores
- **4 workers**: ~75% faster, uses 4 CPU cores
- **Recommended**: Set to number of available CPU cores minus 1

**Example**:
```python
# For 8-core CPU, use 4-6 workers for optimal performance
parallel_stdf_workers = 4
```

**Note**: Processing multiple STDF files simultaneously is most effective when you have multiple files to process. Single file processing remains sequential.

## Documentation

- **[User Guide](doc/USER_GUIDE.html)** - Complete guide for end users
- **[Developer Guide](doc/DEVELOPER_GUIDE.html)** - Technical documentation for developers
- **[Original Docs](doc/ART.html)** - Original documentation

## Technology Stack

- **[Polars](https://pola.rs)** - Lightning-fast DataFrame library
- **[Plotly](https://plotly.com)** - Interactive visualization library
- **[Apache Parquet](https://parquet.apache.org)** - Efficient columnar storage format
- **Python 3.8+** - Core language

## System Architecture

```
┌─────────────────┐
│  Directory      │
│  Polling        │
└────────┬────────┘
         │
         ├──> STDF Files ──> STDF2DATA ──> Parquet Files
         │                                      │
         ├──> Condition HTML ─────────────────> │
         │                                      │
         ├──> Shmoo Files (.shm) ──────────────┤
         │                                      │
         └──────────────────────────────────────┴──> Report Generation
                                                          │
                                    ┌─────────────────────┴──────────────────┐
                                    │                                         │
                              HTML Reports                            Analytics
                           (Interactive Charts)                      (Usage Stats)
```

## Performance

- **Processing Speed**: Handles large STDF files (>1GB) in minutes
- **Memory Efficiency**: Columnar storage allows processing datasets larger than RAM
- **Concurrent Processing**: Multi-threaded operations for faster throughput

## Logging

Logs are stored in the `log/` directory:

- `polling.log` - Directory polling events
- `stdf2data.log` - STDF conversion logs
- `data2report.log` - Report generation logs
- `condition2report.log` - Condition report logs
- `shmoo.log` - Shmoo processing logs

Each log automatically rotates after 1000 lines.

## Contributing

Contributions are welcome! Please contact the development team before making significant changes.

## Support

For questions, issues, or feature requests:

- **Email**: [matteo.terranova@st.com](mailto:matteo.terranova@st.com?subject=[ART]%20-%20Information)
- **GitHub Issues**: [Report an issue](https://github.com/terranovamt/automaticReportTool/issues)

## Author

**Matteo Terranova**
MDRF - GPAM | Test Engineer
STMicroelectronics - Catania, Italy

## License

Copyright © STMicroelectronics - Automatic Report Tool. All rights reserved.

---

<div align="center">
Made with ❤️ by the MDRF-GPAM Team in Catania
</div>
