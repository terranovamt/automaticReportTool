# ART.stdf - Developer Guide

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [System Components](#system-components)
3. [Data Flow](#data-flow)
4. [Core Modules](#core-modules)
5. [Adding New Features](#adding-new-features)
6. [API Reference](#api-reference)
7. [Testing](#testing)
8. [Deployment](#deployment)
9. [Troubleshooting](#troubleshooting)

---

## Architecture Overview

ART.stdf follows a modular, event-driven architecture with the following layers:

```
┌─────────────────────────────────────────────────────┐
│              Main Entry Point (main.py)             │
└──────────────────────┬──────────────────────────────┘
                       │
┌──────────────────────▼──────────────────────────────┐
│         Polling System (polling.py)                  │
│  • Directory monitoring                              │
│  • File detection and classification                 │
│  • Processing queue management                       │
└─────┬──────────┬──────────┬────────────┬───────────┘
      │          │          │            │
      │          │          │            │
┌─────▼────┐ ┌──▼───────┐ ┌▼─────────┐ ┌▼───────────┐
│  STDF    │ │  Data    │ │Condition │ │   Shmoo    │
│ Converter│ │ Reports  │ │ Reports  │ │  Reports   │
└─────┬────┘ └──┬───────┘ └┬─────────┘ └┬───────────┘
      │          │          │            │
      ▼          ▼          ▼            ▼
┌────────────────────────────────────────────────────┐
│        Report Generation Engine (core.py)          │
│  • HTML generation                                  │
│  • Chart creation (Plotly)                          │
│  • Statistical analysis                             │
└────────────────────────────────────────────────────┘
```

### Design Principles

1. **Separation of Concerns**: Each module has a single, well-defined responsibility
2. **Event-Driven**: File system changes trigger processing workflows
3. **Stateless Processing**: Each file is processed independently
4. **Fault Tolerance**: Errors in one file don't affect others
5. **Performance**: Multi-threaded I/O, columnar data storage

---

## System Components

### 1. Polling System (`polling.py`)

The central orchestrator that monitors directories and manages processing workflows.

#### Key Classes

##### `STDFProcessingSystem`
Main coordinator class that manages all processing operations.

```python
class STDFProcessingSystem:
    def __init__(self, watch_path: str):
        """Initialize the processing system

        Args:
            watch_path: Root directory to monitor
        """
        self.watch_path = watch_path
        self.config = ProcessingConfig()
        self.poller = DirectoryPoller(self.config)
        # Initialize workers...
```

**Key Methods:**
- `run_continuous(sleep_interval)`: Main polling loop
- `run_single_cycle()`: Execute one processing cycle
- `process_stdf_files(stdf_list)`: Handle STDF conversion
- `process_data_files(data_list)`: Generate reports from data
- `process_condition_files(condition_list)`: Process condition files
- `process_shmoo_files(shmoo_list)`: Handle shmoo plots

##### `DirectoryPoller`
Scans filesystem and detects files needing processing.

```python
class DirectoryPoller:
    def poll_directory(self, directory: str, logger: Logger) -> Tuple[Lists]:
        """Poll directory for new files

        Returns:
            Tuple of (stdf_list, data_list, condition_list, shmoo_list, char_list)
        """
```

**Detection Logic:**
1. Walk directory tree following allowed patterns
2. Check for completion markers (`REPORT DONE.txt`)
3. Verify file readiness (all parquet files present)
4. Add to appropriate processing queue

##### `ParameterExtractor`
Parses file paths to extract metadata.

```python
@staticmethod
def get_parameter_from_stdf_path(path: str) -> Dict:
    """Extract parameters from STDF file path

    Expected path format:
    PRODUCT/PRODUCTCUT/FLOW/LOT_PKG/LOT_PKG_WAFER_BADGE/TYPE/file.std

    Returns:
        Dict with keys: PRODUCT, CUT, FLOW, LOT, WAFER, TYPE, etc.
    """
```

##### `ProcessingWorker` (Base Class)
Abstract base for all worker types.

**Subclasses:**
- `STDFWorker`: Converts STDF → Parquet
- `ReportWorker`: Generates HTML reports from data
- `ShmooWorker`: Processes shmoo files
- `CharWorker`: Generates characterization reports

---

### 2. STDF Conversion (`stdf2data.py`)

Converts binary STDF files to columnar Parquet format.

#### Process Flow

```
STDF File (.std/.std.gz)
    │
    ├─> Parse with pystdf library
    │
    ├─> Extract record types:
    │   ├─ PTR (Parametric Test Record)
    │   ├─ FTR (Functional Test Record)
    │   ├─ MIR (Master Information Record)
    │   ├─ PRR (Part Results Record)
    │   ├─ PCR (Part Count Record)
    │   ├─ HBR (Hardware Bin Record)
    │   └─ SBR (Software Bin Record)
    │
    └─> Convert to Parquet files:
        ├─ file.ptr.parquet
        ├─ file.ftr.parquet
        ├─ file.mir.parquet
        ├─ file.prr.parquet
        ├─ file.pcr.parquet
        ├─ file.hbr.parquet
        └─ file.sbr.parquet
```

#### Key Functions

```python
def stdf2data_converter(stdf_path: str, output_path: str):
    """Convert STDF to Parquet files

    Args:
        stdf_path: Input STDF file path
        output_path: Output directory for parquet files

    Creates 7 parquet files with different record types
    """
```

---

### 3. Report Generation (`core.py`)

Generates interactive HTML reports with Plotly charts.

#### Key Functions

##### `process_composite(parameter, data_path, df_stdf)`
Generates report for a single composite.

```python
def process_composite(parameter: Dict, data_path: str, df_stdf: Dict):
    """Generate composite report

    Args:
        parameter: Metadata dictionary
        data_path: Path to parquet files
        df_stdf: Pre-loaded DataFrames dictionary

    Generates:
        - HTML report with interactive charts
        - Statistical analysis tables
        - Wafer maps (if applicable)
    """
```

**Report Components:**
1. Header with metadata
2. Test summary statistics
3. Parametric test histograms
4. Functional test Pareto charts
5. Wafer maps
6. Conclusions and recommendations

##### `process_condition(parameter, path, df_stdf)`
Processes anaflow condition files.

```python
def process_condition(parameter: Dict, path: str, df_stdf: Dict):
    """Generate condition report

    Analyzes:
        - Test conditions per composite
        - Variable patterns
        - Pin groups
    """
```

---

### 4. Characterization Reports (`charv3.py`)

Handles multi-corner characterization data.

#### Process

1. **Data Collection**: Read multiple STDF files for different corners
2. **Temperature Mapping**: Associate each file with temperature
3. **Analysis**: Generate charts showing parameter variation vs temperature
4. **Report Generation**: Create comprehensive CHAR report

#### Key Functions

```python
def run(report_path: str, parameter: Dict, composite: str):
    """Generate characterization report

    Args:
        report_path: Output directory
        parameter: Metadata with file paths
        composite: Composite name to analyze
    """
```

---

### 5. Shmoo Processing (`shmoo.py`)

Processes shmoo plot data files.

#### Features

- Interactive 2D heatmaps
- Pass/fail visualization
- Parametric value visualization
- Export capabilities

```python
class ShmooVisualizer:
    def process_shmoo_files(self, directory_path: str):
        """Process all .shm files in directory

        Creates interactive shmoo plots with:
        - X/Y axis parameters
        - Color-coded pass/fail
        - Hover details
        """
```

---

## Data Flow

### Complete Processing Pipeline

```
1. FILE DETECTION
   └─> DirectoryPoller scans filesystem
       └─> Checks against allowed patterns
           └─> Verifies file readiness
               └─> Adds to processing queue

2. STDF CONVERSION (if needed)
   └─> STDFWorker.process_file()
       └─> stdf2data.stdf2data_converter()
           └─> Parse STDF records
               └─> Write Parquet files
                   └─> Mark as DATA READY

3. REPORT GENERATION
   └─> ReportWorker.process_file()
       └─> Load parquet files into Polars DataFrames
           └─> For each composite:
               └─> core.process_composite()
                   ├─> Generate statistics
                   ├─> Create Plotly charts
                   ├─> Build HTML report
                   └─> Write to disk
           └─> Create completion marker

4. USAGE ANALYTICS
   └─> generate_usage() creates summary dashboard
```

---

## Core Modules

### `pystdf/` - STDF Parser Library

Custom STDF parsing library with optimizations.

**Key Components:**
- `V4.py`: STDF V4 record definitions
- `Importer.py`: Binary file reader
- `Types.py`: Data type conversions
- `Writers.py`: Export utilities

### `jupiter/` - Customization System

Manages product-specific configurations.

```python
def get_personalization(parameter: Dict, key: str) -> Any:
    """Retrieve product-specific configuration

    Looks for ART.jsonc in product directory and returns value for key
    """
```

### `script/` - HTML & Analytics

- `htmlgenv2.py`: HTML template generation
- `graphv2.py`: Chart generation helpers
- `usage_analitics.py`: Usage statistics dashboard

### `web/` - Web Templates

HTML components for reports:
- `navbar.html`: Navigation bar
- `footer.html`: Report footer
- `stlogo.html`: ST logo
- `progressicon.html`: Loading indicator

---

## Adding New Features

### Adding a New Report Type

1. **Create Worker Class**

```python
# In polling.py
class MyNewWorker(ProcessingWorker):
    def __init__(self):
        super().__init__(ProcessType.MY_NEW_TYPE)

    def process_file(self, path: str, logger: logging.Logger):
        """Process file and generate report"""
        parameter = ParameterExtractor.get_parameter(path)
        # Your processing logic here
        self.save_history(path, parameter)
```

2. **Add to ProcessType Enum**

```python
class ProcessType(Enum):
    STDF2DATA = "stdf2data"
    DATA2REPORT = "data2report"
    MY_NEW_TYPE = "my_new_type"  # Add this
```

3. **Integrate into Polling System**

```python
# In STDFProcessingSystem.__init__
self.my_new_worker = MyNewWorker()

# In run_single_cycle
my_new_list = self.poller.poll_for_my_new_files()
self.process_my_new_files(my_new_list)
```

4. **Add Detection Logic**

```python
# In DirectoryPoller
def check_my_new_files(self, path: str, file_list: List[str]) -> bool:
    """Detect files needing MY_NEW_TYPE processing"""
    # Your detection logic
```

### Adding a New Chart Type

1. **Create Chart Function** (in `script/graphv2.py`)

```python
def create_my_chart(df: pl.DataFrame, title: str) -> str:
    """Create custom Plotly chart

    Returns:
        HTML div string with chart
    """
    import plotly.graph_objects as go

    fig = go.Figure()
    # Build your chart

    return fig.to_html(div_id="my_chart", include_plotlyjs=False)
```

2. **Integrate into Report** (in `core.py`)

```python
# In process_composite or other generation function
chart_html = create_my_chart(df, title="My Chart")
# Add to HTML template
```

### Adding Configuration Parameters

1. **Update ART.jsonc Schema**

Document new parameters in `doc/USER_GUIDE.html`:

```jsonc
{
  "my_new_param": "value",  // Description of what it does
}
```

2. **Add Getter Function** (in `jupiter/utility.py`)

```python
def get_my_new_param(parameter: Dict, default_value=None):
    """Retrieve my_new_param from configuration"""
    return get_personalization(parameter, "my_new_param") or default_value
```

3. **Use in Processing**

```python
my_value = get_my_new_param(parameter, default="default_value")
# Use my_value in your logic
```

---

## API Reference

### ProcessingWorker Base Class

All workers inherit from this base class.

#### Methods

```python
def save_history(self, path: str, parameter: dict):
    """Save processing history to history.parquet"""

def create_title(self, parameter: Dict, composite: str) -> str:
    """Create standardized report title"""

def get_completion_marker_info(self) -> Tuple[str, str]:
    """Get marker filename and content"""
    return ("REPORT DONE.txt", "content...")

def read_to_dataframe(self, parameter: Dict, data_path: str) -> Dict:
    """Load parquet files into DataFrames"""
```

### FileProcessor Utilities

```python
@staticmethod
def check_completion_marker(path: str, marker_name: str) -> bool:
    """Check if processing is already complete"""

@staticmethod
def create_completion_marker(path: str, marker_name: str, content: str):
    """Mark processing as complete"""

@staticmethod
def get_report_path(base_path: str, parameter: Dict, process_type: ProcessType) -> str:
    """Generate report output path"""
```

### CompositeManager

```python
@staticmethod
def get_composite_list(logger: Logger, svn_url: str) -> List[str]:
    """Fetch composite list from SVN"""

@staticmethod
def should_skip_composite(parameter: Dict, process_type: ProcessType) -> bool:
    """Determine if composite should be skipped"""
```

---

## Testing

### Unit Tests

Create test files in `tests/` directory:

```python
# tests/test_parameter_extraction.py
import unittest
from src.polling import ParameterExtractor

class TestParameterExtractor(unittest.TestCase):
    def test_stdf_path_parsing(self):
        path = "44E/44EY/EWS1/Q443616/Q443616_01/LOOP/test.std"
        params = ParameterExtractor.get_parameter_from_stdf_path(path)

        self.assertEqual(params['CODE'], '44E')
        self.assertEqual(params['CUT'], '44EY')
        self.assertEqual(params['FLOW'], 'EWS1')
        self.assertEqual(params['LOT'], 'Q443616')
```

### Integration Tests

```python
# tests/test_stdf_conversion.py
def test_stdf_to_parquet_conversion():
    """Test complete STDF conversion pipeline"""
    stdf_path = "test_data/sample.std"
    output_path = "test_output/"

    stdf2data.stdf2data_converter(stdf_path, output_path)

    assert os.path.exists(f"{output_path}.ptr.parquet")
    assert os.path.exists(f"{output_path}.ftr.parquet")
    # ... check all outputs
```

### Running Tests

```bash
# Run all tests
python -m unittest discover tests/

# Run specific test file
python -m unittest tests.test_parameter_extraction

# Run with coverage
pip install coverage
coverage run -m unittest discover tests/
coverage report
coverage html  # Generate HTML report
```

---

## Deployment

### Production Deployment

1. **Server Setup**

```bash
# Install Python 3.8+
sudo apt-get install python3.8 python3-pip

# Install system dependencies
sudo apt-get install libxml2-dev libxslt1-dev
```

2. **Application Setup**

```bash
# Clone repository
git clone https://github.com/terranovamt/automaticReportTool.git
cd automaticReportTool

# Install dependencies
pip install -r requirements.txt

# Configure environment
cp .env.example .env
# Edit .env with production settings
```

3. **Service Configuration**

Create systemd service file `/etc/systemd/system/art-stdf.service`:

```ini
[Unit]
Description=ART.stdf Processing System
After=network.target

[Service]
Type=simple
User=artuser
WorkingDirectory=/path/to/automaticReportTool
ExecStart=/usr/bin/python3 main.py
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

4. **Start Service**

```bash
sudo systemctl daemon-reload
sudo systemctl enable art-stdf
sudo systemctl start art-stdf
sudo systemctl status art-stdf
```

### Monitoring

```bash
# View logs
sudo journalctl -u art-stdf -f

# Check processing logs
tail -f log/polling.log
tail -f log/stdf2data.log
tail -f log/data2report.log
```

---

## Troubleshooting

### Common Issues

#### 1. STDF Parsing Errors

**Symptom**: "Error parsing STDF file"

**Solutions:**
- Verify file is valid STDF format
- Check file permissions
- Try decompressing .gz file manually
- Verify STDF version (only V4 supported)

#### 2. Missing Parquet Files

**Symptom**: "Parquet file not found"

**Solutions:**
```python
# Check if STDF was fully converted
import os
data_path = "path/to/data"
required_files = ['.ptr.parquet', '.ftr.parquet', '.mir.parquet',
                  '.prr.parquet', '.pcr.parquet', '.hbr.parquet', '.sbr.parquet']

for suffix in required_files:
    if not os.path.exists(data_path + suffix):
        print(f"Missing: {suffix}")
        # Re-run STDF conversion
```

#### 3. SVN Composite Fetch Fails

**Symptom**: "SVN repo not found"

**Solutions:**
- Verify network connectivity to SVN server
- Check credentials
- Verify composite.cnf exists in SVN
- Use fallback: `composite_list = ["YIELD", "TTIME"]`

#### 4. Memory Issues with Large Files

**Symptom**: "Out of memory error"

**Solutions:**
- Use `.std.gz` compressed files
- Process files in chunks
- Increase available RAM
- Use selective column reading:

```python
# Read only needed columns
df = pl.read_parquet("file.ptr.parquet", columns=[0, 1, 5, 6])
```

#### 5. Report Generation Slow

**Solutions:**
- Optimize DataFrame operations
- Use lazy evaluation
- Profile code to find bottlenecks:

```python
import cProfile
import pstats

profiler = cProfile.Profile()
profiler.enable()

# Your code here

profiler.disable()
stats = pstats.Stats(profiler)
stats.sort_stats('cumtime')
stats.print_stats(20)  # Top 20 slowest functions
```

### Debugging Tips

#### Enable Verbose Logging

```python
# In polling.py
logger.setLevel(logging.DEBUG)

# Add debug statements
logger.debug(f"Processing file: {file_path}")
logger.debug(f"Parameters: {parameter}")
```

#### Interactive Debugging

```python
# Add breakpoint in code
import pdb; pdb.set_trace()

# Or use iPython debugger
import IPython; IPython.embed()
```

#### Check Data Integrity

```python
import polars as pl

df = pl.read_parquet("file.ptr.parquet")
print(df.shape)  # Check dimensions
print(df.head())  # View first rows
print(df.describe())  # Statistics
print(df.null_count())  # Check for missing data
```

---

## Performance Optimization

### Best Practices

1. **Use Lazy Evaluation**

```python
# Good: Lazy evaluation
df = pl.scan_parquet("file.parquet")\
    .filter(pl.col("TEST_NUM") < 1000)\
    .select(["TEST_NUM", "RESULT"])\
    .collect()

# Bad: Eager loading
df = pl.read_parquet("file.parquet")
df = df.filter(df["TEST_NUM"] < 1000)
df = df.select(["TEST_NUM", "RESULT"])
```

2. **Batch Operations**

```python
# Process multiple files in parallel
from concurrent.futures import ThreadPoolExecutor

with ThreadPoolExecutor(max_workers=4) as executor:
    results = executor.map(process_file, file_list)
```

3. **Memory Management**

```python
# Clear memory after large operations
import gc

df = process_large_dataframe()
# Use df...
del df
gc.collect()
```

### Profiling

```python
# Use line_profiler for detailed profiling
from line_profiler import LineProfiler

lp = LineProfiler()
lp.add_function(my_function)
lp.run('my_function()')
lp.print_stats()
```

---

## Contributing Guidelines

### Code Style

Follow PEP 8 with these specifics:
- Line length: 88 characters (Black formatter)
- Use type hints
- Docstrings in Google style

```python
def process_data(df: pl.DataFrame, threshold: float = 0.5) -> pl.DataFrame:
    """Process DataFrame with given threshold.

    Args:
        df: Input DataFrame
        threshold: Filtering threshold

    Returns:
        Processed DataFrame

    Raises:
        ValueError: If threshold is negative
    """
```

### Commit Messages

```
type(scope): short description

Longer description if needed

Fixes #123
```

Types: `feat`, `fix`, `docs`, `style`, `refactor`, `test`, `chore`

### Pull Request Process

1. Fork repository
2. Create feature branch: `git checkout -b feature/my-feature`
3. Make changes with tests
4. Run tests: `python -m unittest discover`
5. Commit with descriptive messages
6. Push and create PR
7. Wait for review

---

## Additional Resources

- **STDF Specification**: https://www.semiconductors.org/resources/esd-roadmap-2/
- **Polars Documentation**: https://pola-rs.github.io/polars/
- **Plotly Python**: https://plotly.com/python/
- **Apache Parquet**: https://parquet.apache.org/docs/

---

## Contact & Support

**Developer Contact:**
- Name: Matteo Terranova
- Email: matteo.terranova@st.com
- GitHub: @terranovamt

**For Technical Issues:**
- Create GitHub issue with:
  - Environment details
  - Error messages
  - Steps to reproduce
  - Expected vs actual behavior

---

_Last Updated: 2025-10-30_
_Version: 1.0.0_
