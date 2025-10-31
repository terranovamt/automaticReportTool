# ART.stdf System Architecture

## Overview

ART.stdf (Automatic Report Tool) is designed with a modular, layered architecture that separates concerns and enables scalability. The system follows clean architecture principles with clear separation between data processing, business logic, and presentation.

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                        User Interface                            │
│                      (HTML Reports + CLI)                        │
└────────────────────────────┬────────────────────────────────────┘
                             │
┌────────────────────────────▼────────────────────────────────────┐
│                    System Orchestration                          │
│                  (src/system/polling.py)                         │
│                                                                   │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐          │
│  │ Directory    │  │ File Type    │  │ Process      │          │
│  │ Poller       │→ │ Classifier   │→ │ Dispatcher   │          │
│  └──────────────┘  └──────────────┘  └──────────────┘          │
└─────────────┬───────────────┬────────────────┬──────────────────┘
              │               │                │
    ┌─────────▼──────┐ ┌─────▼──────┐  ┌─────▼──────────┐
    │  Conversion    │ │  Analysis  │  │  Report Gen    │
    │   Module       │ │   Module   │  │    Module      │
    └────────────────┘ └────────────┘  └────────────────┘
```

## Module Structure

### 1. System Module (`src/system/`)

**Purpose**: Orchestrates the entire workflow from file detection to report generation.

**Key Components**:
- `polling.py` - Main entry point for continuous processing
  - `DirectoryPoller` - Scans directories for new files
  - `STDFProcessingSystem` - Coordinates all workers
  - `ProcessingConfig` - System-wide configuration

**Responsibilities**:
- Monitor configured directories
- Classify files by type (STDF, Condition, Shmoo)
- Dispatch files to appropriate processors
- Manage processing queues
- Handle logging and error recovery

### 2. Conversion Module (`src/conversion/`)

**Purpose**: Convert binary STDF files to efficient Parquet format.

**Key Components**:
- `stdf2data.py` - STDF to Parquet converter
  - `stdf2data_converter()` - Main conversion function
  - Automatic decompression (.gz, .7z, .zip, .bz2, etc.)
  - Compression management (auto-compress uncompressed files)

**Data Flow**:
```
STDF Binary (.std/.stdf)
    → Decompress (if needed)
    → Parse with pystdf
    → Convert to Polars DataFrame
    → Write Parquet files (.ptr, .ftr, .mir, .prr, etc.)
    → Compress original (if not already compressed)
```

**Performance Features**:
- Parallel processing support (2-4 workers)
- Batch processing (1000 records per batch)
- LZ4 compression for Parquet
- Ultra-fast mode with Polars backend

### 3. Analysis Module (`src/analysis/`)

**Purpose**: Specialized analysis for characterization and shmoo data.

#### 3.1 Characterization Processor (`char_processor.py`)

**Purpose**: Process multi-corner characterization test data.

**Key Features**:
- Multi-corner support (SSTT, FFTT, SFTT, F1TT, etc.)
- Temperature-based analysis
- Wafer map coordinate reconstruction
- Split test handling (VDD conditions)

**Processing Flow**:
```
Corner Folders (CHAR/)
    → Process each corner independently
    → Extract temperature from MIR
    → Filter test numbers by composite
    → Apply result scaling
    → Consolidate all corners
    → Generate per-composite reports
```

**Functions**:
- `run()` - Main entry point for CHAR processing
- `rework_stdf_multiple()` - Process multiple corners
- `process_single_corner_file()` - Process single corner
- `consolidate_corner_data()` - Merge all corners
- `gen_mainmenu()` - Generate navigation menu

#### 3.2 Shmoo Visualizer (`shmoo_visualizer.py`)

**Purpose**: Generate interactive shmoo plots from .shm files.

**Key Features**:
- Multi-DUT aggregation
- Pass percentage heatmaps
- SPEC highlighting (blue lines)
- Automatic axis swapping (Level/Timing)
- Failure count annotations

**Processing Flow**:
```
.shm Files
    → Parse multi-DUT data
    → Extract axis information
    → Detect axis order (swap if needed)
    → Create aggregated percentage matrix
    → Generate interactive Plotly heatmap
    → Save HTML report
    → Move .shm to shm/ subfolder
```

**Class Structure**:
```python
class ShmooVisualizer:
    def parse_shmoo_file()          # Parse .shm format
    def extract_axis_info()         # Get X/Y parameters
    def create_aggregated_matrix()  # Aggregate DUTs
    def create_shmoo_plot()         # Generate Plotly viz
    def process_shmoo_files()       # Batch process
```

### 4. Charts Module (`src/charts/`)

**Purpose**: Reusable chart generation with Plotly.

**Key Components**:
- `chart_generator.py` - Chart factory with 8 chart types

**Chart Types**:
1. Scatter plots (with limit lines)
2. Histograms
3. Box plots
4. Line plots
5. Bar charts
6. Wafer maps (heatmaps)
7. Distribution heatmaps
8. Custom multi-axis plots

**Usage Pattern**:
```python
from charts import ChartGenerator

generator = ChartGenerator()
chart_html = generator.create_scatter(
    df=data,
    x_col='X_COORD',
    y_col='Value',
    title='Test Results',
    show_limits=True,
    lower_limit=-0.5,
    upper_limit=0.5
)
```

### 5. Reports Module (`src/reports/`)

**Purpose**: Generate professional HTML reports with ST branding.

**Key Components**:

#### 5.1 HTML Template (`html_template.py`)

**ST Corporate Colors**:
- ST Blue: `#03234B` (headers, primary)
- ST Cyan: `#3CB4E6` (accents)
- ST Red: `#E6007E` (highlights)

**Template Structure**:
```html
<!DOCTYPE html>
<html>
  <head>
    <title>Report Title</title>
    <style>ST Corporate Branding</style>
  </head>
  <body>
    <header>Report Info + Summary</header>
    <section>Interactive Charts</section>
    <section>Data Tables</section>
    <footer>Metadata</footer>
  </body>
</html>
```

#### 5.2 Report Generators (`report_generators.py`)

**Base Class**:
```python
class BaseReportGenerator:
    def generate(parameter, data, output_path)
    def calculate_summary_stats(df)
    def create_charts(df)
    def create_tables(df)
```

**Specialized Generators**:

1. **ConditionReportGenerator**
   - Input: Test condition data
   - Output: Per-test condition analysis
   - Charts: Condition distributions, bin analysis

2. **YieldReportGenerator**
   - Input: Volume/test data
   - Output: Yield analysis
   - Charts: Bin pareto, wafer maps, trend analysis

3. **VolumeReportGenerator**
   - Input: Volume test data
   - Output: Statistical process control
   - Charts: Distributions, Cp/CpK charts

4. **LoopTimeReportGenerator**
   - Input: Loop test data
   - Output: Test time analysis
   - Charts: Time trends, per-test breakdown

5. **CharReportGenerator**
   - Input: Multi-corner characterization
   - Output: Corner comparison analysis
   - Charts: Temperature sweeps, corner overlays

### 6. Processors Module (`src/processors/`)

**Purpose**: Orchestrate data processing pipelines.

**Key Components**:

#### 6.1 STDF Processor (`stdf_processor.py`)

**Responsibilities**:
- Load Parquet files
- Apply business logic transformations
- Filter by composite
- Scale results
- Handle retests

**Processing Steps**:
```
Parquet Files
    → Load with Polars
    → Filter by test numbers
    → Apply result scaling
    → Handle unit prefixes
    → Parse test names (regex)
    → Split VDD/Temperature variants
    → Remove retests
    → Return processed DataFrames
```

#### 6.2 Report Processor (`report_processor.py`)

**Responsibilities**:
- Coordinate report generation
- Manage output directories
- Handle multiple composites
- Create completion markers

### 7. Services Module (`src/services/`)

**Purpose**: Business logic layer with reusable services.

**Key Components**:

#### 7.1 File Service (`file_service.py`)

**Operations**:
- File discovery and filtering
- Path validation
- Completion marker management
- Directory creation

#### 7.2 Processing Service (`processing_service.py`)

**Operations**:
- Composite list retrieval (SVN)
- Parameter extraction from paths
- Report path generation
- History tracking

### 8. Core Module (`src/core/`)

**Purpose**: Shared data models and constants.

**Key Components**:

#### 8.1 Models (`models.py`)

**Data Classes**:
```python
@dataclass
class TestRecord:
    test_num: int
    test_name: str
    result: float
    limits: Tuple[float, float]
    unit: str

@dataclass
class ReportMetadata:
    product: str
    lot: str
    wafer: str
    flow: str
    timestamp: datetime
```

#### 8.2 Constants (`constants.py`)

**System Constants**:
```python
# STDF Record Types
RECORD_TYPES = ['ptr', 'ftr', 'mir', 'prr', 'pcr', 'hbr', 'sbr', 'tsr']

# Report Types
REPORT_TYPES = ['CONDITION', 'VOLUME', 'LOOP', 'YIELD', 'CHAR']

# Flow Types
ALLOWED_FLOWS = {'EWS1', 'EWS2', 'EWS3', 'EWSDIE', 'FT', 'FT1', 'FT2', 'EWSCHAR'}

# ST Corporate Colors
ST_COLORS = {
    'blue': '#03234B',
    'cyan': '#3CB4E6',
    'red': '#E6007E'
}
```

#### 8.3 Exceptions (`exceptions.py`)

**Custom Exceptions**:
```python
class STDFProcessingError(Exception)
class InvalidParameterError(Exception)
class ReportGenerationError(Exception)
class FileNotFoundError(Exception)
```

### 9. Utils Module (`src/utils/`)

**Purpose**: Generic utilities and helpers.

**Key Components**:

#### 9.1 Validation (`validation.py`)

- Parameter validation
- Data type checking
- Range validation
- Schema enforcement

#### 9.2 Parallel Processing (`parallel.py`)

- Parallel map operations
- Worker pool management
- Progress tracking
- Error handling in parallel contexts

#### 9.3 File Utilities (`file_utils.py`)

- Path manipulation
- Safe file operations
- Temporary file management
- Archive handling

## Data Flow

### End-to-End Processing Flow

```
1. File Detection
   ├─ Directory Poller scans for new files
   ├─ Classifies by extension (.std, .shm, .html)
   └─ Adds to processing queue

2. STDF Conversion (if .std file)
   ├─ STDFWorker receives file path
   ├─ Decompress if needed
   ├─ Convert to Parquet (parallel)
   ├─ Create completion marker
   └─ Add to report generation queue

3. Report Generation
   ├─ ReportWorker receives parquet path
   ├─ Load composite list from SVN
   ├─ For each composite:
   │   ├─ STDFProcessor: Load and transform data
   │   ├─ ReportGenerator: Generate HTML report
   │   └─ Save to Report/ directory
   └─ Create completion marker

4. Shmoo Processing (if .shm file)
   ├─ ShmooWorker receives .shm path
   ├─ Parse multi-DUT data
   ├─ Generate aggregated plot
   ├─ Save HTML visualization
   └─ Move .shm to shm/ subfolder

5. Condition Processing (if anaflow.html)
   ├─ ConditionWorker receives HTML path
   ├─ Parse condition table
   ├─ For each composite:
   │   ├─ Filter by composite
   │   ├─ Generate condition report
   │   └─ Save to Report/ directory
   └─ Create completion marker
```

## Configuration Management

### Hierarchical Configuration

```
1. System Level (polling.py)
   - ProcessingConfig dataclass
   - Parallel workers
   - Allowed flows
   - Polling interval

2. Product Level (ART.jsonc)
   - Product-specific parameters
   - Wafer map settings
   - Test number mappings
   - Temperature colors

3. Composite Level (SVN composites.cnf)
   - Composite list per product/flow
   - Retrieved dynamically from SVN

4. Runtime Level (Environment)
   - Watch directory path
   - Debug flags
   - Log levels
```

### Configuration Loading Priority

```
Runtime Args > Product ART.jsonc > System Defaults
```

## Error Handling Strategy

### Layered Error Handling

```
1. Worker Level
   ├─ Catch and log exceptions
   ├─ Report to system logger
   └─ Continue processing other files

2. System Level
   ├─ Monitor worker health
   ├─ Retry failed operations (3x)
   └─ Send alerts if needed

3. File Level
   ├─ Skip corrupt files
   ├─ Log detailed error info
   └─ Create error marker

4. User Level
   ├─ Display friendly error messages
   ├─ Provide actionable suggestions
   └─ Include support contact info
```

## Performance Optimizations

### Key Optimizations

1. **Parallel STDF Conversion**
   - 2-4 worker processes
   - 50-75% faster processing
   - Configurable worker count

2. **Polars DataFrames**
   - Columnar memory layout
   - Lazy evaluation
   - Automatic parallelization
   - Zero-copy operations

3. **Parquet Storage**
   - LZ4 compression (~70% size reduction)
   - Columnar encoding
   - Predicate pushdown
   - Fast filtering

4. **Batch Processing**
   - 1000-record batches
   - Reduced memory footprint
   - Streaming-friendly

5. **Caching**
   - Composite list caching
   - Configuration caching
   - Template caching

## Scalability Considerations

### Current Capacity

- **Files per minute**: ~10-20 STDF files (2GB each)
- **Concurrent processing**: 2-4 parallel workers
- **Memory usage**: ~500MB per worker
- **Storage**: Parquet reduces size by 70%

### Scaling Options

1. **Horizontal Scaling**
   - Deploy multiple instances
   - Each monitors different directories
   - Shared network storage

2. **Vertical Scaling**
   - Increase parallel workers (4 → 8)
   - More RAM for larger files
   - SSD for faster I/O

3. **Distributed Processing**
   - Queue-based architecture
   - Worker pool on multiple machines
   - Centralized result storage

## Security Considerations

### Data Protection

1. **Access Control**
   - SVN authentication required
   - Network share permissions
   - Read-only mode for STDF files

2. **Data Integrity**
   - Checksum validation
   - Atomic file operations
   - Completion markers

3. **Audit Trail**
   - Comprehensive logging
   - Processing history (history.parquet)
   - Timestamp tracking

## Monitoring and Logging

### Log Structure

```
log/
├── polling.log          # Directory scanning events
├── stdf2data.log        # Conversion operations
├── data2report.log      # Report generation
├── condition2report.log # Condition processing
└── shmoo.log           # Shmoo visualization
```

### Log Rotation

- **Trigger**: 1000 lines per file
- **Backups**: 1 backup file kept
- **Format**: `%(asctime)s - %(message)s`

### Performance Metrics

```
history.parquet columns:
- path: File location
- file: Filename
- creation_time: Original file timestamp
- end_time: Processing completion
- productcut: Product identifier
- flow: Test flow
- ID: Lot_Wafer identifier
- type: Report type
```

## Future Architecture Enhancements

### Planned Improvements

1. **Database Integration**
   - PostgreSQL for metadata
   - Query interface for reports
   - Historical trend analysis

2. **Web Dashboard**
   - Real-time processing status
   - Interactive report browser
   - Search and filter capabilities

3. **API Layer**
   - RESTful API for data access
   - Webhook notifications
   - Integration with other tools

4. **Advanced Analytics**
   - Machine learning for anomaly detection
   - Predictive yield modeling
   - Automated root cause analysis

## Dependencies

### Core Libraries

```
polars >= 0.19.0        # DataFrame processing
plotly >= 5.14.0        # Interactive charts
numpy >= 1.24.0         # Numerical operations
py7zr                   # 7z decompression
```

### System Requirements

- **Python**: 3.8+
- **Memory**: 4GB minimum, 8GB recommended
- **Storage**: 100GB+ for STDF/Parquet files
- **CPU**: 4+ cores for parallel processing
- **Network**: Access to SVN and network shares

---

**Document Version**: 2.0
**Last Updated**: 2025-10-31
**Author**: Matteo Terranova (MDRF-GPAM)
