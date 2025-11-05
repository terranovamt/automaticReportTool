# ART.stdf - Architecture Documentation

**Version:** 2.0.0
**Author:** Matteo Terranova (matteo.terranova@st.com)
**Organization:** STMicroelectronics - MDRF GPAM
**Last Updated:** November 2025

## Table of Contents

1. [Overview](#overview)
2. [Architectural Principles](#architectural-principles)
3. [System Architecture](#system-architecture)
4. [Layer Details](#layer-details)
5. [Data Flow](#data-flow)
6. [Key Components](#key-components)
7. [Design Patterns](#design-patterns)
8. [Technology Stack](#technology-stack)

## Overview

ART.stdf is an automated system for processing STDF (Standard Test Data Format) files from semiconductor testing and generating comprehensive analytical reports. The system follows Clean Architecture principles with a layered approach that ensures maintainability, testability, and scalability.

### System Purpose

- **Parse** STDF files from semiconductor test equipment
- **Convert** binary STDF to Parquet format for efficient processing
- **Analyze** test data with statistical methods
- **Generate** HTML reports with interactive visualizations
- **Monitor** directories for automatic processing

### Key Features

- ✅ Automatic STDF file detection and processing
- ✅ Multi-format compression support (7z, gz, zip, etc.)
- ✅ Parquet-based data processing (ultra-fast with Polars)
- ✅ Interactive HTML reports with Plotly visualizations
- ✅ Pure Python report generation (no Jupyter dependency)
- ✅ Modular, testable architecture
- ✅ 100% type hints coverage

## Architectural Principles

### Clean Architecture

The system follows Uncle Bob's Clean Architecture with strict layer separation:

```
┌─────────────────────────────────────────────────────────────┐
│                      Presentation Layer                      │
│         (Report Generators, Visualizers, Templates)          │
└──────────────────────────▲──────────────────────────────────┘
                           │
┌──────────────────────────┼──────────────────────────────────┐
│                   Application Layer                          │
│           (Use Cases, Services, Orchestration)               │
└──────────────────────────▲──────────────────────────────────┘
                           │
┌──────────────────────────┼──────────────────────────────────┐
│                     Domain Layer                             │
│         (Models, Interfaces, Business Logic)                 │
└──────────────────────────▲──────────────────────────────────┘
                           │
┌──────────────────────────┼──────────────────────────────────┐
│                  Infrastructure Layer                        │
│      (Parsers, Storage, External Dependencies)               │
└─────────────────────────────────────────────────────────────┘
```

### SOLID Principles

1. **Single Responsibility**: Each class has one reason to change
2. **Open/Closed**: Open for extension, closed for modification
3. **Liskov Substitution**: Subtypes are substitutable for base types
4. **Interface Segregation**: Clients don't depend on unused interfaces
5. **Dependency Inversion**: Depend on abstractions, not concretions

### Additional Principles

- **DRY** (Don't Repeat Yourself): Code reuse through abstraction
- **Dependency Injection**: Constructor injection for testability
- **Port and Adapters**: External systems accessed through interfaces

## System Architecture

### Project Structure

```
automaticReportTool/
├── config/                      # Configuration layer
│   ├── settings.py              # Central configuration
│   └── logging_config.py        # Logging setup
│
├── src/
│   ├── domain/                  # Domain layer (core business logic)
│   │   ├── models/              # Domain models (Parameter, etc.)
│   │   └── interfaces/          # Port interfaces (IParser, IRepository)
│   │
│   ├── application/             # Application layer (use cases)
│   │   ├── use_cases/           # Business workflows
│   │   │   ├── convert_stdf_use_case.py
│   │   │   ├── generate_report_use_case.py
│   │   │   └── process_*.py
│   │   ├── services/            # Application services
│   │   │   ├── file_classifier.py
│   │   │   ├── completion_tracker.py
│   │   │   ├── processing_orchestrator.py
│   │   │   └── directory_monitor.py
│   │   └── dtos/                # Data Transfer Objects
│   │
│   ├── infrastructure/          # Infrastructure layer (external)
│   │   ├── logging/             # Custom logging handlers
│   │   ├── storage/             # File and Parquet repositories
│   │   ├── parsers/             # STDF parser adapters
│   │   └── pystdf/              # STDF parsing library (21 modules)
│   │
│   ├── presentation/            # Presentation layer (UI/reports)
│   │   ├── report_generators/   # Pure Python HTML generators (5 types)
│   │   ├── visualizers/         # Plotly and HTML builders
│   │   └── templates/           # HTML/CSS/JS templates
│   │
│   └── legacy/                  # Legacy code (to be refactored)
│       ├── core.py              # Original processing logic
│       ├── condition.py
│       ├── shmoo.py
│       └── charv3.py
│
├── tests/                       # Test suite
│   ├── unit/                    # Unit tests per layer
│   ├── integration/             # Integration tests
│   └── conftest.py              # Pytest fixtures
│
├── scripts/                     # Utility scripts
│   └── analytics/               # Usage analytics
│
└── docs/                        # Documentation
    ├── ARCHITECTURE.md          # This file
    ├── MIGRATION_GUIDE.md       # Migration guide
    └── API_REFERENCE.md         # API documentation
```

## Layer Details

### 1. Domain Layer (`src/domain/`)

The **innermost layer** containing business logic and models. No dependencies on outer layers.

#### Components

**Models** (`domain/models/`):
- `Parameter`: Core data model for test parameters
- `FileCorner`: Test corner and file path information
- `report_types.py`: Enumerations (ProcessType, ReportType, FlowType, PackageType)

**Interfaces** (`domain/interfaces/`):
- `IParser`: Parser abstraction
- `ISTDFParser`: STDF-specific parser interface
- `IFileRepository`: File storage abstraction
- `IParquetRepository`: Parquet storage abstraction

**Key Principles**:
- Pure Python dataclasses
- 100% type hints
- No external dependencies
- Business rules enforcement

**Example**:
```python
@dataclass
class Parameter:
    code: str
    cut: str
    flow: str
    type: str
    lot: str
    wafer: str
    file: Dict[str, FileCorner] = field(default_factory=dict)

    @property
    def is_ews_flow(self) -> bool:
        """Check if this is an EWS flow."""
        return self.flow.upper().startswith("EWS")
```

### 2. Application Layer (`src/application/`)

Orchestrates business logic and coordinates between layers.

#### Use Cases (`application/use_cases/`)

Each use case represents a single business workflow:

1. **ConvertSTDFUseCase** (186 lines)
   - Converts STDF files to Parquet format
   - Handles decompression automatically
   - Validates and logs conversion

2. **GenerateReportUseCase** (237 lines)
   - Generates HTML reports from data
   - Determines report type (VOLUME, TTIME, YIELD, CONDITION)
   - Uses appropriate report generator

3. **ProcessConditionUseCase** (155 lines)
   - Processes condition test files
   - Generates condition reports

4. **ProcessShmooUseCase** (107 lines)
   - Processes shmoo plot data
   - Generates shmoo visualizations

5. **ProcessCharUseCase** (178 lines)
   - Processes characterization data
   - Generates char reports

#### Services (`application/services/`)

Application-level services for cross-cutting concerns:

1. **FileClassifier** (377 lines)
   - Classifies files by type (STDF, data, report, etc.)
   - Validates file names and paths
   - Extracts product/flow information

2. **CompletionTracker** (365 lines)
   - Tracks processing completion
   - Manages completion markers
   - Caches completion status

3. **ProcessingOrchestrator** (420 lines)
   - Central coordination hub
   - Orchestrates entire processing cycle
   - Manages dependencies between steps

4. **DirectoryMonitor** (480 lines)
   - Scans directories for new files
   - Validates directory hierarchy
   - Filters by product/flow

#### DTOs (`application/dtos/`)

- `ProcessingResultDTO`: Results of processing operations
- `CycleResultDTO`: Results of processing cycles
- `ScanResult`: Directory scan results

### 3. Infrastructure Layer (`src/infrastructure/`)

Handles external systems and technical concerns.

#### Logging (`infrastructure/logging/`)

- `LineCountRotatingFileHandler`: Custom log rotation by line count
- `LoggerFactory`: Centralized logger creation

#### Storage (`infrastructure/storage/`)

1. **FileRepository** (205 lines)
   - File system operations
   - Completion marker management
   - File finding with glob patterns

2. **CompressionHandler** (320 lines)
   - Multi-format decompression (7z, gz, zip, bz2, xz, tar, rar)
   - Automatic format detection
   - Cleanup after extraction

#### Parsers (`infrastructure/parsers/`)

1. **STDFParser** (150 lines)
   - High-level STDF parsing adapter
   - Wraps pystdf library
   - Parquet output generation

2. **pystdf/** (21 modules)
   - Low-level STDF binary parsing
   - Record type handling
   - Preserved from original implementation

### 4. Presentation Layer (`src/presentation/`)

Generates user-facing reports and visualizations.

#### Report Generators (`presentation/report_generators/`)

Pure Python HTML generators (replaced Jupyter notebooks):

1. **BaseReportGenerator** (222 lines)
   - Abstract base class
   - Common HTML structure
   - Template loading

2. **VolumeReportGenerator** (690 lines)
   - Volume production reports
   - PTR/FTR analysis
   - Statistical tables

3. **LoopReportGenerator** (185 lines)
   - Loop test analysis
   - Stability tracking

4. **TTimeReportGenerator** (145 lines)
   - Test time analysis
   - Time distribution charts

5. **YieldReportGenerator** (165 lines)
   - Yield analysis
   - Pareto charts

6. **ConditionReportGenerator** (165 lines)
   - Test condition analysis
   - Corner coverage

**Factory Pattern**:
```python
generator = create_report_generator(
    report_type="VOLUME",  # or LOOP, TTIME, YIELD, CONDITION
    parameter=parameter,
    logger=logger
)
report_path = generator.generate(data_path, output_path, df_stdf)
```

#### Visualizers (`presentation/visualizers/`)

1. **plotly_builder.py** (1,225 lines)
   - Interactive histograms
   - Box plots with corner splits
   - Scatter plots
   - Heatmaps
   - Shmoo plots

2. **html_builder.py** (420 lines)
   - Color-coded tables (PTR, FTR, limits)
   - CSV export functionality
   - Interactive filtering

#### Templates (`presentation/templates/`)

- HTML templates (navbar, footer, etc.)
- CSS styling (ST branding)
- JavaScript for interactivity

## Data Flow

### STDF Processing Flow

```
┌─────────────────┐
│   STDF Files    │ (Binary test data)
│  (.std, .stdf)  │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Decompression   │ (If .7z, .gz, etc.)
│     Handler     │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  STDF Parser    │ (pystdf library)
│ Binary → Dict   │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Parquet Writer  │ (Polars DataFrame)
│ .ptr, .ftr, ... │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Report Generator│ (Pure Python)
│   HTML + Plotly │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  HTML Report    │ (Interactive, standalone)
└─────────────────┘
```

### Processing Cycle

```
1. Directory Monitor scans for new STDF files
   ↓
2. File Classifier validates file type and path
   ↓
3. Completion Tracker checks if already processed
   ↓
4. ConvertSTDFUseCase converts STDF → Parquet
   ↓
5. GenerateReportUseCase determines report type
   ↓
6. Report Generator creates HTML report
   ↓
7. Completion Tracker marks as complete
```

## Key Components

### Processing Orchestrator

Central hub coordinating the entire processing pipeline:

```python
orchestrator = ProcessingOrchestrator(
    convert_use_case=convert_use_case,
    generate_report_use_case=report_use_case,
    process_condition_use_case=condition_use_case,
    process_shmoo_use_case=shmoo_use_case,
    process_char_use_case=char_use_case,
)

result = orchestrator.process_cycle(
    stdf_files=stdf_files,
    data_files=data_files,
    condition_files=condition_files,
    shmoo_files=shmoo_files,
    char_files=char_files,
)
```

### Parameter Model

Core data model throughout the system:

```python
# Legacy format (dict)
parameter_dict = {
    "CODE": "44E",
    "LOT": "Q445172",
    "WAFER": "05",
    # ... more fields
}

# New format (dataclass)
parameter = Parameter(
    code="44E",
    lot="Q445172",
    wafer="05",
    # ... more fields
)

# Bidirectional conversion
param = Parameter.from_dict(parameter_dict)
dict_data = param.to_dict()
```

## Design Patterns

### 1. Factory Pattern

**Report Generator Factory**:
```python
def create_report_generator(report_type, parameter, logger):
    generators = {
        "VOLUME": VolumeReportGenerator,
        "LOOP": LoopReportGenerator,
        # ...
    }
    return generators[report_type](parameter, logger)
```

### 2. Repository Pattern

**FileRepository**:
```python
class FileRepository:
    @staticmethod
    def check_completion_marker(path: str) -> bool:
        # Abstract file system operations
        pass
```

### 3. Adapter Pattern

**STDFParser** adapts pystdf library:
```python
class STDFParser:
    def parse_to_parquet(self, stdf_path, output_dir):
        # Wraps pystdf with high-level interface
        pass
```

### 4. Use Case Pattern

**Each business workflow is a use case**:
```python
class ConvertSTDFUseCase:
    def execute(self, stdf_path, parameter):
        # 1. Validate
        # 2. Log start
        # 3. Execute conversion
        # 4. Verify results
        # 5. Log completion
        pass
```

### 5. Strategy Pattern

**Report generation strategy varies by type**:
```python
# Different strategies for different report types
if report_type == "TTIME":
    generator = TTimeReportGenerator()
elif report_type == "YIELD":
    generator = YieldReportGenerator()
```

### 6. Template Method Pattern

**BaseReportGenerator defines template**:
```python
class BaseReportGenerator(ABC):
    def generate(self, data_path, output_path):
        header = self.build_html_header()
        content = self._generate_content()  # Subclass implements
        footer = self.build_html_footer()
        return header + content + footer
```

## Technology Stack

### Core Technologies

| Component | Technology | Version | Purpose |
|-----------|-----------|---------|---------|
| **Language** | Python | 3.8+ | Core development |
| **Data Processing** | Polars | 0.19+ | Fast DataFrame operations |
| **Visualization** | Plotly | 5.17+ | Interactive charts |
| **STDF Parsing** | pystdf | Custom | Binary STDF parsing |
| **Compression** | py7zr | 0.20+ | 7z archive support |
| **Testing** | pytest | 7.4+ | Test framework |
| **Type Checking** | mypy | 1.4+ | Static type analysis |

### Why These Technologies?

1. **Polars over Pandas**:
   - 5-10x faster for large datasets
   - Better memory efficiency
   - Lazy evaluation support
   - Native Parquet support

2. **Plotly over Matplotlib**:
   - Interactive charts
   - Standalone HTML export
   - Professional appearance
   - No server required

3. **Pure Python over Jupyter**:
   - Faster execution (no subprocess)
   - Better testability
   - Easier debugging
   - Version control friendly

### Dependencies

**Production**:
```
polars>=0.19.0       # Data processing
plotly>=5.17.0       # Visualization
py7zr>=0.20.0        # Compression
scipy>=1.11.0        # Statistics
json5>=0.9.0         # Config files
beautifulsoup4       # HTML parsing
```

**Development**:
```
pytest>=7.4.0        # Testing
pytest-cov>=4.1.0    # Coverage
black>=23.7.0        # Formatting
mypy>=1.4.0          # Type checking
```

## Performance Characteristics

### Benchmarks

| Operation | Performance | Notes |
|-----------|-------------|-------|
| STDF → Parquet | ~1-2 MB/s | Depends on record count |
| Parquet Load | ~50-100 MB/s | Polars parallel loading |
| Report Generation | ~2-5 seconds | Pure Python (was ~10s with Jupyter) |
| Directory Scan | ~1000 files/s | Cached completion status |

### Memory Usage

- **STDF Parsing**: ~2x file size (peak)
- **Parquet Processing**: ~1.5x file size (streaming)
- **Report Generation**: ~100-200 MB (typical)

### Scalability

- ✅ Handles files up to 2GB (tested)
- ✅ Concurrent processing safe (file-based locking)
- ✅ Memory-efficient streaming where possible
- ⚠️ Single-threaded (can be parallelized in future)

## Security Considerations

1. **File Path Validation**:
   - All paths validated before use
   - No arbitrary file access
   - Directory traversal prevention

2. **Subprocess Usage**:
   - Minimal subprocess usage
   - No shell=True with user input
   - Controlled command execution

3. **Data Privacy**:
   - All processing local
   - No external API calls
   - No telemetry or tracking

## Future Enhancements

### Planned (Phase 8+)

1. **Parallel Processing**:
   - Multi-file concurrent processing
   - Thread pool for report generation

2. **API Layer**:
   - REST API for remote processing
   - Web dashboard for monitoring

3. **Database Integration**:
   - Historical data storage
   - Trend analysis across lots

4. **Cloud Support**:
   - S3/Azure Blob storage
   - Cloud-native deployment

### Under Consideration

- Real-time streaming processing
- Machine learning for anomaly detection
- Advanced statistical process control
- Multi-site aggregation

## Conclusion

The ART.stdf architecture follows industry best practices with Clean Architecture, SOLID principles, and modern Python patterns. The layered approach ensures:

- ✅ **Maintainability**: Clear separation of concerns
- ✅ **Testability**: Dependency injection throughout
- ✅ **Scalability**: Modular components
- ✅ **Performance**: Optimized data processing
- ✅ **Reliability**: Type-safe, well-tested code

For migration from legacy code, see [MIGRATION_GUIDE.md](MIGRATION_GUIDE.md).
For API details, see [API_REFERENCE.md](API_REFERENCE.md).

---

**Document Version**: 1.0
**Architecture Version**: 2.0.0
**Last Review**: November 2025
