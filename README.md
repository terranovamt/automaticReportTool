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

- **Clean Architecture**: SOLID principles with 4-layer separation (Domain, Application, Infrastructure, Presentation)
- **Pure Python Reports**: ~50% faster HTML generation without Jupyter subprocess overhead
- **Automatic Processing**: Monitors directories and automatically processes STDF files
- **Multiple Report Types**: Condition, Stability, Volume, Test Time, Yield, and Characterization reports
- **Interactive Visualizations**: Uses Plotly for rich, interactive charts and graphs
- **High Performance**: Built with Polars for ultra-fast data processing
- **Efficient Storage**: Stores data in Apache Parquet format for optimal performance
- **100% Type Safe**: Full type hints coverage with mypy validation
- **Fully Testable**: Dependency injection throughout, comprehensive test suite

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

## Architecture

**ART.stdf** follows **Clean Architecture** principles with a 4-layer design:

```
automaticReportTool/
├── main.py                          # Entry point
├── src/
│   ├── domain/                      # Domain Layer (Business Models)
│   │   └── models/
│   │       └── parameter.py         # Parameter & FileCorner dataclasses
│   │
│   ├── application/                 # Application Layer (Use Cases)
│   │   ├── use_cases/
│   │   │   ├── convert_stdf_use_case.py
│   │   │   └── generate_report_use_case.py
│   │   └── interfaces/              # Abstract interfaces
│   │
│   ├── infrastructure/              # Infrastructure Layer (External Systems)
│   │   ├── repositories/            # Data persistence
│   │   │   ├── file_repository.py
│   │   │   └── parquet_repository.py
│   │   ├── parsers/                 # Data parsers
│   │   │   └── stdf_parser.py
│   │   └── services/                # Infrastructure services
│   │       ├── file_classifier.py
│   │       └── completion_tracker.py
│   │
│   ├── presentation/                # Presentation Layer (Reports & UI)
│   │   ├── report_generators/       # Pure Python HTML generators
│   │   │   ├── volume_report_generator.py
│   │   │   ├── loop_report_generator.py
│   │   │   ├── ttime_report_generator.py
│   │   │   ├── yield_report_generator.py
│   │   │   └── condition_report_generator.py
│   │   ├── visualizers/             # Chart builders
│   │   │   ├── plotly_builder.py
│   │   │   └── html_builder.py
│   │   └── templates/               # HTML templates
│   │       └── web/                 # CSS, navbar, etc.
│   │
│   ├── polling.py                   # Directory polling system
│   ├── core.py                      # Legacy core (being phased out)
│   ├── stdf2data.py                 # Legacy conversion (being phased out)
│   └── pystdf/                      # STDF parsing library
│
├── tests/                           # Test Suite
│   ├── conftest.py                  # Pytest fixtures
│   ├── unit/                        # Unit tests
│   │   ├── domain/
│   │   ├── application/
│   │   ├── infrastructure/
│   │   └── presentation/
│   └── integration/                 # Integration tests
│
├── scripts/                         # Utility scripts
│   └── analytics/                   # Usage analytics
│
├── doc/                             # Documentation
│   ├── ART.html                     # Original user guide
│   ├── USER_GUIDE.html              # Enhanced user guide
│   └── DEVELOPER_GUIDE.html         # Developer documentation
│
├── ARCHITECTURE.md                  # Architecture documentation
├── MIGRATION_GUIDE.md               # Migration guide
└── README.md                        # This file
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

## Documentation

### For Users
- **[User Guide](doc/USER_GUIDE.html)** - Complete guide for end users
- **[Original Docs](doc/ART.html)** - Original documentation

### For Developers
- **[ARCHITECTURE.md](ARCHITECTURE.md)** - Complete system architecture documentation
- **[MIGRATION_GUIDE.md](MIGRATION_GUIDE.md)** - Guide for migrating from legacy to Clean Architecture
- **[Developer Guide](doc/DEVELOPER_GUIDE.html)** - Technical documentation for developers
- **[Test Documentation](tests/)** - Unit and integration test examples

### API Examples

**Using the new Clean Architecture:**

```python
from src.domain.models.parameter import Parameter
from src.application.use_cases.convert_stdf_use_case import ConvertSTDFUseCase
from src.application.use_cases.generate_report_use_case import GenerateReportUseCase

# Create type-safe parameter object
parameter = Parameter(
    code="44E",
    cut="44EZ",
    flow="EWSCHAR",
    type="CHAR",
    lot="Q445172",
    wafer="05"
)

# Convert STDF to Parquet
convert_use_case = ConvertSTDFUseCase()
parquet_files = convert_use_case.execute(
    stdf_path="/path/to/file.std",
    parameter=parameter
)

# Generate HTML report (pure Python, no Jupyter)
report_use_case = GenerateReportUseCase()
report_path = report_use_case.execute(
    report_type="VOLUME",  # VOLUME, LOOP, TTIME, YIELD, or CONDITION
    parameter=parameter
)

print(f"Report generated: {report_path}")
```

**Benefits of the new architecture:**
- ✅ ~50% faster (no Jupyter subprocess overhead)
- ✅ 100% type-safe with IDE autocomplete
- ✅ Fully testable with dependency injection
- ✅ Better error handling and logging
- ✅ SOLID principles and Clean Architecture

## Technology Stack

### Core Technologies
- **[Python 3.8+](https://www.python.org/)** - Core language with type hints
- **[Polars](https://pola.rs)** - Lightning-fast DataFrame library (100x faster than pandas)
- **[Plotly](https://plotly.com)** - Interactive visualization library
- **[Apache Parquet](https://parquet.apache.org)** - Efficient columnar storage format

### Architecture & Patterns
- **Clean Architecture** - 4-layer separation of concerns
- **SOLID Principles** - Object-oriented design principles
- **Factory Pattern** - Report generator creation
- **Repository Pattern** - Data access abstraction
- **Use Case Pattern** - Business workflow encapsulation
- **Dependency Injection** - Testability and flexibility

### Development Tools
- **[pytest](https://pytest.org)** - Testing framework with fixtures
- **[mypy](http://mypy-lang.org/)** - Static type checker
- **Type Hints** - 100% coverage for IDE autocomplete and validation

## System Architecture

**Clean Architecture** with 4 distinct layers and clear dependency flow:

```
┌─────────────────────────────────────────────────────────────┐
│                    Presentation Layer                        │
│         (Report Generators, Visualizers, Templates)          │
│                                                               │
│  - Pure Python HTML generators (no Jupyter)                  │
│  - Plotly interactive charts                                 │
│  - Factory pattern for report creation                       │
└──────────────────────────▲──────────────────────────────────┘
                           │
                           │ Uses
                           │
┌──────────────────────────┼──────────────────────────────────┐
│                   Application Layer                          │
│            (Use Cases, Business Workflows)                   │
│                                                               │
│  - ConvertSTDFUseCase: STDF → Parquet                       │
│  - GenerateReportUseCase: Data → HTML Reports               │
│  - Dependency Injection throughout                           │
└──────────────────────────▲──────────────────────────────────┘
                           │
                           │ Uses
                           │
┌──────────────────────────┼──────────────────────────────────┐
│                  Infrastructure Layer                        │
│         (External Systems, I/O, Persistence)                 │
│                                                               │
│  - STDFParser: Wraps pystdf library                         │
│  - FileRepository: File system operations                    │
│  - ParquetRepository: Parquet data access                    │
│  - Services: FileClassifier, CompletionTracker               │
└──────────────────────────▲──────────────────────────────────┘
                           │
                           │ Uses
                           │
┌──────────────────────────┼──────────────────────────────────┐
│                      Domain Layer                            │
│                  (Core Business Models)                      │
│                                                               │
│  - Parameter: Type-safe test parameter model                 │
│  - FileCorner: File location and metadata                    │
│  - No external dependencies (Pure Python)                    │
└──────────────────────────────────────────────────────────────┘

                    Data Flow Example:

    STDF File → STDFParser → Parquet → ReportGenerator → HTML
        ↓           ↓           ↓            ↓             ↓
    Infrastructure  Infra   Repository  Presentation  Presentation
```

**Key Architectural Principles:**

- **Separation of Concerns**: Each layer has a single responsibility
- **Dependency Inversion**: Outer layers depend on inner layers (never reverse)
- **Testability**: All components fully unit-testable via dependency injection
- **Type Safety**: 100% type hints coverage with mypy validation
- **SOLID Principles**: Applied throughout the codebase

For detailed architecture documentation, see [ARCHITECTURE.md](ARCHITECTURE.md).

## Performance

### New Architecture Improvements
- **~50% Faster Report Generation**: Pure Python generators vs. Jupyter subprocess (5-10s saved per report)
- **Processing Speed**: Handles large STDF files (>1GB) in minutes
- **Memory Efficiency**: Columnar storage (Parquet + Polars) allows processing datasets larger than RAM
- **Concurrent Processing**: Multi-threaded operations for faster throughput
- **Lazy Evaluation**: Polars lazy frames for optimized query execution

### Benchmark Results

| Operation | Legacy (Jupyter) | New (Python) | Improvement |
|-----------|-----------------|--------------|-------------|
| VOLUME Report | ~12s | ~6s | **50% faster** |
| LOOP Report | ~10s | ~5s | **50% faster** |
| TTIME Report | ~8s | ~4s | **50% faster** |
| STDF Parsing | ~45s | ~45s | Same (pystdf) |
| Parquet I/O | ~2s | ~2s | Same (Polars) |

**Total workflow time reduction: ~30-40% overall**

## Logging

Logs are stored in the `log/` directory:

- `polling.log` - Directory polling events
- `stdf2data.log` - STDF conversion logs
- `data2report.log` - Report generation logs
- `condition2report.log` - Condition report logs
- `shmoo.log` - Shmoo processing logs

Each log automatically rotates after 1000 lines.

## Testing

The project includes a comprehensive test suite following Clean Architecture principles:

```bash
# Run all tests
pytest tests/ -v

# Run only unit tests
pytest tests/unit/ -v -m unit

# Run with coverage report
pytest tests/ --cov=src --cov-report=html

# Type checking with mypy
mypy src/ --strict

# Run specific test file
pytest tests/unit/domain/test_parameter.py -v
```

### Test Organization

```
tests/
├── conftest.py              # Shared fixtures and configuration
├── unit/                    # Unit tests (isolated, fast)
│   ├── domain/             # Domain model tests
│   ├── application/        # Use case tests
│   ├── infrastructure/     # Repository/parser tests
│   └── presentation/       # Report generator tests
└── integration/            # Integration tests (slower)
```

### Writing Tests

Example using pytest fixtures:

```python
def test_convert_stdf_use_case(sample_parameter_object, mock_stdf_parser, test_logger):
    """Test STDF conversion with dependency injection."""
    use_case = ConvertSTDFUseCase(
        stdf_parser=mock_stdf_parser,
        logger=test_logger
    )

    result = use_case.execute(
        stdf_path="/test.std",
        parameter=sample_parameter_object
    )

    assert result is not None
    mock_stdf_parser.parse_to_parquet.assert_called_once()
```

## Contributing

Contributions are welcome! Please follow these guidelines:

1. **Read Documentation**: Review [ARCHITECTURE.md](ARCHITECTURE.md) and [MIGRATION_GUIDE.md](MIGRATION_GUIDE.md)
2. **Follow Clean Architecture**: Maintain 4-layer separation
3. **Add Type Hints**: 100% coverage required
4. **Write Tests**: Unit tests for all new code
5. **Run Validation**: `pytest` and `mypy` must pass
6. **Update Docs**: Document significant changes

Contact the development team before making significant changes.

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
