# Modular Architecture Guide

## Overview

ART.stdf now features a clean, modular architecture that separates concerns and makes the codebase more maintainable, testable, and extensible.

## Architecture Layers

```
┌─────────────────────────────────────────────────────────┐
│                    User Interface                        │
│              (CLI, API, Web Interface)                   │
└───────────────────────┬─────────────────────────────────┘
                        │
┌───────────────────────▼─────────────────────────────────┐
│                   Service Layer                          │
│  ┌──────────────────┐    ┌──────────────────────────┐  │
│  │ ProcessingService│    │    FileService           │  │
│  │  - Orchestration │    │  - File discovery        │  │
│  │  - Pipeline mgmt │    │  - Status tracking       │  │
│  └──────────────────┘    └──────────────────────────┘  │
└───────────────────────┬─────────────────────────────────┘
                        │
┌───────────────────────▼─────────────────────────────────┐
│                   Processor Layer                        │
│  ┌─────────────┐  ┌──────────────┐  ┌──────────────┐  │
│  │STDF         │  │Report        │  │Char          │  │
│  │Processor    │  │Processor     │  │Processor     │  │
│  └─────────────┘  └──────────────┘  └──────────────┘  │
└───────────────────────┬─────────────────────────────────┘
                        │
┌───────────────────────▼─────────────────────────────────┐
│                      Core Layer                          │
│  ┌─────────┐  ┌───────────┐  ┌───────────┐            │
│  │ Models  │  │Exceptions │  │Constants  │            │
│  └─────────┘  └───────────┘  └───────────┘            │
└───────────────────────┬─────────────────────────────────┘
                        │
┌───────────────────────▼─────────────────────────────────┐
│                   Utilities Layer                        │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐             │
│  │File Utils│  │Validation│  │Parallel  │             │
│  └──────────┘  └──────────┘  └──────────┘             │
└─────────────────────────────────────────────────────────┘
```

## Directory Structure

```
automaticReportTool/
├── config/                  # Configuration management
│   ├── __init__.py
│   ├── settings.py         # Global settings
│   ├── paths.py            # Path configuration
│   └── logging_config.py   # Logging setup
│
├── src/
│   ├── core/               # Core domain models
│   │   ├── __init__.py
│   │   ├── models.py       # Data models
│   │   ├── exceptions.py   # Custom exceptions
│   │   └── constants.py    # Application constants
│   │
│   ├── processors/         # Processing components
│   │   ├── __init__.py
│   │   ├── base.py         # Base processor interface
│   │   ├── stdf_processor.py      # STDF conversion
│   │   └── report_processor.py    # Report generation
│   │
│   ├── services/           # Business logic orchestration
│   │   ├── __init__.py
│   │   ├── processing_service.py  # Main service
│   │   └── file_service.py        # File management
│   │
│   ├── utils/              # Utility functions
│   │   ├── __init__.py
│   │   ├── file_utils.py   # File operations
│   │   ├── validation.py   # Input validation
│   │   └── parallel.py     # Parallel processing
│   │
│   └── pystdf/             # STDF parsing library
│       ├── IO.py
│       ├── Importer.py
│       └── V4.py
│
├── docs/                   # Documentation
│   ├── ARCHITECTURE.md
│   ├── API.md
│   └── CONFIGURATION.md
│
└── examples/               # Usage examples
    └── using_modular_architecture.py
```

## Key Components

### 1. Service Layer

#### ProcessingService
Main orchestrator for STDF processing pipeline.

**Responsibilities:**
- Coordinate file discovery and processing
- Manage parallel processing
- Track progress and results
- Generate statistics

**Example:**
```python
from src.services import ProcessingService

service = ProcessingService()
results = service.process_directory(
    input_dir=Path("./STDF"),
    parallel=True
)
```

#### FileService
Manages file discovery and status tracking.

**Responsibilities:**
- Discover STDF files
- Track file status (pending, processing, completed, failed)
- Wait for file stability
- Provide statistics

**Example:**
```python
from src.services import FileService

file_service = FileService()
stdf_files = file_service.discover_stdf_files(
    directory=Path("./STDF"),
    recursive=True,
    wait_stable=True
)
```

### 2. Processor Layer

#### BaseProcessor
Abstract base class for all processors.

**Interface:**
```python
class BaseProcessor(ABC):
    @abstractmethod
    def validate_input(self, input_path: Path) -> bool:
        pass

    @abstractmethod
    def process(self, input_path: Path, output_path: Path) -> ProcessingResult:
        pass
```

#### STDFProcessor
Converts STDF files to Parquet format.

**Features:**
- Uses optimized pystdf library
- Handles compressed files (.gz)
- Generates multiple Parquet files (one per record type)
- Includes PART_ID for merging

**Example:**
```python
from src.processors.stdf_processor import STDFProcessor

processor = STDFProcessor()
result = processor.process(
    input_path=Path("file.std.gz"),
    output_path=Path("./output/")
)
```

#### Report Processors
Generate different types of reports.

**Available Processors:**
- `ConditionReportProcessor` - Condition reports
- `CharReportProcessor` - Characterization reports
- `ShmooReportProcessor` - Shmoo plot reports

**Example:**
```python
from src.processors.report_processor import ReportProcessorFactory
from src.core.models import ReportType

processor = ReportProcessorFactory.create(ReportType.CONDITION)
result = processor.process(
    input_path=Path("./data"),
    output_path=Path("./reports")
)
```

### 3. Core Layer

#### Models
Data models using dataclasses.

**Key Models:**
- `STDFFile` - STDF file representation
- `ProcessingResult` - Processing outcome
- `Parameter` - Test parameters
- `STDFData` - Parsed STDF data container
- `Report` - Generated report metadata

**Enums:**
- `FileStatus` - File processing status
- `ProcessType` - Type of processing
- `ReportType` - Type of report

#### Exceptions
Custom exception hierarchy.

```python
from src.core.exceptions import (
    ARTError,           # Base exception
    ConfigurationError, # Configuration issues
    ValidationError,    # Input validation failures
    ProcessingError,    # Processing failures
    ParsingError        # STDF parsing errors
)
```

#### Constants
Application-wide constants.

```python
from src.core.constants import (
    STDF_EXTENSIONS,         # Valid STDF file extensions
    COMPRESSION_TYPES,       # Supported compressions
    RECORD_TYPES,            # STDF record types
    TEST_RECORD_TYPES,       # Test records
    PART_ID_RECORDS         # Records with PART_ID
)
```

### 4. Utilities Layer

#### file_utils
File operations and management.

**Functions:**
- `ensure_directory()` - Create directory if needed
- `get_file_checksum()` - Calculate file hash
- `is_file_locked()` - Check if file is locked
- `safe_remove_file()` - Safely delete file
- `safe_remove_directory()` - Safely delete directory
- `get_file_size_mb()` - Get file size
- `find_files_by_extension()` - Find files by extension
- `wait_for_file_stable()` - Wait for file to stop growing
- `copy_file_safe()` - Safe file copy
- `move_file_safe()` - Safe file move
- `get_temp_directory()` - Create temp directory
- `cleanup_old_files()` - Remove old files

#### validation
Input validation functions.

**Functions:**
- `validate_stdf_file()` - Validate STDF file
- `validate_parameter()` - Validate parameter dict
- `validate_dataframe()` - Validate DataFrame columns

#### parallel
Parallel processing utilities.

**Functions:**
- `parallel_process_files()` - Process files in parallel
- `process_with_pool()` - ProcessPoolExecutor wrapper

### 5. Configuration

#### Settings
Global configuration singleton.

**Usage:**
```python
from config.settings import settings

# Access settings
workers = settings.processing.parallel_stdf_workers
compression = settings.processing.compression
log_level = settings.logging.log_level

# Modify settings
settings.processing.parallel_stdf_workers = 4
settings.processing.compression = "lz4"
```

**Configuration Options:**
- `ProcessingConfig` - Processing settings
- `FlowConfig` - Test flow validation
- `LoggingConfig` - Logging configuration
- `ReportConfig` - Report generation settings

## Usage Patterns

### Pattern 1: Simple Processing

```python
from pathlib import Path
from src.services import ProcessingService

# Create service and process
service = ProcessingService()
results = service.process_directory(
    input_dir=Path("./STDF"),
    parallel=True
)

# Check results
for result in results:
    print(f"{result.file_path}: {'✓' if result.success else '✗'}")
```

### Pattern 2: Complete Pipeline

```python
from pathlib import Path
from src.services import ProcessingService

service = ProcessingService()

# Step 1: Convert STDF to Parquet
stdf_results = service.process_directory(
    input_dir=Path("./STDF"),
    output_dir=Path("./data"),
    parallel=True
)

# Step 2: Generate reports
report_results = service.generate_reports(
    data_dir=Path("./data"),
    output_dir=Path("./reports")
)

# Step 3: Check statistics
stats = service.get_statistics()
print(f"Success rate: {stats['success_rate']*100:.1f}%")
```

### Pattern 3: Custom Processing

```python
from pathlib import Path
from src.processors.stdf_processor import STDFProcessor
from src.core.models import STDFFile, FileStatus

# Create processor
processor = STDFProcessor()

# Process single file
result = processor.process(
    input_path=Path("file.std"),
    output_path=Path("./output")
)

if result.success:
    print(f"Created {len(result.output_files)} files")
    print(f"Processing time: {result.processing_time:.2f}s")
```

## Benefits

### 1. **Separation of Concerns**
- Each layer has clear responsibilities
- Easy to understand and maintain
- Changes isolated to specific components

### 2. **Testability**
- Components can be tested independently
- Easy to mock dependencies
- Clear interfaces for testing

### 3. **Extensibility**
- Easy to add new processors
- Simple to add new services
- Plugin architecture ready

### 4. **Reusability**
- Utilities can be used anywhere
- Processors can be combined
- Services can be orchestrated

### 5. **Maintainability**
- Code organized by function
- Easy to find relevant code
- Clear dependencies

## Migration Guide

### From Old Code

**Before:**
```python
# Old monolithic approach
import src.stdf2data as stdf2data
stdf2data.stdf2data_converter(input_file, output_dir)
```

**After:**
```python
# New modular approach
from src.services import ProcessingService

service = ProcessingService()
results = service.process_directory(input_dir)
```

### Compatibility

The new modular architecture is **fully compatible** with existing code. Old functions still work, but new code should use the modular approach.

## Best Practices

1. **Use Services for orchestration**
   - Don't call processors directly from main code
   - Use `ProcessingService` for coordination

2. **Use Processors for specific tasks**
   - One processor per conversion/report type
   - Processors should be stateless

3. **Use Core models for data**
   - Always use dataclasses from `src.core.models`
   - Type hints for better IDE support

4. **Use Utilities for common operations**
   - Don't reimplement file operations
   - Use validated utility functions

5. **Configure via Settings**
   - Don't hardcode configuration
   - Use `settings` singleton

## Future Enhancements

Planned improvements:
1. **Dependency Injection** - Better testability
2. **Event System** - Progress notifications
3. **Plugin Architecture** - Custom processors
4. **Web API** - REST API for services
5. **Async Processing** - Async/await support

## See Also

- [Performance Optimizations](PERFORMANCE_OPTIMIZATIONS.md)
- [API Reference](API.md)
- [Configuration Guide](CONFIGURATION.md)
- [Architecture Documentation](ARCHITECTURE.md)
