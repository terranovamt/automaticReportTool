# ART.stdf Architecture Documentation

## Overview

ART.stdf (Automatic Report Tool for STDF) is a modular, high-performance system for processing semiconductor test data in STDF format and generating comprehensive HTML reports.

## Design Principles

1. **Modularity**: Clear separation of concerns with dedicated modules
2. **Type Safety**: Extensive use of type hints and data validation
3. **Performance**: Optimized with Polars, parallel processing, and efficient I/O
4. **Maintainability**: Clean code structure with comprehensive documentation
5. **Extensibility**: Easy to add new report types and processors

## System Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     Entry Point (main.py)                    │
└───────────────────────────────┬─────────────────────────────┘
                                │
                ┌───────────────┴───────────────┐
                │    Configuration System        │
                │  (config/)                     │
                │  - settings.py                 │
                │  - paths.py                    │
                │  - logging_config.py           │
                └───────────────┬───────────────┘
                                │
        ┌───────────────────────┴───────────────────────┐
        │            Directory Monitoring                │
        │         (src/monitoring/poller.py)             │
        └───────────────────────┬───────────────────────┘
                                │
                ┌───────────────┴───────────────┐
                │      Processing Pipeline       │
                │     (src/processors/)          │
                │                                │
                │  ┌─────────────────────────┐  │
                │  │   STDF Processor        │  │
                │  │   (stdf_processor.py)   │  │
                │  │   - STDF → Parquet      │  │
                │  └──────────┬──────────────┘  │
                │             │                  │
                │  ┌──────────┴──────────────┐  │
                │  │  Report Processors      │  │
                │  │  - Condition            │  │
                │  │  - Stability/Volume     │  │
                │  │  - Char                 │  │
                │  │  - Shmoo                │  │
                │  └──────────┬──────────────┘  │
                └─────────────┼─────────────────┘
                              │
              ┌───────────────┴───────────────┐
              │    Report Generation           │
              │   (src/generators/)            │
              │   - html_generator.py          │
              │   - chart_generator.py         │
              └───────────────┬───────────────┘
                              │
                     ┌────────┴────────┐
                     │  Output Files   │
                     │  - HTML Reports │
                     │  - Charts       │
                     │  - Parquet Data │
                     └─────────────────┘
```

## Module Structure

### 1. Configuration (`config/`)

**Purpose**: Centralized configuration management with validation

**Files**:
- `settings.py`: Global settings (processing, flow, logging, reports)
- `paths.py`: Path management and validation
- `logging_config.py`: Logging setup with rotation

**Key Classes**:
- `Settings`: Singleton for global configuration
- `ProcessingConfig`: Processing parameters (workers, compression, etc.)
- `FlowConfig`: Test flow validation rules
- `PathConfig`: Path management
- `LoggingConfig`: Logging configuration

### 2. Core (`src/core/`)

**Purpose**: Fundamental data structures and business logic

**Files**:
- `models.py`: Data models (STDFFile, Parameter, ProcessingResult, etc.)
- `exceptions.py`: Custom exception hierarchy
- `constants.py`: Application-wide constants

**Key Models**:
```python
- ProcessType: Enum for processing types
- FileStatus: File processing status
- ReportType: Report types
- STDFFile: STDF file representation
- ProcessingResult: Processing outcome
- Parameter: Extracted test parameters
- STDFData: Container for STDF DataFrames
- Report: Generated report metadata
```

### 3. Processors (`src/processors/`)

**Purpose**: Processing pipeline implementation

**Files**:
- `base.py`: BaseProcessor abstract interface
- `stdf_processor.py`: STDF → Parquet conversion
- `report_processor.py`: Report generation coordinator
- `char_processor.py`: Characterization processing
- `shmoo_processor.py`: Shmoo plot processing

**Processing Flow**:
```
STDF File → STDFProcessor → Parquet Files
Parquet Files → ReportProcessor → HTML Reports
```

### 4. Converters (`src/converters/`)

**Purpose**: Data format conversion

**Files**:
- `stdf_converter.py`: STDF parsing and conversion logic
- `parquet_writer.py`: Optimized Parquet writing

**Features**:
- Parallel processing support
- Memory-efficient streaming
- Automatic compression (LZ4 default)
- Type optimization for Polars

### 5. Parsers (`src/parsers/`)

**Purpose**: File and data parsing

**Files**:
- `stdf_parser.py`: STDF format parsing (wraps pystdf)
- `parameter_parser.py`: Parameter extraction from paths/files

**Responsibilities**:
- Parse STDF binary format
- Extract test parameters
- Validate data integrity

### 6. Generators (`src/generators/`)

**Purpose**: Report and visualization generation

**Files**:
- `html_generator.py`: HTML report generation
- `chart_generator.py`: Interactive chart creation (Plotly)
- `templates/`: HTML templates

**Report Types**:
- Condition Analysis
- Stability (LOOP)
- Volume IP
- Test Time Analysis
- Yield Analysis
- Characterization (CHAR)
- Shmoo Plots

### 7. Utilities (`src/utils/`)

**Purpose**: Shared utility functions

**Files**:
- `file_utils.py`: File operations (checksum, locking, etc.)
- `validation.py`: Data validation
- `parallel.py`: Parallel processing helpers

### 8. Monitoring (`src/monitoring/`)

**Purpose**: Directory polling and file tracking

**Files**:
- `poller.py`: Directory monitoring system
- `file_tracker.py`: Track processed files

**Features**:
- Continuous directory monitoring
- Duplicate detection
- File status tracking
- Completion markers

## Data Flow

### 1. STDF Processing Pipeline

```
┌────────────┐
│ STDF File  │
│ (.std.gz)  │
└──────┬─────┘
       │
       ▼
┌────────────────────┐
│ STDFParser         │
│ - Decompress       │
│ - Parse records    │
│ - Add PART_ID      │
└──────┬─────────────┘
       │
       ▼
┌────────────────────┐
│ OptimizedWriter    │
│ - Accumulate data  │
│ - Type optimization│
│ - Memory efficient │
└──────┬─────────────┘
       │
       ▼
┌────────────────────┐
│ Parquet Files      │
│ - mir.parquet      │
│ - prr.parquet      │
│ - ptr.parquet      │
│ - ftr.parquet      │
│ - ...              │
└────────────────────┘
```

### 2. Report Generation Pipeline

```
┌────────────────────┐
│ Parquet Files      │
└──────┬─────────────┘
       │
       ▼
┌────────────────────┐
│ Data Loading       │
│ (Polars LazyFrame) │
└──────┬─────────────┘
       │
       ▼
┌────────────────────┐
│ Data Transformation│
│ - Filter tests     │
│ - Calculate stats  │
│ - Remove retests   │
└──────┬─────────────┘
       │
       ▼
┌────────────────────┐
│ Chart Generation   │
│ (Plotly)           │
└──────┬─────────────┘
       │
       ▼
┌────────────────────┐
│ HTML Report        │
│ (Interactive)      │
└────────────────────┘
```

## Performance Optimizations

### 1. Parallel Processing

```python
# STDF files processed in parallel
parallel_stdf_workers = 2  # Default: 2x speed improvement

# Usage:
# 2 workers: ~50% time reduction
# 4 workers: ~75% time reduction
```

**Implementation**: `multiprocessing.Pool` for true parallelism

### 2. Memory Efficiency

- **Lazy Loading**: Polars LazyFrame for deferred execution
- **Streaming**: Process data in chunks
- **Type Optimization**: Downcast to smallest dtype
- **Parquet Compression**: LZ4 for fast I/O

### 3. I/O Optimization

- **Buffer Size**: 2MB buffer for file I/O
- **Compression**: LZ4 (fast) vs GZIP (small)
- **Batch Writing**: Accumulate before writing

## Configuration

### Environment Variables

```bash
# Parallel processing
export ART_PARALLEL_WORKERS=4

# Compression
export ART_COMPRESSION=lz4  # lz4, snappy, gzip, zstd

# Logging
export ART_LOG_LEVEL=INFO  # DEBUG, INFO, WARNING, ERROR

# Watch path
export ART_WATCH_PATH=/path/to/stdf/files
```

### Configuration Files

1. **Global Settings** (`config/settings.py`)
   - Processing configuration
   - Flow validation rules
   - Report settings

2. **Product Configuration** (`ART.jsonc`)
   - Per-product settings
   - Wafer map parameters
   - Temperature palette

3. **Product List** (`ARTstdf_Product.cnf`)
   - List of products to process

## Error Handling

### Exception Hierarchy

```
ARTError (base)
├── ConfigurationError
├── ValidationError
├── ProcessingError
│   ├── ConversionError
│   ├── ReportGenerationError
│   └── ParquetError
├── ParsingError
│   ├── STDFFormatError
│   └── ParameterExtractionError
├── FileNotFoundError
└── MonitoringError
```

### Error Recovery

1. **File-level Errors**: Skip file, continue processing others
2. **Processing Errors**: Log, mark as failed, notify
3. **Configuration Errors**: Halt system, require fix

## Logging

### Log Files

- `art.log`: Main application log
- `stdf2data.log`: STDF conversion logs
- `data2report.log`: Report generation logs
- `condition2report.log`: Condition report logs
- `char.log`: Characterization logs
- `shmoo.log`: Shmoo processing logs

### Log Rotation

- **Rotation**: By line count (1000 lines default)
- **Backup**: 1 backup file
- **Format**: `%(asctime)s - %(name)s - %(levelname)s - %(message)s`

## Testing Strategy

### Unit Tests
- Test individual modules
- Mock external dependencies
- Validate business logic

### Integration Tests
- Test complete pipelines
- Real STDF files
- Validate output format

### Performance Tests
- Measure processing time
- Memory profiling
- Parallel scaling

## Future Enhancements

1. **Web Interface**: Flask/FastAPI dashboard
2. **Database Backend**: PostgreSQL for metadata
3. **Real-time Processing**: WebSocket updates
4. **ML Integration**: Anomaly detection
5. **Cloud Deployment**: Docker/Kubernetes support

## References

- [Polars Documentation](https://pola.rs/docs)
- [STDF Specification](http://www.xess.com/wiki/STDF-V4-Spec.pdf)
- [Plotly Python](https://plotly.com/python/)
- [Python Multiprocessing](https://docs.python.org/3/library/multiprocessing.html)
