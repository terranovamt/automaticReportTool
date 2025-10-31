# ART.stdf Configuration Guide

## Overview

ART.stdf uses a multi-level configuration system:
1. **Code Configuration** (`config/settings.py`)
2. **Environment Variables**
3. **Product Configuration** (`ART.jsonc`)
4. **Product List** (`ARTstdf_Product.cnf`)

## 1. Code Configuration

### Editing Settings

Edit `config/settings.py` to modify default settings:

```python
@dataclass
class ProcessingConfig:
    """Processing configuration."""

    # Parallel processing
    parallel_stdf_workers: int = 2  # ← Change this for more workers
    max_workers: int = 4

    # Memory management
    chunk_size: int = 10000
    max_memory_mb: int = 2048

    # Performance
    use_polars: bool = True
    compression: str = "lz4"  # lz4, snappy, gzip, zstd
    buffer_size_mb: int = 2

    # Processing options
    remove_retests: bool = True
    optimize_dtypes: bool = True

    # Validation
    validate_stdf: bool = True
    strict_mode: bool = False
```

### Performance Tuning

**Parallel Workers**:
- `parallel_stdf_workers = 2`: Default, 50% faster
- `parallel_stdf_workers = 4`: 75% faster (requires 4+ CPU cores)
- `parallel_stdf_workers = 8`: 87.5% faster (requires 8+ CPU cores)

**Compression**:
- `lz4`: Fastest, good compression (default)
- `snappy`: Fast, slightly worse compression
- `gzip`: Slow, best compression
- `zstd`: Balanced
- `uncompressed`: Fastest but large files

**Memory**:
- `chunk_size`: Records per processing chunk (increase for more memory)
- `max_memory_mb`: Maximum memory usage
- `buffer_size_mb`: I/O buffer size

### Flow Configuration

```python
@dataclass
class FlowConfig:
    """Flow validation configuration."""

    allowed_flows: Set[str] = {
        "EWS1", "EWS2", "EWS3", "EWSDIE", "EWSCHAR",
        "FT", "FT1", "FT2",
    }

    allowed_packages: Set[str] = {
        "QFP", "QFN", "DIP", "WLCSP", "CSP", "BGA"
    }
```

### Logging Configuration

```python
@dataclass
class LoggingConfig:
    """Logging configuration."""

    log_dir: Path = Path("log")
    max_lines_per_file: int = 1000
    backup_count: int = 1
    log_level: str = "INFO"  # DEBUG, INFO, WARNING, ERROR, CRITICAL
```

## 2. Environment Variables

Set environment variables to override defaults:

### Linux/Mac

```bash
# Parallel processing
export ART_PARALLEL_WORKERS=4

# Compression
export ART_COMPRESSION=lz4

# Logging
export ART_LOG_LEVEL=INFO

# Watch path
export ART_WATCH_PATH=/path/to/stdf/files
```

### Windows

```cmd
REM Parallel processing
set ART_PARALLEL_WORKERS=4

REM Compression
set ART_COMPRESSION=lz4

REM Logging
set ART_LOG_LEVEL=INFO

REM Watch path
set ART_WATCH_PATH=\\\\server\\path\\to\\stdf
```

### Python Script

```python
import os

# Set before importing config
os.environ["ART_PARALLEL_WORKERS"] = "4"
os.environ["ART_COMPRESSION"] = "snappy"

from config import Settings
settings = Settings()
```

## 3. Product Configuration (ART.jsonc)

Create `ART.jsonc` in your product directory:

```jsonc
{
  // Product Information
  "product_name": "Mosquito512K",
  "product_code": "44E",

  // Wafer Coordinate Range
  "xwafer": [0, 30],  // X-axis range
  "ywafer": [0, 30],  // Y-axis range

  // Test Configuration
  "touch_down": 150,

  // Wafer Map Reconstruction Test Numbers
  "XY_XL": "4500001",  // X Low byte
  "XY_XH": "4500002",  // X High byte
  "XY_YL": "4500003",  // Y Low byte
  "XY_YH": "4500004",  // Y High byte
  "XY_Lot1": "4500005",
  "XY_Lot2": "4500006",
  "XY_Lot3": "4500007",
  "XY_Lot4": "4500008",
  "XY_Lot5": "4500009",
  "XY_Lot6": "4500010",

  // Temperature-to-Color Mapping for CHAR
  "STPaletteChar": {
    "-40": "#03234B",  // Dark blue
    "-10": "#3CB4E6",  // Light blue
    "30": "#49B170",   // Green
    "60": "#A4C238",   // Yellow-green
    "90": "#FFD200",   // Yellow
    "130": "#F3693F"   // Orange-red
  },

  // Optional: Custom Test Numbers
  "custom_tests": {
    "power_consumption": ["1000", "1001", "1002"],
    "timing_tests": ["2000", "2001"]
  }
}
```

### Field Descriptions

| Field | Type | Description | Required |
|-------|------|-------------|----------|
| `product_name` | string | Product display name | Yes |
| `product_code` | string | 3-char product code | Yes |
| `xwafer` | [int, int] | X-axis range | Yes |
| `ywafer` | [int, int] | Y-axis range | Yes |
| `touch_down` | int | Touchdown count | No |
| `XY_*` | string | Wafer coordinate test numbers | Yes (for wafer map) |
| `STPaletteChar` | object | Temperature color mapping | No |
| `custom_tests` | object | Custom test groupings | No |

## 4. Product List (ARTstdf_Product.cnf)

Specify which products to process:

```
[44E, 44F, 449, 44A]
```

Format:
- Comma-separated list
- 3-character product codes
- Enclosed in square brackets

## 5. Runtime Configuration

### Command Line

```bash
# Basic usage with default watch path
python main.py

# Specify watch path
python main.py "/path/to/stdf/files"

# With environment variables
ART_PARALLEL_WORKERS=4 ART_LOG_LEVEL=DEBUG python main.py
```

### Programmatic Configuration

```python
from config import Settings, PathConfig
from pathlib import Path

# Get settings instance
settings = Settings()

# Modify settings
settings.processing.parallel_stdf_workers = 4
settings.processing.compression = "snappy"
settings.logging.log_level = "DEBUG"

# Configure paths
paths = PathConfig(
    project_root=Path.cwd(),
    src_root=Path.cwd() / "src",
    watch_path=Path("/custom/watch/path")
)

# Validate
settings.validate()
paths.validate()
```

## Configuration Priority

Configuration is loaded in this order (later overrides earlier):

1. Default values in `config/settings.py`
2. Environment variables (`ART_*`)
3. Product configuration (`ART.jsonc`)
4. Runtime modifications

## Best Practices

### Performance

```python
# For 8-core CPU with 16GB RAM
parallel_stdf_workers = 6  # Leave 2 cores for system
max_workers = 8
chunk_size = 50000  # Larger chunks for more memory
compression = "lz4"  # Fast compression
```

### Development

```python
# Development settings
log_level = "DEBUG"
validate_stdf = True
strict_mode = True
parallel_stdf_workers = 1  # Easier debugging
```

### Production

```python
# Production settings
log_level = "INFO"
validate_stdf = False  # Skip validation for speed
strict_mode = False
parallel_stdf_workers = 4  # Maximize throughput
compression = "lz4"
```

## Troubleshooting

### Issue: Slow processing

**Solution**: Increase `parallel_stdf_workers`

```python
parallel_stdf_workers = 4  # Use more CPU cores
```

### Issue: High memory usage

**Solution**: Reduce `chunk_size`

```python
chunk_size = 5000  # Smaller chunks
max_memory_mb = 1024  # Limit memory
```

### Issue: Large Parquet files

**Solution**: Increase compression

```python
compression = "gzip"  # Better compression, slower
```

### Issue: Missing wafer maps

**Solution**: Check `XY_*` test numbers in `ART.jsonc`

```jsonc
{
  "XY_XL": "4500001",  // Verify these match your STDF
  "XY_XH": "4500002",
  "XY_YL": "4500003",
  "XY_YH": "4500004"
}
```

## Examples

### High-Performance Setup

```python
# config/settings.py
@dataclass
class ProcessingConfig:
    parallel_stdf_workers: int = 8  # 8 cores
    max_workers: int = 8
    chunk_size: int = 100000  # Large chunks
    max_memory_mb: int = 8192  # 8GB
    compression: str = "lz4"  # Fast
    buffer_size_mb: int = 8  # Large buffer
    remove_retests: bool = True
    optimize_dtypes: bool = True
```

### Memory-Constrained Setup

```python
# config/settings.py
@dataclass
class ProcessingConfig:
    parallel_stdf_workers: int = 2  # Limited parallelism
    max_workers: int = 2
    chunk_size: int = 5000  # Small chunks
    max_memory_mb: int = 1024  # 1GB limit
    compression: str = "gzip"  # Best compression
    buffer_size_mb: int = 1  # Small buffer
    optimize_dtypes: bool = True  # Save memory
```

### Debug Setup

```python
# config/settings.py
@dataclass
class ProcessingConfig:
    parallel_stdf_workers: int = 1  # Sequential for debugging
    validate_stdf: bool = True  # Validate everything
    strict_mode: bool = True  # Strict validation

@dataclass
class LoggingConfig:
    log_level: str = "DEBUG"  # Verbose logging
    max_lines_per_file: int = 10000  # Keep more logs
```
