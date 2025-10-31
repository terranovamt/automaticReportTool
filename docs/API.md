# ART.stdf API Documentation

## Configuration API

### Settings

```python
from config import Settings

# Get global settings instance
settings = Settings()

# Access configuration
workers = settings.processing.parallel_stdf_workers
compression = settings.processing.compression
log_level = settings.logging.log_level

# Validate settings
settings.validate()

# Convert to dict
config_dict = settings.to_dict()
```

### Path Configuration

```python
from config import PathConfig, get_default_paths

# Get default paths
paths = get_default_paths()

# Access paths
log_dir = paths.log_dir
temp_dir = paths.temp_dir
output_dir = paths.output_dir

# Get derived paths
parquet_dir = paths.get_parquet_dir(stdf_path)
report_dir = paths.get_report_dir(stdf_path)
```

### Logging

```python
from config import setup_logging, get_logger

# Setup global logging
setup_logging(
    log_dir=Path("log"),
    log_level="INFO",
    max_lines=1000,
    backup_count=1
)

# Get logger for component
logger = get_logger("my_component", "my_component.log")
logger.info("Processing started")
```

## Core API

### Models

```python
from src.core import STDFFile, ProcessingResult, Parameter

# Create STDF file object
stdf_file = STDFFile(
    path=Path("test.std.gz"),
    filename="test.std.gz",
    status=FileStatus.PENDING
)

# Check file properties
if stdf_file.is_compressed:
    print("File is compressed")

if stdf_file.is_valid_stdf:
    print("Valid STDF file")

# Create parameter object
param = Parameter(
    CUT="44E",
    FLOW="EWS1",
    LOT="ABC123",
    WAFER="01",
    TYPE="CONDITION"
)

# Convert to dict
param_dict = param.to_dict()
```

### Exceptions

```python
from src.core.exceptions import (
    ProcessingError,
    ValidationError,
    FileNotFoundError
)

try:
    # Process file
    result = processor.process(input_path, output_path)
except FileNotFoundError as e:
    logger.error(f"File not found: {e}")
except ValidationError as e:
    logger.error(f"Validation failed: {e}")
except ProcessingError as e:
    logger.error(f"Processing failed: {e}")
```

## Processors API

### STDF Processor

```python
from src.processors import STDFProcessor

# Create processor
processor = STDFProcessor()

# Process STDF file
result = processor.process(
    input_path=Path("test.std.gz"),
    output_path=Path("output/parquet")
)

# Check result
if result.success:
    print(f"Processed {len(result.output_files)} files")
    print(f"Time: {result.processing_time:.2f}s")
else:
    print(f"Error: {result.error_message}")
```

### Base Processor Interface

```python
from src.processors.base import BaseProcessor
from pathlib import Path

class CustomProcessor(BaseProcessor):
    def __init__(self):
        super().__init__("CustomProcessor")

    def validate_input(self, input_path: Path) -> bool:
        # Custom validation
        return input_path.exists()

    def process(self, input_path: Path, output_path: Path) -> ProcessingResult:
        try:
            # Custom processing logic
            # ...

            return ProcessingResult(
                success=True,
                file_path=input_path,
                output_files=[output_path / "result.html"]
            )
        except Exception as e:
            return ProcessingResult(
                success=False,
                file_path=input_path,
                error_message=str(e)
            )
```

## Utilities API

### File Utilities

```python
from src.utils import (
    ensure_directory,
    get_file_checksum,
    is_file_locked,
    safe_remove_file
)

# Ensure directory exists
output_dir = ensure_directory(Path("output"))

# Get file checksum
checksum = get_file_checksum(Path("file.std"), algorithm="md5")

# Check if file is locked
if not is_file_locked(Path("file.std")):
    # Process file
    pass

# Safely remove file
safe_remove_file(Path("temp.dat"))
```

### Validation

```python
from src.utils import (
    validate_stdf_file,
    validate_parameter,
    validate_dataframe
)

# Validate STDF file
try:
    validate_stdf_file(Path("test.std"))
except ValidationError as e:
    print(f"Invalid: {e}")

# Validate parameter
validate_parameter({"CUT": "44E", "FLOW": "EWS1", ...})

# Validate DataFrame
validate_dataframe(df, required_columns=["PART_ID", "TEST_NUM"])
```

### Parallel Processing

```python
from src.utils import parallel_process_files, process_with_pool

# Process files in parallel
def process_file(file_path):
    # Processing logic
    return result

results = parallel_process_files(
    files=file_list,
    processor_func=process_file,
    num_workers=4
)

# Check results
for success, result, error in results:
    if success:
        print(f"Success: {result}")
    else:
        print(f"Failed: {error}")
```

## Constants

```python
from src.core.constants import (
    STDF_EXTENSIONS,
    PARQUET_EXTENSIONS,
    DEFAULT_COMPRESSION,
    RECORD_TYPES,
    TEST_RECORD_TYPES,
    REPORT_TYPES
)

# Use constants
if file_path.suffix in STDF_EXTENSIONS:
    print("STDF file detected")

compression = DEFAULT_COMPRESSION  # "lz4"

for rec_type, description in RECORD_TYPES.items():
    print(f"{rec_type}: {description}")
```

## Usage Examples

### Complete Processing Pipeline

```python
from pathlib import Path
from config import Settings, get_default_paths, setup_logging
from src.processors import STDFProcessor
from src.core import STDFFile, FileStatus

# Setup
settings = Settings()
paths = get_default_paths()
setup_logging(paths.log_dir, "INFO")

# Create processor
processor = STDFProcessor()

# Process STDF file
stdf_path = Path("data/test.std.gz")
output_path = paths.get_parquet_dir(stdf_path)

result = processor.process(stdf_path, output_path)

if result.success:
    print(f"✅ Processing successful")
    print(f"   Output files: {len(result.output_files)}")
    print(f"   Time: {result.processing_time:.2f}s")
    for file in result.output_files:
        print(f"   - {file.name}")
else:
    print(f"❌ Processing failed: {result.error_message}")
```

### Batch Processing with Parallel

```python
from src.utils import parallel_process_files
from src.processors import STDFProcessor

# Get list of STDF files
stdf_files = list(Path("data").glob("*.std.gz"))

# Define processing function
def process_stdf(stdf_path):
    processor = STDFProcessor()
    output_dir = stdf_path.parent / "parquet"
    result = processor.process(stdf_path, output_dir)
    return (result.success, stdf_path, result.error_message)

# Process in parallel
results = parallel_process_files(
    files=stdf_files,
    processor_func=process_stdf,
    num_workers=4
)

# Report results
successful = sum(1 for success, _, _ in results if success)
print(f"Processed {successful}/{len(results)} files successfully")
```

## Environment Variables

```python
import os

# Set configuration via environment
os.environ["ART_PARALLEL_WORKERS"] = "4"
os.environ["ART_COMPRESSION"] = "lz4"
os.environ["ART_LOG_LEVEL"] = "DEBUG"
os.environ["ART_WATCH_PATH"] = "/path/to/stdf/files"

# Settings automatically load from environment
from config import Settings
settings = Settings()
print(settings.processing.parallel_stdf_workers)  # 4
```
