# ART.stdf - Migration Guide

**Complete guide for migrating from legacy code to Clean Architecture**

Version: 1.0
Last Updated: 2025-11-05
Author: ART.stdf Development Team

---

## Table of Contents

1. [Overview](#overview)
2. [Quick Start](#quick-start)
3. [Migration Patterns](#migration-patterns)
   - [Parameter Model Migration](#parameter-model-migration)
   - [Report Generation Migration](#report-generation-migration)
   - [Use Case Pattern Migration](#use-case-pattern-migration)
   - [Import Path Changes](#import-path-changes)
4. [Step-by-Step Migration](#step-by-step-migration)
5. [Code Examples](#code-examples)
6. [Testing Your Migration](#testing-your-migration)
7. [Troubleshooting](#troubleshooting)
8. [Backward Compatibility](#backward-compatibility)

---

## Overview

### What Changed?

The ART.stdf project has been refactored from a monolithic script-based architecture to a Clean Architecture design following SOLID principles. Key changes include:

- **Jupyter Notebooks → Pure Python**: All 5 Jupyter notebooks replaced with Python report generators
- **Dict-based Models → Dataclasses**: Type-safe domain models with validation
- **Direct Function Calls → Use Cases**: Business logic encapsulated in use case classes
- **Scattered Code → Layered Architecture**: 4-layer separation (Domain, Application, Infrastructure, Presentation)

### Why Migrate?

- **Better Type Safety**: 100% type hints coverage with static analysis
- **Improved Testability**: All components unit-testable with dependency injection
- **Better Performance**: ~50% faster report generation without subprocess overhead
- **Better Error Handling**: Comprehensive logging and error recovery
- **Better Maintainability**: SOLID principles and clear separation of concerns

### Migration Strategy

You have two options:

1. **Gradual Migration** (Recommended): Use backward compatibility layer while incrementally updating code
2. **Full Migration**: Update all code at once using this guide

---

## Quick Start

### Most Common Scenarios

**Before (Legacy):**
```python
from src.script.main import process_analysis

# Dict-based parameter
param_dict = {
    "CODE": "44E",
    "CUT": "44EZ",
    "FLOW": "EWSCHAR",
    # ... more fields
}

# Direct function call
process_analysis(param_dict)
```

**After (New):**
```python
from src.domain.models.parameter import Parameter
from src.application.use_cases.generate_report_use_case import GenerateReportUseCase

# Type-safe dataclass
parameter = Parameter(
    code="44E",
    cut="44EZ",
    flow="EWSCHAR",
    type="CHAR",
    lot="Q445172",
    wafer="05"
)

# Use case pattern
use_case = GenerateReportUseCase()
use_case.execute(report_type="VOLUME", parameter=parameter)
```

---

## Migration Patterns

### Parameter Model Migration

#### Legacy Dict Format

**Old Code:**
```python
# Creating parameter dict
param = {
    "CODE": "44E",
    "CUT": "44EZ",
    "FLOW": "EWSCHAR",
    "TYPE": "CHAR",
    "LOT": "Q445172",
    "WAFER": "05",
    "FILE": {
        "05": {
            "corner": "TTTT",
            "path": "./STDF/44E/44EZ/EWSCHAR/Q445172_05_TTTT"
        }
    }
}

# Accessing values
code = param["CODE"]
flow = param["FLOW"]
```

**New Code:**
```python
from src.domain.models.parameter import Parameter, FileCorner

# Creating Parameter object
parameter = Parameter(
    code="44E",
    cut="44EZ",
    flow="EWSCHAR",
    type="CHAR",
    lot="Q445172",
    wafer="05",
    file={
        "05": FileCorner(
            corner="TTTT",
            path="./STDF/44E/44EZ/EWSCHAR/Q445172_05_TTTT"
        )
    }
)

# Accessing values with autocomplete and type checking
code = parameter.code
flow = parameter.flow
```

#### Migration Helpers

The new `Parameter` class provides bidirectional conversion:

```python
from src.domain.models.parameter import Parameter

# Convert legacy dict to Parameter
legacy_dict = load_legacy_parameter()
parameter = Parameter.from_dict(legacy_dict)

# Convert Parameter back to dict (for backward compatibility)
new_dict = parameter.to_dict()
```

#### Parameter Properties

The new model includes computed properties:

```python
parameter = Parameter(code="44E", cut="44EZ", flow="EWS1", ...)

# Check if EWS flow
if parameter.is_ews_flow:
    print("This is an EWS flow")

# Get formatted title
print(parameter.title)  # "Q445172_05_44E_44EZ_EWS1_CHAR"

# Get formatted COM
print(parameter.com)  # "44E 44EZ Q445172 EWS1 WAFER:05"
```

---

### Report Generation Migration

#### Legacy Jupyter Notebook Approach

**Old Code (Jupyter subprocess):**
```python
import subprocess
from pathlib import Path

def generate_volume_report(param_dict, data_path):
    """Generate VOLUME report using Jupyter notebook."""
    notebook = Path(__file__).parent / "notebooks" / "VOLUME.ipynb"
    output_dir = Path(data_path).parent

    # Execute notebook as subprocess
    cmd = [
        'jupyter', 'nbconvert',
        '--execute',
        '--to', 'html',
        '--output-dir', str(output_dir),
        str(notebook)
    ]

    result = subprocess.call(cmd)
    if result != 0:
        raise Exception("Notebook execution failed")
```

**Issues with old approach:**
- Subprocess overhead (~5-10 seconds per report)
- Difficult to test
- Poor error messages
- No dependency injection
- Hard to customize

**New Code (Pure Python generators):**
```python
from pathlib import Path
from src.domain.models.parameter import Parameter
from src.presentation.report_generators import create_report_generator

def generate_volume_report(parameter: Parameter, data_path: str) -> Path:
    """Generate VOLUME report using Python generator."""
    # Create generator using factory pattern
    generator = create_report_generator("VOLUME", parameter)

    # Generate report (direct method call, no subprocess)
    output_path = Path(data_path).parent
    report_path = generator.generate(
        data_path=data_path,
        output_path=output_path
    )

    return report_path
```

**Benefits of new approach:**
- ~50% faster (no subprocess overhead)
- Fully testable with dependency injection
- Rich error messages with logging
- Type-safe with IDE autocomplete
- Easy to customize and extend

#### All Report Types

```python
from src.presentation.report_generators import create_report_generator

# Supported report types
report_types = ["VOLUME", "LOOP", "TTIME", "YIELD", "CONDITION"]

for report_type in report_types:
    generator = create_report_generator(report_type, parameter)
    report_path = generator.generate(data_path, output_path)
    print(f"Generated {report_type} report: {report_path}")
```

#### Custom Template Directory

```python
# Use custom templates
generator = create_report_generator(
    "VOLUME",
    parameter,
    template_dir="/custom/templates"
)
```

---

### Use Case Pattern Migration

#### Legacy Direct Function Calls

**Old Code:**
```python
from src.script.converter import convert_stdf_to_parquet
from src.script.analyzer import analyze_data
from src.script.reporter import generate_reports

# Direct function calls scattered throughout code
def process_stdf_file(stdf_path, param_dict):
    # Step 1: Convert
    parquet_files = convert_stdf_to_parquet(stdf_path, param_dict)

    # Step 2: Analyze
    analysis_results = analyze_data(parquet_files, param_dict)

    # Step 3: Report
    reports = generate_reports(analysis_results, param_dict)

    return reports
```

**Issues:**
- Business logic scattered across multiple files
- Hard to test (no dependency injection)
- Difficult to mock external dependencies
- No single source of truth for workflows

**New Code (Use Case Pattern):**
```python
from src.domain.models.parameter import Parameter
from src.application.use_cases.convert_stdf_use_case import ConvertSTDFUseCase
from src.application.use_cases.generate_report_use_case import GenerateReportUseCase

def process_stdf_file(stdf_path: str, parameter: Parameter):
    """Process STDF file using use case pattern."""
    # Step 1: Convert (encapsulated workflow)
    convert_use_case = ConvertSTDFUseCase()
    parquet_files = convert_use_case.execute(
        stdf_path=stdf_path,
        parameter=parameter
    )

    # Step 2: Generate reports (encapsulated workflow)
    report_use_case = GenerateReportUseCase()
    reports = report_use_case.execute(
        report_type="VOLUME",
        parameter=parameter
    )

    return reports
```

**Benefits:**
- Business logic encapsulated in single class per workflow
- Easy to test with dependency injection
- Clear separation of concerns
- Single source of truth for each workflow

#### Use Case with Dependency Injection

```python
from src.application.use_cases.convert_stdf_use_case import ConvertSTDFUseCase
from src.infrastructure.parsers.stdf_parser import STDFParser
from src.infrastructure.repositories.file_repository import FileRepository
import logging

# Create dependencies
logger = logging.getLogger(__name__)
stdf_parser = STDFParser()
file_repository = FileRepository()

# Inject dependencies
use_case = ConvertSTDFUseCase(
    stdf_parser=stdf_parser,
    logger=logger
)

# Execute with custom configuration
result = use_case.execute(
    stdf_path="/path/to/file.std",
    parameter=parameter,
    compression="zstd"  # Optional parameter
)
```

#### Testing Use Cases

```python
import pytest
from unittest.mock import Mock
from src.application.use_cases.convert_stdf_use_case import ConvertSTDFUseCase

def test_convert_use_case():
    # Create mocks
    mock_parser = Mock()
    mock_parser.parse_to_parquet.return_value = {"ptr": Path("test.ptr.parquet")}

    mock_logger = Mock()

    # Inject mocks
    use_case = ConvertSTDFUseCase(
        stdf_parser=mock_parser,
        logger=mock_logger
    )

    # Execute and assert
    result = use_case.execute(stdf_path="/test.std", parameter=parameter)

    assert result is not None
    mock_parser.parse_to_parquet.assert_called_once()
```

---

### Import Path Changes

#### Module Reorganization

**Domain Layer:**
```python
# OLD: No domain layer
# NEW:
from src.domain.models.parameter import Parameter, FileCorner
```

**Application Layer:**
```python
# OLD:
from src.script.converter import convert_stdf

# NEW:
from src.application.use_cases.convert_stdf_use_case import ConvertSTDFUseCase
from src.application.use_cases.generate_report_use_case import GenerateReportUseCase
```

**Infrastructure Layer:**
```python
# OLD:
from src.utils.file_utils import save_file
from src.parsers.stdf import parse_stdf

# NEW:
from src.infrastructure.repositories.file_repository import FileRepository
from src.infrastructure.repositories.parquet_repository import ParquetRepository
from src.infrastructure.parsers.stdf_parser import STDFParser
from src.infrastructure.services.file_classifier import FileClassifier
from src.infrastructure.services.completion_tracker import CompletionTracker
```

**Presentation Layer:**
```python
# OLD:
from src.notebooks.VOLUME import generate_volume_report

# NEW:
from src.presentation.report_generators import create_report_generator
from src.presentation.report_generators.volume_report_generator import VolumeReportGenerator
from src.presentation.visualizers.plotly_builder import PlotlyBuilder
from src.presentation.visualizers.html_builder import HTMLBuilder
```

#### Complete Import Mapping

| Old Import | New Import |
|-----------|-----------|
| `from src.script.main import *` | `from src.application.use_cases.*` |
| `from src.utils.file_utils import *` | `from src.infrastructure.repositories.*` |
| `from src.parsers.stdf import *` | `from src.infrastructure.parsers.stdf_parser import *` |
| `from src.notebooks.* import *` | `from src.presentation.report_generators import *` |
| No equivalent | `from src.domain.models.parameter import Parameter` |

---

## Step-by-Step Migration

### Phase 1: Update Parameter Usage

1. **Find all dict-based parameters:**
   ```bash
   grep -r "param\[\"CODE\"\]" src/
   grep -r "param_dict" src/
   ```

2. **Replace with Parameter class:**
   ```python
   # Before
   def my_function(param_dict: Dict):
       code = param_dict["CODE"]

   # After
   from src.domain.models.parameter import Parameter

   def my_function(parameter: Parameter):
       code = parameter.code
   ```

3. **Update function signatures:**
   ```python
   # Before
   def process(param: Dict) -> Dict:
       ...

   # After
   def process(parameter: Parameter) -> Parameter:
       ...
   ```

### Phase 2: Replace Jupyter Notebooks

1. **Find all notebook subprocess calls:**
   ```bash
   grep -r "jupyter nbconvert" src/
   grep -r "subprocess.call" src/
   ```

2. **Replace with report generators:**
   ```python
   # Before
   cmd = f'jupyter nbconvert --execute ...'
   subprocess.call(cmd)

   # After
   from src.presentation.report_generators import create_report_generator
   generator = create_report_generator("VOLUME", parameter)
   report_path = generator.generate(data_path, output_path)
   ```

3. **Remove Jupyter imports:**
   ```python
   # Remove these:
   import subprocess
   from IPython.display import display
   import jupyter
   ```

### Phase 3: Adopt Use Case Pattern

1. **Identify business workflows:**
   - STDF conversion workflow → `ConvertSTDFUseCase`
   - Report generation workflow → `GenerateReportUseCase`

2. **Replace direct function calls:**
   ```python
   # Before
   from src.script.converter import convert_stdf
   result = convert_stdf(path, param_dict)

   # After
   from src.application.use_cases.convert_stdf_use_case import ConvertSTDFUseCase
   use_case = ConvertSTDFUseCase()
   result = use_case.execute(stdf_path=path, parameter=parameter)
   ```

3. **Add dependency injection:**
   ```python
   # Inject custom logger
   import logging
   logger = logging.getLogger(__name__)
   use_case = ConvertSTDFUseCase(logger=logger)
   ```

### Phase 4: Update Imports

1. **Update all import statements** using the mapping table above

2. **Run import checks:**
   ```bash
   python -m pytest tests/ -v
   python -m mypy src/ --strict
   ```

### Phase 5: Add Type Hints

1. **Add type hints to all functions:**
   ```python
   # Before
   def process(param):
       return param

   # After
   from src.domain.models.parameter import Parameter

   def process(parameter: Parameter) -> Parameter:
       return parameter
   ```

2. **Verify with mypy:**
   ```bash
   mypy src/ --strict
   ```

### Phase 6: Update Tests

1. **Convert test fixtures:**
   ```python
   # Before
   @pytest.fixture
   def sample_param():
       return {"CODE": "44E", "CUT": "44EZ"}

   # After
   @pytest.fixture
   def sample_param():
       return Parameter(code="44E", cut="44EZ", flow="EWS1",
                       type="CHAR", lot="L001", wafer="01")
   ```

2. **Add mock fixtures** (see `tests/conftest.py` for examples)

---

## Code Examples

### Example 1: Complete Workflow Migration

**Before (Legacy):**
```python
import subprocess
from pathlib import Path

def complete_workflow(stdf_file: str, param_dict: dict):
    """Old workflow - mixed concerns, hard to test."""
    # Step 1: Parse STDF
    from src.parsers.stdf import parse_stdf_file
    data = parse_stdf_file(stdf_file)

    # Step 2: Save to Parquet
    from src.utils.file_utils import save_parquet
    parquet_path = save_parquet(data, param_dict["CODE"])

    # Step 3: Generate report via Jupyter
    notebook = Path(__file__).parent / "notebooks" / "VOLUME.ipynb"
    cmd = f'jupyter nbconvert --execute {notebook}'
    subprocess.call(cmd.split())

    return parquet_path
```

**After (New Architecture):**
```python
from pathlib import Path
from src.domain.models.parameter import Parameter
from src.application.use_cases.convert_stdf_use_case import ConvertSTDFUseCase
from src.application.use_cases.generate_report_use_case import GenerateReportUseCase

def complete_workflow(stdf_file: str, parameter: Parameter) -> Path:
    """New workflow - clean separation, fully testable."""
    # Step 1: Convert STDF to Parquet (single responsibility)
    convert_use_case = ConvertSTDFUseCase()
    parquet_files = convert_use_case.execute(
        stdf_path=stdf_file,
        parameter=parameter
    )

    # Step 2: Generate report (no subprocess, pure Python)
    report_use_case = GenerateReportUseCase()
    report_path = report_use_case.execute(
        report_type="VOLUME",
        parameter=parameter
    )

    return report_path
```

### Example 2: Custom Report Generator

**Before (Modifying Jupyter notebook):**
```python
# Had to edit .ipynb JSON file directly - very error-prone
# Or create new notebook and maintain multiple copies
```

**After (Inherit from BaseReportGenerator):**
```python
from src.presentation.report_generators.base_report_generator import BaseReportGenerator
from pathlib import Path
from typing import Optional, Dict
import polars as pl

class CustomReportGenerator(BaseReportGenerator):
    """Custom report with your own logic."""

    def get_report_type(self) -> str:
        return "CUSTOM"

    def generate(
        self,
        data_path: str,
        output_path: Path,
        df_stdf: Optional[Dict[str, pl.DataFrame]] = None
    ) -> Path:
        """Generate custom report."""
        # Your custom logic here
        html_content = self._generate_html(df_stdf)

        # Save report
        report_path = output_path / f"{self.parameter.title}_custom.html"
        self.save_report(html_content, report_path)

        return report_path

    def _generate_html(self, df_stdf: Dict[str, pl.DataFrame]) -> str:
        """Generate custom HTML content."""
        # Use inherited helpers
        navbar = self.get_template_content("web/navbar.html")
        style = self.get_template_content("web/style.css")

        # Your custom visualizations
        from src.presentation.visualizers.plotly_builder import PlotlyBuilder
        plotly = PlotlyBuilder()

        fig = plotly.create_scatter(
            df_stdf["ptr"],
            x="TestNumber",
            y="Value",
            title="Custom Analysis"
        )

        chart_html = plotly.to_html(fig)

        # Combine into HTML
        html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <style>{style}</style>
        </head>
        <body>
            {navbar}
            <h1>Custom Report</h1>
            {chart_html}
        </body>
        </html>
        """

        return html

# Usage
from src.presentation.report_generators import create_report_generator

# Register your custom generator
GENERATOR_REGISTRY["CUSTOM"] = CustomReportGenerator

# Use it
generator = create_report_generator("CUSTOM", parameter)
report = generator.generate(data_path, output_path)
```

### Example 3: Batch Processing

**Before (No structure):**
```python
import os

def batch_process(stdf_dir, output_dir):
    """Process all STDF files - monolithic, hard to maintain."""
    for file in os.listdir(stdf_dir):
        if file.endswith('.std'):
            # Parse
            data = parse_stdf_file(os.path.join(stdf_dir, file))

            # Save
            save_parquet(data, output_dir)

            # Report
            cmd = f'jupyter nbconvert --execute ...'
            subprocess.call(cmd.split())
```

**After (Use Case composition):**
```python
from pathlib import Path
from typing import List
from src.domain.models.parameter import Parameter
from src.application.use_cases.convert_stdf_use_case import ConvertSTDFUseCase
from src.application.use_cases.generate_report_use_case import GenerateReportUseCase
import logging

def batch_process(
    stdf_dir: Path,
    output_dir: Path,
    parameters: List[Parameter]
) -> List[Path]:
    """Process multiple STDF files using use cases."""
    logger = logging.getLogger(__name__)

    # Create use cases once
    convert_use_case = ConvertSTDFUseCase(logger=logger)
    report_use_case = GenerateReportUseCase(logger=logger)

    results = []

    for stdf_file in stdf_dir.glob("*.std"):
        try:
            # Find matching parameter
            parameter = find_parameter_for_file(stdf_file, parameters)

            # Convert
            parquet_files = convert_use_case.execute(
                stdf_path=str(stdf_file),
                parameter=parameter
            )

            # Generate all reports
            for report_type in ["VOLUME", "LOOP", "TTIME", "YIELD", "CONDITION"]:
                report_path = report_use_case.execute(
                    report_type=report_type,
                    parameter=parameter
                )
                results.append(report_path)

            logger.info(f"Successfully processed {stdf_file}")

        except Exception as e:
            logger.error(f"Failed to process {stdf_file}: {e}")
            continue

    return results

def find_parameter_for_file(
    stdf_file: Path,
    parameters: List[Parameter]
) -> Parameter:
    """Find parameter object matching STDF filename."""
    # Your logic here
    pass
```

---

## Testing Your Migration

### Unit Tests

```python
import pytest
from src.domain.models.parameter import Parameter
from src.application.use_cases.convert_stdf_use_case import ConvertSTDFUseCase

def test_parameter_migration():
    """Test Parameter dataclass creation."""
    param = Parameter(
        code="44E",
        cut="44EZ",
        flow="EWS1",
        type="CHAR",
        lot="L001",
        wafer="01"
    )

    assert param.code == "44E"
    assert param.is_ews_flow is True
    assert "44E" in param.title

def test_use_case_migration(mock_stdf_parser, test_logger):
    """Test use case pattern."""
    use_case = ConvertSTDFUseCase(
        stdf_parser=mock_stdf_parser,
        logger=test_logger
    )

    parameter = Parameter(code="44E", cut="44EZ", flow="EWS1",
                         type="CHAR", lot="L001", wafer="01")

    result = use_case.execute(
        stdf_path="/test.std",
        parameter=parameter
    )

    assert result is not None
    mock_stdf_parser.parse_to_parquet.assert_called_once()
```

### Integration Tests

```python
def test_end_to_end_workflow(tmp_path):
    """Test complete workflow with real files."""
    # Create test STDF file
    stdf_file = tmp_path / "test.std"
    create_test_stdf_file(stdf_file)

    # Create parameter
    parameter = Parameter(
        code="TEST",
        cut="TESTA",
        flow="EWS1",
        type="CHAR",
        lot="L001",
        wafer="01"
    )

    # Run workflow
    from src.application.use_cases.convert_stdf_use_case import ConvertSTDFUseCase
    from src.application.use_cases.generate_report_use_case import GenerateReportUseCase

    convert_use_case = ConvertSTDFUseCase()
    report_use_case = GenerateReportUseCase()

    # Convert
    parquet_files = convert_use_case.execute(
        stdf_path=str(stdf_file),
        parameter=parameter
    )
    assert parquet_files is not None

    # Generate report
    report_path = report_use_case.execute(
        report_type="VOLUME",
        parameter=parameter
    )
    assert report_path.exists()
```

### Type Checking

```bash
# Run mypy to verify type correctness
mypy src/ --strict

# Should see no errors after migration
# Success: no issues found in X source files
```

### Run Test Suite

```bash
# Run all tests
pytest tests/ -v

# Run only unit tests
pytest tests/unit/ -v -m unit

# Run with coverage
pytest tests/ --cov=src --cov-report=html
```

---

## Troubleshooting

### Common Issues

#### Issue 1: Import Errors

**Error:**
```
ModuleNotFoundError: No module named 'src.script.main'
```

**Solution:**
```python
# Update import
# OLD:
from src.script.main import process_analysis

# NEW:
from src.application.use_cases.generate_report_use_case import GenerateReportUseCase
```

#### Issue 2: Parameter Dict vs Object

**Error:**
```
TypeError: 'Parameter' object is not subscriptable
```

**Cause:**
```python
parameter = Parameter(code="44E", ...)
code = parameter["CODE"]  # Wrong! Parameter is not a dict
```

**Solution:**
```python
# Use attribute access
code = parameter.code

# Or convert to dict if needed
param_dict = parameter.to_dict()
code = param_dict["CODE"]
```

#### Issue 3: Report Generator Not Found

**Error:**
```
ValueError: Unknown report type: volume
```

**Cause:**
```python
# Report type must be uppercase
generator = create_report_generator("volume", parameter)  # Wrong
```

**Solution:**
```python
# Use uppercase or let factory handle it
generator = create_report_generator("VOLUME", parameter)  # Correct

# Or use case-insensitive version (factory handles .upper())
generator = create_report_generator("volume", parameter)  # Also works
```

#### Issue 4: Missing Dependencies

**Error:**
```
ModuleNotFoundError: No module named 'jupyter'
```

**Solution:**
```bash
# Jupyter is no longer required for production
# Remove from requirements.txt or comment out

# If needed for development only:
pip install jupyter  # Optional
```

#### Issue 5: Template Not Found

**Error:**
```
FileNotFoundError: Template 'navbar.html' not found
```

**Cause:**
```python
# Wrong template path
self.get_template_content("navbar.html")
```

**Solution:**
```python
# Templates are in web/ subdirectory
self.get_template_content("web/navbar.html")
```

---

## Backward Compatibility

### Using Legacy Code with New Architecture

The new architecture maintains backward compatibility through conversion helpers:

```python
from src.domain.models.parameter import Parameter

# Convert legacy dict to Parameter
def legacy_function_that_expects_dict(param_dict: dict):
    """Old function that expects dict."""
    code = param_dict["CODE"]
    # ... legacy logic

# Use with new Parameter object
parameter = Parameter(code="44E", cut="44EZ", flow="EWS1",
                     type="CHAR", lot="L001", wafer="01")

# Convert for legacy function
param_dict = parameter.to_dict()
legacy_function_that_expects_dict(param_dict)
```

### Mixing Old and New

```python
from src.domain.models.parameter import Parameter
from src.application.use_cases.convert_stdf_use_case import ConvertSTDFUseCase

def hybrid_workflow(param_dict: dict):
    """Mix legacy and new code during transition."""
    # Convert to new format
    parameter = Parameter.from_dict(param_dict)

    # Use new use case
    use_case = ConvertSTDFUseCase()
    result = use_case.execute(stdf_path="/test.std", parameter=parameter)

    # Convert back for legacy code
    legacy_result = parameter.to_dict()

    # Call legacy function
    from src.script.legacy_module import legacy_process
    legacy_process(legacy_result)
```

### Gradual Migration Checklist

- [ ] Start with Parameter model (lowest risk)
- [ ] Update type hints in function signatures
- [ ] Replace report generators (high impact)
- [ ] Adopt use case pattern (architectural change)
- [ ] Update all imports
- [ ] Add tests for migrated code
- [ ] Run full test suite
- [ ] Update documentation

---

## Migration Checklist

### Pre-Migration

- [ ] Read this guide completely
- [ ] Review ARCHITECTURE.md
- [ ] Understand Clean Architecture principles
- [ ] Backup current code
- [ ] Create feature branch
- [ ] Set up test environment

### Phase 1: Domain Layer

- [ ] Update Parameter usage from dict to dataclass
- [ ] Add FileCorner where needed
- [ ] Update function signatures with type hints
- [ ] Test Parameter.from_dict() and to_dict() conversions

### Phase 2: Presentation Layer

- [ ] Replace Jupyter subprocess calls
- [ ] Use create_report_generator() factory
- [ ] Update template paths (web/ subdirectory)
- [ ] Test report generation

### Phase 3: Application Layer

- [ ] Replace direct function calls with use cases
- [ ] Add ConvertSTDFUseCase where needed
- [ ] Add GenerateReportUseCase where needed
- [ ] Implement dependency injection

### Phase 4: Infrastructure Layer

- [ ] Update file operations to use FileRepository
- [ ] Update Parquet operations to use ParquetRepository
- [ ] Use STDFParser instead of direct pystdf
- [ ] Update service imports

### Phase 5: Testing

- [ ] Add unit tests for migrated code
- [ ] Run pytest: `pytest tests/ -v`
- [ ] Run mypy: `mypy src/ --strict`
- [ ] Check coverage: `pytest --cov=src`
- [ ] Fix any failing tests

### Phase 6: Documentation

- [ ] Update inline documentation
- [ ] Update README.md
- [ ] Add migration notes
- [ ] Document any custom changes

### Post-Migration

- [ ] Code review
- [ ] Performance testing
- [ ] User acceptance testing
- [ ] Deploy to production
- [ ] Monitor for issues

---

## Additional Resources

- **ARCHITECTURE.md**: Complete system architecture documentation
- **README.md**: Project overview and setup instructions
- **REFACTORING_STATUS.md**: Current refactoring progress
- **tests/conftest.py**: Test fixtures and examples
- **src/presentation/report_generators/**: Report generator examples

## Need Help?

If you encounter issues during migration:

1. Check the troubleshooting section above
2. Review code examples in this guide
3. Look at test cases in `tests/` directory
4. Check ARCHITECTURE.md for design patterns
5. Contact the development team

---

**Good luck with your migration! The new architecture will make your code more maintainable, testable, and performant.**
