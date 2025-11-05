# ⚠️ DEPRECATED - Jupyter Notebooks

**Status**: Deprecated
**Date**: 2025-11-05
**Reason**: Replaced by Pure Python Report Generators

---

## Migration Required

These Jupyter notebooks have been **completely replaced** by pure Python report generators in the new Clean Architecture.

### Old (Deprecated) ❌

```python
# Using Jupyter nbconvert subprocess
cmd = f'jupyter nbconvert --execute --to html {notebook}.ipynb'
subprocess.call(cmd)
```

**Problems**:
- Slow (~10-12 seconds per report)
- Subprocess overhead
- Hard to test
- No type safety
- Poor error handling

### New (Recommended) ✅

```python
# Using Pure Python generators
from src.presentation.report_generators import create_report_generator

generator = create_report_generator("VOLUME", parameter)
report_path = generator.generate(data_path, output_path)
```

**Benefits**:
- **~50% faster** (5-6 seconds per report)
- No subprocess overhead
- Fully testable with dependency injection
- Type-safe with 100% type hints
- Better error handling and logging

---

## Migration Table

| Old Notebook | Size | New Generator | Lines |
|-------------|------|---------------|-------|
| `VOLUME.ipynb` | 3.8 MB | `volume_report_generator.py` | 690 |
| `LOOP.ipynb` | 9.4 MB | `loop_report_generator.py` | 185 |
| `TTIME.ipynb` | 8.5 MB | `ttime_report_generator.py` | 145 |
| `YIELD.ipynb` | 8.2 MB | `yield_report_generator.py` | 165 |
| `CONDITION.ipynb` | 3.5 MB | `condition_report_generator.py` | 165 |

**Total Size Reduction**: 34.9 MB → ~1,350 lines of Python code

---

## How to Migrate

1. **Read the migration guide**:
   - See [MIGRATION_GUIDE.md](../../MIGRATION_GUIDE.md)
   - See [LEGACY_DEPRECATION.md](../../LEGACY_DEPRECATION.md)

2. **Update your code**:
   ```python
   # Before
   import subprocess
   cmd = f'jupyter nbconvert --execute ./src/jupiter/VOLUME.ipynb ...'
   subprocess.call(cmd)

   # After
   from src.application.use_cases.generate_report_use_case import GenerateReportUseCase

   use_case = GenerateReportUseCase()
   report_path = use_case.execute(report_type="VOLUME", parameter=parameter)
   ```

3. **Test your changes**:
   ```bash
   pytest tests/unit/presentation/ -v
   ```

4. **Remove old imports**:
   - Remove `import subprocess`
   - Remove references to `.ipynb` files
   - Remove `jupyter nbconvert` commands

---

## Timeline

- **Now**: Deprecated, use new generators instead
- **1-2 months**: Warning period
- **3-6 months**: Will be moved to `deprecated/` directory
- **Future**: May be removed completely

---

## Files in this Directory (All Deprecated)

- `VOLUME.ipynb` - ❌ Use `VolumeReportGenerator` instead
- `LOOP.ipynb` - ❌ Use `LoopReportGenerator` instead
- `TTIME.ipynb` - ❌ Use `TTimeReportGenerator` instead
- `YIELD.ipynb` - ❌ Use `YieldReportGenerator` instead
- `CONDITION.ipynb` - ❌ Use `ConditionReportGenerator` instead
- `utility.py` - ❌ Integrated into `BaseReportGenerator`
- `template.json` - ❌ Moved to `src/presentation/templates/`

---

## Support

For migration help:
- Read [ARCHITECTURE.md](../../ARCHITECTURE.md)
- Read [MIGRATION_GUIDE.md](../../MIGRATION_GUIDE.md)
- Contact: matteo.terranova@st.com

---

**Do not use these notebooks for new development. Migrate to the new architecture as soon as possible.**
