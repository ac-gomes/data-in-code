# LogControl Test Suite

## Location
This test suite is located at: `tests/test_logger_control.md`

The LogControl implementation being tested is centralized at:
- **Centralized repo**: `/Workspace/Users/data.in.code/data-in-code/error_handler_logging/src/logger_control`
- **Local docs**: `sdd/features/error_handler_logging/` (plan, spec, tasks, traceability)

## Running Tests

### Option 1: Run inline tests in the centralized notebook
Execute the test cells in the centralized logger_control notebook:
```
/Workspace/Users/data.in.code/data-in-code/error_handler_logging/src/logger_control
```

### Option 2: Use pytest (advanced)
From the project root (`vendas_regionais/`):
```bash
# If tests were local (currently centralized)
pytest tests/test_logger_control.md -v
```

## Test Coverage

* ✅ Initialization (default and custom parameters)
* ✅ Log level methods (info, success, warning, error)
* ✅ Error handler (multiple exception types)
* ✅ DataFrame creation
* ✅ Edge cases (unicode, special chars, multiple instances)

## Total: 13 tests, 100% passing

## References

* **Implementation**: Centralized at `/data-in-code/error_handler_logging/src/logger_control`
* **Documentation**: `sdd/features/error_handler_logging/` (relative to project root)
* **Usage examples**: See notebooks in `src/` (e.g., `src/nb_vendas_base_ingestion.py`)
