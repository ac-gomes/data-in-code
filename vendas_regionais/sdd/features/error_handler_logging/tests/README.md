# LogControl Test Suite

## Location
This test suite is located in the same directory as the LogControl notebook.

## Running Tests

### Option 1: Run inline tests in the notebook
Execute Cell 5 "Run All Tests - Inline" in the logger_control notebook.
This runs 13 comprehensive tests covering all functionality.

### Option 2: Use pytest (advanced)
```bash
pytest /Workspace/Users/data.in.code@gmail.com/data-in-code/vendas_regionais/sdd/features/error_handler_logging/src/tests/ -v
```

## Test Coverage

* ✅ Initialization (default and custom parameters)
* ✅ Log level methods (info, success, warning, error)
* ✅ Error handler (multiple exception types)
* ✅ DataFrame creation
* ✅ Edge cases (unicode, special chars, multiple instances)

## Total: 13 tests, 100% passing
