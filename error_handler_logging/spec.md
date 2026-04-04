# Feature Context: Error Handler & Logging

## 🎯 Purpose

Provide a **standardized, reusable logging and error handling framework** for all features in the `vendas_regionais` project. This feature ensures consistent log formatting, comprehensive error tracking with full stack traces, and optional persistence to Delta tables for audit and monitoring.

## 🏗️ Architecture

### Core Component: LogControl Class

A centralized class that wraps Python's `logging` module with enhanced capabilities:

* **Standardized log levels**: INFO, SUCCESS, WARNING, ERROR
* **Automatic stack trace capture**: Function name, line number, exception type, notebook path
* **Structured JSON output**: Machine-readable format for downstream analysis
* **Optional persistence**: Save logs to Delta tables for historical analysis
* **Configurable handlers**: Console output with customizable log levels

## 📋 Business Rules

1. **Mandatory Usage**: All new features MUST use `LogControl` for logging and error handling
2. **No Custom Loggers**: Avoid implementing ad-hoc logging solutions
3. **Error Context**: Always capture full exception context using `error_handler()`
4. **Persistence Policy**:
   * Development: `debug_write_mode=False` (console only)
   * Production: `debug_write_mode=True` (persist to Delta table)
5. **Table Naming**: Log tables should follow `<catalog>.<schema>.logs_<feature_name>` pattern

## 🔧 Technical Specifications

### Instantiation

```python
logger = LogControl(
    logger_name="feature_name",      # Unique identifier for this logger
    tbl_name="catalog.schema.logs",  # Optional: Delta table for persistence
    log_level=logging.INFO           # Default: INFO
)
```

### Log Methods

| Method | Level | Use Case |
|--------|-------|----------|
| `log_info(msg)` | INFO | General information, progress updates |
| `log_success(msg)` | INFO | Successful operation completion |
| `log_warning(msg)` | WARNING | Non-critical issues, deprecation notices |
| `log_error(msg)` | ERROR | Critical errors, failures |

### Error Handler

```python
try:
    # risky operation
except Exception as e:
    logger.error_handler(
        exc=e,                      # Exception object
        debug_write_mode=True       # Persist to Delta table
    )
```

**Captured Information:**
* `asctime`: Timestamp (YYYY-MM-DD HH:MM:SS)
* `levelname`: ERROR
* `exc_type`: Exception class name (e.g., `ValueError`)
* `exc_value`: Exception message
* `function_name`: Function where error occurred
* `line_number`: Exact line number
* `notebook_path`: Full notebook path
* `message`: Formatted error message

### Persistence

```python
# Create DataFrame from log record
df = logger.create_dataframe(log_record)

# Save to Delta table
logger.persist_logs(
    df=df,
    tbl_name="catalog.schema.logs",
    save_mode="append"  # or "overwrite"
)
```

## ✅ Quality Assurance

### Test Coverage

**13 automated tests** (100% passing):

1. **Initialization**: Default and custom parameters
2. **Log Levels**: All four log methods validated
3. **Error Handler**: Multiple exception types (ZeroDivisionError, ValueError, KeyError)
4. **DataFrame Creation**: None handling
5. **Edge Cases**: Unicode, special characters, multiple logger instances

**Test Location**: `/tests/test_logger_control`

**How to Run**: Execute Cell 4 in `test_logger_control` notebook

## 🚀 Usage Examples

### Basic Logging

```python
%run "/Users/.../sdd/features/error_handler_logging/src/logger_control"

logger = LogControl(logger_name="data_ingestion")

logger.log_info("Starting data ingestion pipeline")
# Process data...
logger.log_success("Ingestion completed: 1M records loaded")
```

### Error Handling with Persistence

```python
logger = LogControl(
    logger_name="transformation",
    tbl_name="prod.monitoring.transformation_logs"
)

try:
    df = spark.read.table("source_table")
    # Transformation logic...
except Exception as e:
    logger.error_handler(e, debug_write_mode=True)  # Persists to Delta
    raise  # Re-raise after logging
```

## 📦 Dependencies

* `logging` (Python standard library)
* `sys`, `datetime`, `json` (Python standard library)
* `pyspark.sql.DataFrame` (for persistence)

## 🔄 Integration Pattern

```python
# 1. Import at the beginning of notebook/module
%run "/path/to/error_handler_logging/src/logger_control"

# 2. Instantiate logger
logger = LogControl(logger_name="my_feature", tbl_name="logs.my_feature")

# 3. Use throughout the code
logger.log_info("Process started")

try:
    # Business logic
    logger.log_success("Operation successful")
except Exception as e:
    logger.error_handler(e, debug_write_mode=True)
```

## 📐 Design Rationale

* **Centralization**: Single source of truth for logging prevents inconsistencies
* **Traceability**: Full stack trace enables rapid debugging in production
* **Auditability**: Delta table persistence supports compliance and monitoring
* **Reusability**: Shared class reduces code duplication across features
* **Testability**: 100% test coverage ensures reliability

## 🚫 Anti-Patterns to Avoid

* ❌ Using `print()` statements for logging
* ❌ Implementing custom logging without standardization
* ❌ Ignoring exceptions without logging
* ❌ Not capturing full stack trace context
* ❌ Using `try/except: pass` without error handling

## 📌 Maintenance Notes

* **Owner**: Data Engineering Team
* **Status**: Production-ready ✅
* **Version**: 1.0
* **Last Updated**: 2024
* **Breaking Changes**: None expected (backward compatible)

---

**This feature is a MANDATORY dependency for all new features in the `vendas_regionais` project.**
