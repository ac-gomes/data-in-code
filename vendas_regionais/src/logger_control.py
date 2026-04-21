# Databricks notebook source
# DBTITLE 1,Imports
# Bibliotecas necessárias para a classe LogControl
import logging
import sys
import datetime as _dt
import json
from pyspark.sql import DataFrame

# COMMAND ----------

# Class with standard methods for logging and error handling

class LogControl:
    """Standardized logging class for Data Engineering projects following Spec-Driven Development (SDD).

    This class provides a unified interface for logging operations with multiple severity levels,
    error handling with full stack trace capture, and optional persistence to Delta tables.

    Args:
        logger_name (str, optional): Name identifier for the logger instance. 
            Defaults to "logcontrol_log_cls".
        tbl_name (str, optional): Fully qualified table name (catalog.schema.table) for 
            persisting logs. If None, logs are only output to console. Defaults to None.

    Attributes:
        _logger (logging.Logger): Internal Python logger instance.
        tbl_name (str): Table name for log persistence.

    Usage:
        Basic logging:
            logger = LogControl(logger_name="my_pipeline_log")
            logger.log_info("Loading file...")
            logger.log_success(f"Loaded {count} records successfully")
            logger.log_warning("Column not found, using default value")
            logger.log_error("Table not found")

        Error handling with stack trace:
            logger = LogControl(logger_name="my_pipeline_log", 
                                tbl_name="logs.error_logs")
            try:
                write_table()
            except Exception as e:
                logger.error_handler(e, debug_write_mode=True)

        Persisting logs to table:
            logger = LogControl(tbl_name="logs.application_logs")
            logger.error_handler(exception, debug_write_mode=True)
    """

    def __init__(self, logger_name: str = "logcontrol_log_cls", tbl_name: str = None):
        self._logger = logging.getLogger(logger_name)
        self._logger.setLevel(logging.DEBUG)
        self.tbl_name = tbl_name

        if not self._logger.hasHandlers():
           _handler = logging.StreamHandler()
           _handler.setLevel(logging.DEBUG)
           _handler.setFormatter(logging.Formatter("%(asctime)s  - %(levelname)s - %(message)s"))
           self._logger.addHandler(_handler)
           self._logger.propagate = False

    # -- Log methods by level

    def log_info(self, message: str) -> None:
        """Log an informational message.
        
        Args:
            message (str): The message to log.
        
        Returns:
            None
        """
        self._logger.info(f"[INFO] {message}")

    def log_success(self, message: str) -> None:
        """Log a success message (uses INFO level with SUCCESS prefix).
        
        Args:
            message (str): The success message to log.
        
        Returns:
            None
        """
        self._logger.info(f"[SUCCESS] {message}")

    def log_warning(self, message: str) -> None:
        """Log a warning message.
        
        Args:
            message (str): The warning message to log.
        
        Returns:
            None
        """
        self._logger.warning(f"[WARNING] {message}")

    def log_error(self, message: str) -> None:
        """Log an error message.
        
        Args:
            message (str): The error message to log.
        
        Returns:
            None
        """
        self._logger.error(f"[ERROR] {message}")    

    # -- Persist Logs

    def create_dataframe(self, log_record: dict) -> DataFrame:
        """Create a PySpark DataFrame from a log record dictionary.
        
        Args:
            log_record (dict): Dictionary containing log information with keys like
                'asctime', 'levelname', 'message', etc.
        
        Returns:
            DataFrame: A single-row PySpark DataFrame containing the log record,
                or None if log_record is None.
        """
        if log_record is not None:
            return spark.createDataFrame([log_record])
        return None

    def persist_logs(self, df: DataFrame, tbl_name: str = None) -> None:
        """Persist log DataFrame to a Delta table.
        
        If the table exists, appends the logs. Otherwise, creates a new table.
        
        Args:
            df (DataFrame): PySpark DataFrame containing log records.
            tbl_name (str, optional): Fully qualified table name. If None, uses
                self.tbl_name. Defaults to None.
        
        Returns:
            None
        
        Raises:
            Exception: If tbl_name is None and self.tbl_name is not set.
        """
        if spark.catalog.tableExists(tbl_name):
            df.write.mode("append").saveAsTable(tbl_name)
        else:
            df.write.mode("overwrite").saveAsTable(tbl_name)

    def error_handler(self, exception: Exception, debug_write_mode: bool = None) -> bool:
        """Handle exceptions with full stack trace logging and optional persistence.
        
        Captures complete error context including exception type, value, function name,
        line number, and notebook path. Logs to console and optionally persists to table.
        
        Args:
            exception (Exception): The exception object to handle.
            debug_write_mode (bool, optional): If True, persists logs to the table 
                specified in self.tbl_name. If False or None, only logs to console.
                Defaults to None.
        
        Returns:
            bool: True if logs were persisted to table, None otherwise.
        
        Example:
            try:
                result = risky_operation()
            except Exception as e:
                logger.error_handler(e, debug_write_mode=True)
        """
        exc_type, exc_value, exc_traceback = sys.exc_info()

        log_record = {
            'asctime': _dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'levelname': logging.getLevelName(logging.ERROR),
            'exc_type': exc_type.__name__ if exc_type else 'unknown',
            'exc_value': str(exc_value) if exc_value else '',
            'function_name': exc_traceback.tb_frame.f_code.co_name if exc_traceback else 'unknown',
            'line_number': exc_traceback.tb_lineno if exc_traceback else 0,
            'message': str(exception),
            'notebook_path': dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
        }

        log_json = json.dumps(log_record, default=str, indent=2)

        # -- Log to console
        self._logger.error(f"[ERROR] Something went wrong: {log_json}")

        # -- Log to table when debug_write_mode is True
        if debug_write_mode is True:
            df = self.create_dataframe(log_record)
            if df is not None:
                self.persist_logs(df, tbl_name=self.tbl_name)
                self._logger.info(f"[INFO] Logs persisted to table: {self.tbl_name}")
            return True

# COMMAND ----------

# DBTITLE 1,Testing
# MAGIC %md
# MAGIC # Testing
# MAGIC
# MAGIC ## Test Notebook Location
# MAGIC
# MAGIC A dedicated test notebook has been created:
# MAGIC
# MAGIC **[test_logger_control](#notebook-1521773189207461)**
# MAGIC
# MAGIC Path: `./src/tests/test_logger_control`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## How to Run Tests
# MAGIC
# MAGIC ### Option 1: Open Test Notebook (Recommended)
# MAGIC
# MAGIC 1. Click the link above or navigate to:
# MAGIC    ```
# MAGIC    /Workspace/Users/<USER_EMAIL>/data-in-code/vendas_regionais/tests/test_logger_control
# MAGIC    ```
# MAGIC
# MAGIC 2. The test notebook will:
# MAGIC    * Import the LogControl class from this notebook (using `%run`)
# MAGIC    * Execute 13 comprehensive tests
# MAGIC    * Display results with pass/fail status
# MAGIC
# MAGIC ### Option 2: Run from this Notebook
# MAGIC
# MAGIC ```python
# MAGIC %run ./src/tests/test_logger_control
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Test Coverage
# MAGIC
# MAGIC The test suite includes:
# MAGIC
# MAGIC * ✅ **Initialization** (2 tests) - Default and custom parameters
# MAGIC * ✅ **Log Levels** (4 tests) - info, success, warning, error
# MAGIC * ✅ **Error Handler** (3 tests) - Multiple exception types with stack traces
# MAGIC * ✅ **DataFrame Creation** (1 test) - None handling
# MAGIC * ✅ **Edge Cases** (3 tests) - Special characters, unicode, multiple instances
# MAGIC
# MAGIC **Total: 13 tests**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Project Structure
# MAGIC
# MAGIC ```
# MAGIC error_handler_logging/
# MAGIC ├── logger_control (this notebook)
# MAGIC │   ├── Cell 1: Imports
# MAGIC │   ├── Cell 2: LogControl Class
# MAGIC │   └── Cell 3: Testing Instructions (you are here)
# MAGIC │
# MAGIC └── src/tests/
# MAGIC     ├── test_logger_control (test notebook)
# MAGIC     │   ├── Cell 1: Import LogControl (%run parent)
# MAGIC     │   ├── Cell 2: Documentation
# MAGIC     │   └── Cell 3: Run All Tests
# MAGIC     └── README.md
# MAGIC ```
