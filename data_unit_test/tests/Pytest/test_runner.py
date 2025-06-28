# Databricks notebook source
# MAGIC %pip install pytest

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

# MAGIC %run ./Unit_test_pytest
# MAGIC

# COMMAND ----------

# MAGIC %run ./Unit_test_pytest_metrics

# COMMAND ----------

import traceback
import inspect
import time
from datetime import datetime

def run_tests(test_funcs, fixture_funcs=None, target_table=None, log_path=None):
    """
    Runner estilo pytest com:
    - Injeção de fixtures
    - Saída formatada
    - Registro de logs no DataFrame Spark e opcionalmente salva como Iceberg ou delta
    """
    fixtures = {}
    logs = []

    
    if fixture_funcs:
        if isinstance(fixture_funcs, dict):
            for name, fx in fixture_funcs.items():
                fixtures[name] = fx()
        elif isinstance(fixture_funcs, list):
            for fx in fixture_funcs:
                fixtures[fx.__name__] = fx()

    
    if target_table:
        fixtures['target_table'] = target_table

    for func in test_funcs:
        start = time.time()
        test_name = func.__name__
        desc = (func.__doc__ or "").strip()
        try:
            sig = inspect.signature(func)
            kwargs = {
                pname: fixtures[pname]
                for pname in sig.parameters
                if pname in fixtures
            }

            func(**kwargs)

            duration = time.time() - start
            print(f"✅ {test_name} PASSED ({duration:.2f}s)")
            # if desc:
            #    print(f"📘 {desc}")
            logs.append((test_name, "PASSED", "", duration, datetime.now()))

        except AssertionError as e:
            duration = time.time() - start
            print(f"❌ {test_name} FAILED: {e} ({duration:.2f}s)")
            logs.append((test_name, "FAILED", str(e), duration, datetime.now()))

        except Exception as e:
            duration = time.time() - start
            print(f"💥 {test_name} ERROR: ({duration:.2f}s)")
            traceback.print_exc()
            logs.append((test_name, "ERROR", str(e), duration, datetime.now()))

    
    spark = fixtures.get("spark")
    if spark and logs:
        log_df = spark.createDataFrame(logs, ["test_name", "status", "message", "duration", "timestamp"]) 
        

        if log_path:
            log_df.write.mode("append").format("iceberg").save(log_path)
            print(f"\n📁 Logs salvos em: {log_path}")
        else:
            print("\n📋 Resultado dos testes:")
            log_df.show()

# COMMAND ----------

target_table='dev.silver.tbl_silver_product_catalog'
# target_table ='dev.bronze.tbl_bronze_order_events'

# test_funcs=[test_column_unique, test_column_not_null, test_schema_validations]
# test_funcs=[test_returned_orders]

run_tests(
    test_funcs=[test_column_unique, test_column_not_null, test_schema_validations],
    fixture_funcs={'spark':  _create_spark_session},
    target_table=target_table,
    log_path=None  # None to show in notbooks, path to write in iceberg or delta format
)
