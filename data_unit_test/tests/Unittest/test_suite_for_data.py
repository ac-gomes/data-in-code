# Databricks notebook source
# MAGIC %md
# MAGIC ### Test suite for table validation
# MAGIC
# MAGIC ### Conjunto de testes para validação de tabelas

# COMMAND ----------

# MAGIC %run ./Unit_test_unittest

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test suite for metrics validation
# MAGIC
# MAGIC ### Conjunto de testes para validação de métricas

# COMMAND ----------

# MAGIC %run ./Unit_test_unittest_metrics

# COMMAND ----------

# Fábrica para criar uma nova classe de teste com o target_table embutido (injeção de dependência com class factory)

def create_test_suite_for_table(target_table):
    
    class PatchedTestSuite(DataQualityTestSuite):
        def __init__(self, methodName='runTest'):
            super().__init__(methodName=methodName, target_table=target_table)
            
    PatchedTestSuite.__name__ = f"DataQualityTestSuite_{target_table.replace('.', '_')}"

    return unittest.TestLoader().loadTestsFromTestCase(PatchedTestSuite)

# COMMAND ----------

# target_table='dev.bronze.tbl_bronze_product_catalog'
target_table='dev.bronze.tbl_bronze_order_events'

metrics_quality_suite = create_test_suite_for_table(target_table)
tests = unittest.TestSuite([metrics_quality_suite])
runner = unittest.TextTestRunner(verbosity=2)
runner.run(tests)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from dev.bronze.tbl_bronze_product_catalog
