# Databricks notebook source
# MAGIC %md
# MAGIC [pt-br]
# MAGIC ### Criar catalogo
# MAGIC
# MAGIC [en]
# MAGIC ### Create catalog

# COMMAND ----------

spark.sql("CREATE CATALOG IF NOT EXISTS dev")

# COMMAND ----------

# MAGIC %md
# MAGIC [pt-br]
# MAGIC ### Criar camadas do data lakehouse
# MAGIC
# MAGIC [en]
# MAGIC ### Create data lakehouse layers

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS dev.bronze")

spark.sql("CREATE SCHEMA IF NOT EXISTS dev.silver")

spark.sql("CREATE SCHEMA IF NOT EXISTS dev.gold")
