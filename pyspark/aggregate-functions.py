# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC [en]
# MAGIC ### How to aggregate data in  PySpark
# MAGIC - I'll load the notebook we created in video #01
# MAGIC
# MAGIC #### Methods
# MAGIC ```python
# MAGIC groupBy()
# MAGIC sum()
# MAGIC count()
# MAGIC orderBy()
# MAGIC ```
# MAGIC
# MAGIC [pt-br]
# MAGIC ### Como agregar valores em Pyspark
# MAGIC - Vou carregar o notebook que criamos no video #01 
# MAGIC
# MAGIC #### Métodos
# MAGIC ```python
# MAGIC groupBy()
# MAGIC sum()
# MAGIC count()
# MAGIC orderBy()
# MAGIC ```

# COMMAND ----------

# MAGIC %run ./Create-Table

# COMMAND ----------

# MAGIC %md
# MAGIC ### Vamos importar as função;
# MAGIC - count()
# MAGIC - col()
# MAGIC - sum()

# COMMAND ----------

from pyspark.sql.functions import count, col, sum

# COMMAND ----------

# HAVING - SQL
spark.sql("""
          select
            status,
            count(order_number) as qtd_pedidos,
            sum(qty_ordered) as qtd_itens
          from {orders_df}
          group by status
          having qtd_pedidos > 1          
          """,
          orders_df = orders_df
        ).show()

# COMMAND ----------

# Mesma logica, pois pyspark não tem a clausula Having
orders_df.groupBy("status")\
    .agg(count("order_number").alias("qtd_pedidos"),\
        sum("qty_ordered").alias("qtd_itens"))\
    .where(col("qtd_pedidos") >=4)\
    .orderBy("qtd_itens")\
    .show()                

