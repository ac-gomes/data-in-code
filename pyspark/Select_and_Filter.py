# Databricks notebook source
# MAGIC %md
# MAGIC [en]
# MAGIC ### How to select and filter values in Pyspark
# MAGIC - I'll load the notebook we created in video #01
# MAGIC
# MAGIC #### Methods
# MAGIC ```python
# MAGIC select()
# MAGIC filter()
# MAGIC where()
# MAGIC isin()
# MAGIC contains() #available in Column class.
# MAGIC startswith()
# MAGIC endswith()
# MAGIC ```
# MAGIC
# MAGIC [pt-br]
# MAGIC ### Como selecionar e filtrar valores no Pyspark
# MAGIC - Vou carregar o notebook que criamos no video #01 
# MAGIC
# MAGIC #### Métodos
# MAGIC ```python
# MAGIC select()
# MAGIC filter()
# MAGIC where()
# MAGIC isin()
# MAGIC contains() #disponivel na classe Column
# MAGIC startswith()
# MAGIC endswith()
# MAGIC ```

# COMMAND ----------

# MAGIC %run ./Create-Table

# COMMAND ----------

# Vamos observar os dados
orders_df.show()

# COMMAND ----------

#Vamos criar uma lista de colunas, uma lista com alguns status e uma lista de países para usar no filtro
my_columns = ["country","product_id","unit_price","status"]

status_list = ["Shipped","In Process","Resolved"]

country_list = ["Norway","Canada","Japan"]

orders_df.select(my_columns)\
    .where(
        orders_df.status.isin(status_list) & orders_df.country.isin(country_list)
    ).show()


# COMMAND ----------

# Vamos importar a função col (ela tem alguns metodos imbutido que são muito uties em operações de transformação e filtro)
from pyspark.sql.functions import col

# COMMAND ----------

# Vamos fazer um filtro por status que contenha as letras/patterns "put"
# lembre a variavel "my_columns" é uma lista de colunas, ela foi definida acima

orders_df.select(my_columns).filter(col("status").contains("put")).show()

# COMMAND ----------

# Bonus
# Técnica de filtragem semelhante ao "LIKE" do SQL
orders_df.filter(orders_df.status.rlike('In')).show()

# COMMAND ----------

# Bonus - comparativo

spark.sql( """ select * from {orders_df} where status like '%put%' """,
         orders_df=orders_df,
         ).show()
