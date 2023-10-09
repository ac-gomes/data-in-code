# Databricks notebook source
# MAGIC %run ./Create-Table

# COMMAND ----------

# MAGIC %md
# MAGIC #### Primeiro vamos fazer um merge do delta vindo da comparação de duas tabelas com a mesma strutura/schema em SQL.

# COMMAND ----------

#  Vamos converter os DFs em views temporarias para usarmos como exemplo
#  orders_df
#  order_new_lines_df

orders_df.createOrReplaceTempView("orders")

order_new_lines_df.createOrReplaceTempView("order_new_lines")


# COMMAND ----------

# MAGIC %md
# MAGIC #### Vamos ver os dados

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Exibir dados da tabelas old_orders
# MAGIC SELECT * FROM orders;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Exibir dados da tabelas new_orders
# MAGIC -- Uma dessas 4 linhas já existe na view old_orders
# MAGIC
# MAGIC SELECT * FROM order_new_lines;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Como podemos ver na view new_orders tem uma linha que já existe na view old_orders.
# MAGIC -- Temos que fazer o merge de novas linhas na view old_orders. 
# MAGIC -- E para isso vamos ter que filtrar apenas o que é realmente novo.
# MAGIC -- E Finalmente executamos o merge => E vamos aprender como fazer o mesmo em Pyspark. 😎
# MAGIC
# MAGIC --- Vamos ver os dados, apenas 3 linhas de diferenças entre as duas views
# MAGIC SELECT * 
# MAGIC   FROM order_new_lines AS A
# MAGIC   WHERE NOT EXISTS (SELECT * FROM orders AS B WHERE A.order_number = B.order_number)
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT * FROM orders;
# MAGIC
# MAGIC -- agora temos um total de 23 linhas.... :-)
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Agora iremos executar esta mesma logica em Pyspark

# COMMAND ----------

#  orders_df
#  order_new_lines_df
# neste caso iremos usar os DFs gerados lá no inicio, vamos testa-los.

# Pegar apenas as linhas novas (delta)

delta_df = order_new_lines_df.exceptAll(orders_df)

orders_df = orders_df.unionAll(delta_df)

orders_df.display()
# Temos aí o DF com as novas linhas ahaha

# COMMAND ----------

# verificar qtd de linhas 
orders_df.count()
order_new_lines_df.count()
str = f"qtd linhas old orders: {orders_df.count()} \nqtd linhas new orders: {order_new_lines_df.count()}"
print(str)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bonus:
# MAGIC  - Um outro metodo para chegar ao mesmo resultados

# COMMAND ----------

# Obs: join será explicado nos videos seguintes

delta_df2 = order_new_lines_df.join(orders_df, order_new_lines_df.order_number == orders_df.order_number, "left_anti")

orders_df = orders_df.unionAll(delta_df2)

orders_df.display()
