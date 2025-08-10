# Databricks notebook source
# MAGIC %md
# MAGIC ## Lógica de Programação para Engenharia de Dados - Nivelamento

# COMMAND ----------

# MAGIC %md
# MAGIC ### Revisão Transformations_&_Actions
# MAGIC
# MAGIC #### Fluxo resumido:
# MAGIC
# MAGIC - Programa Driver cria RDDs e executa uma ação (count);
# MAGIC
# MAGIC - SparkContext solicita recursos ao Gerenciador de Cluster;
# MAGIC
# MAGIC - DAG Scheduler cria o plano lógico de execução (grafo);
# MAGIC
# MAGIC - Task Scheduler envia as tarefas para os Executores nos Workers;
# MAGIC
# MAGIC - Executores executam as tarefas e retornam os resultados ao Driver.
# MAGIC
# MAGIC Fontes:
# MAGIC
# MAGIC https://spark.apache.org/docs/latest/cluster-overview.html
# MAGIC
# MAGIC https://data-flair.training/blogs/how-apache-spark-works/

# COMMAND ----------

# MAGIC %md
# MAGIC ### Comparativos: Imperativo vs Declarativo no Apache Spark

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType, TimestampType

file_path ='/Volumes/dev/bronze/flat_files/orders.csv'

schema = StructType([
    StructField("event_id", StringType(), False),
    StructField("user_id", StringType(), False),
    StructField("order_id", StringType(), False),
    StructField("product_id", StringType(), False),
    StructField("event_type", StringType(), False),
    StructField("price", DoubleType(), True),
    StructField("region", StringType(), True),
    StructField("event_timestamp", TimestampType(), True),
    StructField("is_delayed", BooleanType(), True),
    StructField("is_returned", BooleanType(), True)
])

orders_df = spark.read.schema(schema).csv(file_path, header=True, inferSchema=False)

# COMMAND ----------

orders_df.display()

# COMMAND ----------

# DBTITLE 1,Exemplo 1

#############################  Imperativo ruim #############################
total = 0
count = 0
for row in orders_df.collect():
    if row["event_type"] == "ORDER_RETURNED" and row["is_returned"] == True:
        total += row["price"]
        count += 1

avg_price = total / count if count > 0 else 0
print(avg_price)

# COMMAND ----------

# DBTITLE 1,Exemplo 1
#############################  Funcional declarativa ideal #############################

from pyspark.sql.functions import col, avg

orders_df.filter(
  (col("event_type") == "ORDER_RETURNED") 
  & (col("is_returned") == True)
  ) \
  .agg(avg("price")) \
  .show()

# COMMAND ----------

# DBTITLE 1,Exemplo 2
#############################  Imperativo ruim #############################

counts = {}
for row in orders_df.collect():
    key = row["region"]
    counts[key] = counts.get(key, 0) + 1
print(counts)

# COMMAND ----------

# DBTITLE 1,Exemplo 2
#############################  Declarativa Funcional ideal #############################

orders_df.groupBy("region").count().show()

# COMMAND ----------

# DBTITLE 1,Exemplo 3
#############################  Imperativo ruim #############################

new_rows = []
for row in orders_df.collect():

    if row["is_delayed"] == True:
        status = "Entregue com atraso"
        new_rows.append((row["order_id"], status))
    else:
        status = "Entregue a tempo"
            

print(new_rows)

# COMMAND ----------

# DBTITLE 1,Exemplo 3
#############################  Declarativo Funcional ideal #############################

from pyspark.sql.functions import when, col

orders_df = orders_df.withColumn(
    "status",
    when(orders_df["is_delayed"] == True, "Entregue com atraso").otherwise("Entregue a tempo")
).filter(col("status") == "Entregue com atraso").show()


# COMMAND ----------

# MAGIC %md
# MAGIC ✅ Boas práticas Spark (funcional + eficiente)
# MAGIC
# MAGIC ✔ Use encadeamento de transformações (lazy)
# MAGIC
# MAGIC ✔ Use funções nativas do Spark SQL (filter, select, agg, withColumn)
# MAGIC
# MAGIC ✔ Prefira expressões com col(), when(), lit() ao invés de lógica Python pura
# MAGIC
# MAGIC ✔ Evite collect(), loops for, if/else fora de transformações
# MAGIC
# MAGIC ✔ Cache apenas quando necessário
# MAGIC
# MAGIC ✔ UDFs só quando não há alternativa nativa (por questão de performance)
# MAGIC
# MAGIC ✔ Leitura e escrita devem ser no formato colunar (Parquet, Delta etc.)

# COMMAND ----------

# MAGIC %md
# MAGIC fonte:
# MAGIC
# MAGIC https://learn.microsoft.com/pt-br/azure/databricks/pyspark/basics 
# MAGIC
# MAGIC https://data-flair.training/blogs/how-apache-spark-works/
# MAGIC
# MAGIC https://www.ssp.sh/brain/functional-data-engineering/
# MAGIC
# MAGIC https://www.dataengineeringweekly.com/p/functional-data-engineering-a-blueprint
# MAGIC
# MAGIC https://maximebeauchemin.medium.com/functional-data-engineering-a-modern-paradigm-for-batch-data-processing-2327ec32c42a
# MAGIC
# MAGIC https://www.instaclustr.com/education/apache-spark/apache-spark-architecture-concepts-components-and-best-practices/
# MAGIC
