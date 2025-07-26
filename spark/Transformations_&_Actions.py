# Databricks notebook source
# MAGIC %md
# MAGIC ## Transformations & Actions no Spark
# MAGIC  - Catalyst Optimizer
# MAGIC  - DAGScheduler

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformations
# MAGIC São operações declarativas que crieam um novo RDD/DataFrame, mas não são executadas até que uma action entre em cena.
# MAGIC
# MAGIC - **Exemplos:**
# MAGIC
# MAGIC   - ``map()``
# MAGIC
# MAGIC   - ``filter()``
# MAGIC
# MAGIC   - ``select()``
# MAGIC
# MAGIC   - ``withColumn()``
# MAGIC
# MAGIC   - ``join()``
# MAGIC
# MAGIC   - ``groupBy()``
# MAGIC
# MAGIC   - ``distinct()``
# MAGIC
# MAGIC São chamadas de 'Lazy', em resumo o Spark constroi apenas um plano logico, mas não o executa. Esse plano precisa ser avaliado.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Action
# MAGIC São operações que disparam a execução real do plano lógico criado pelas transformations. As ações retornam um valor para o driver ou gravam dados em uma saída externa.inicia os stages e tarefas no cluster.
# MAGIC
# MAGIC - **Exemplos:**
# MAGIC
# MAGIC   - ``count()``
# MAGIC
# MAGIC   - ``collect()``
# MAGIC
# MAGIC   - ``show()``
# MAGIC
# MAGIC   - ``write()``
# MAGIC
# MAGIC   - ``take``
# MAGIC
# MAGIC Action: o Spark executa o job

# COMMAND ----------

# MAGIC %sql
# MAGIC --- Criar Volume Gerenciado em dev
# MAGIC CREATE VOLUME dev.bronze.flat_files
# MAGIC

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

resul_df = (
    orders_df
    .filter(orders_df.region == 'SP')
    .groupBy("event_type", "is_returned")
    .agg({"price": "avg"})
    .orderBy("avg(price)", ascending=False)
    .limit(10)
)

# COMMAND ----------

resul_df.explain(True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Catalyst Optimizer
# MAGIC
# MAGIC - Otimização do Plano de Execução
# MAGIC
# MAGIC - Ajuste Fino de Consultas
# MAGIC   - classificando as operações em transformações estreitas (narrow) e largas (wide). Essa orquestração inteligente contribui significativamente para o desempenho do Spark
# MAGIC
# MAGIC - Predicate Pushdown
# MAGIC   - Em vez de ler todos os dados e depois filtrá-los na memória, o Catalyst move o filtro para mais perto da fonte de dados
# MAGIC   
# MAGIC - Seleção do Plano Mais Eficiente
# MAGIC   - Em certos cenários, o Spark pode gerar múltiplos planos físicos com base nas estratégias de execução propostas pelo Catalyst. Nesses casos, o Modelo de Custo do Spark seleciona o plano mais eficient   

# COMMAND ----------

resul_df = (
    orders_df
    .filter(orders_df.region == 'SP') # Transformation 1
    .groupBy("event_type", "is_returned") # Transformation 2
    .agg({"price": "avg"}) # Transformation 3
    .orderBy("avg(price)", ascending=False) # Transformation 4 
)

resul_df.show() # Action

#Physical Plan (executável no cluster com Photon) em alto nivel
FileScan CSV [event_type, price, region, is_returned]
  └─ PhotonFilter: region IS NOT NULL AND region = 'SP'
     └─ PhotonProject: event_type, price, is_returned
        └─ PhotonGroupingAgg (partial): avg(price) BY event_type, is_returned
           └─ Shuffle (hash partitioning by event_type, is_returned)
              └─ PhotonGroupingAgg (final): avg(price)
                 └─ Shuffle (range partitioning by avg(price) DESC)
                    └─ PhotonSort: avg(price) DESC NULLS LAST
                       └─ ColumnarToRow
                          └─ Output: event_type, is_returned, avg(price)

# COMMAND ----------

# MAGIC %md
# MAGIC ## DAGScheduler 
# MAGIC  - ### Gerencia a execução de estágios, dependências e tratamento de falhas em um cluster Spark
# MAGIC  - O DAGScheduler é uma camada central de agendamento do Apache Spark, rodando exclusivamente no driver
# MAGIC  - O DAGScheduler é fundamental para a tolerância a falhas do Spark, especialmente no contexto de shuffles.
# MAGIC  
# MAGIC  **Responsabilidades:**
# MAGIC  - Computar um Grafo Acíclico Dirigido (DAG) de execução (DAG de estágios) para cada job.
# MAGIC  - Determinar as localizações preferenciais para a execução de cada tarefa. (o Spark minimiza a movimentação de dados pela rede ao executar uma tarefa no mesmo nó onde os dados necessários já residem)
# MAGIC  - Gerenciar falhas devido à perda de arquivos de saída de shuffle.

# COMMAND ----------

## Árvore do Plano Físico - DAG Stages
Stage 1: Leitura e pré-processamento
  - FileScan CSV
  - PhotonFilter
  - PhotonProject
  - PhotonGroupingAgg (partial)
  - → shuffle: hash partitioning by (event_type, is_returned)

Stage 2: Agregação final
  - PhotonGroupingAgg (final)
  - → shuffle: range partitioning by avg(price)

Stage 3: Ordenação final + coleta
  - PhotonSort: avg(price) DESC
  - ColumnarToRow (prepara para exibir .show())
  - Output: envia resultado ao driver


# COMMAND ----------

# MAGIC %md
# MAGIC ### Resumo do fluxo de execução
# MAGIC Transformations criam um plano lógico (lazy).
# MAGIC
# MAGIC Action dispara o job.
# MAGIC
# MAGIC Catalyst optimizer gera o physical plan, construindo o DAG.
# MAGIC
# MAGIC DAGScheduler quebra o DAG em stages (com base em shuffles).
# MAGIC
# MAGIC Cada stage é dividido em várias tasks (uma por partition).
# MAGIC
# MAGIC TaskScheduler envia as tasks aos executors nos workers.
# MAGIC
# MAGIC Os executors rodam as tasks e enviam resultados ao driver.
# MAGIC
# MAGIC Se for um ResultStage, o driver recebe o resultado da action. <br>
# MAGIC
# MAGIC
# MAGIC ```sh
# MAGIC [ Transformations (lazy) ]
# MAGIC             ↓
# MAGIC        [ Action ]
# MAGIC             ↓
# MAGIC     Catalyst Optimizer
# MAGIC             ↓
# MAGIC        Logical Plan
# MAGIC             ↓
# MAGIC        Physical Plan
# MAGIC             ↓
# MAGIC         DAG of stages
# MAGIC             ↓
# MAGIC       ┌────────────┐
# MAGIC       │ Stage 1    │ —> tasks (partitions)
# MAGIC       │ Stage 2    │ —> tasks
# MAGIC       └────────────┘
# MAGIC             ↓
# MAGIC       Executors run tasks
# MAGIC             ↓
# MAGIC       Resultado no driver
# MAGIC
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Conceitos e Propriedades:
# MAGIC
# MAGIC  - Transformações Narrow: Operações RDD (como map, filter) onde cada partição da RDD pai contribui com no máximo uma partição para a RDD filha. Não requerem embaralhamento de dados.
# MAGIC
# MAGIC  - Transformações Wide: Operações RDD (como groupByKey, join, reduceByKey) onde cada partição da RDD pai pode contribuir com múltiplas partições para a RDD filha. Exigem embaralhamento de dados (shuffle), que é uma operação cara.
# MAGIC
# MAGIC - ShuffleDependency: Uma dependência entre RDDs que indica uma transformação wide e requer embaralhamento de dados.
# MAGIC
# MAGIC - ResultStage: O estágio final de um Job, que contém ResultTasks que computam e retornam o resultado final ao Driver.
# MAGIC
# MAGIC - ShuffleMapStage: Um estágio que produz saídas de embaralhamento (map outputs) que serão consumidas por um estágio subsequente (normalmente um estágio Result ou outro estágio ShuffleMap).
# MAGIC
# MAGIC - TaskSet: Um conjunto de tarefas de um único estágio que o TaskScheduler submete para execução.
# MAGIC
# MAGIC - ActiveJob: Uma representação interna de um job ativo no DAGScheduler, mantendo informações como o jobId, a callSite, o JobListener e as propriedades de agendamento.
# MAGIC
# MAGIC - BarrierJobSlotsNumberCheckFailed: Uma exceção lançada quando um estágio de barreira (barrier stage) requer um número específico de slots de tarefa, mas os recursos disponíveis são insuficientes.
# MAGIC
# MAGIC - AccumulatorV2: A API para acumuladores no Spark, que permite que as tarefas construam valores de forma eficiente fora da execução do DAG, como contadores ou somas.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📚 Referências oficiais
# MAGIC
# MAGIC 📄 Stages and Tasks (Performance Tuning): https://spark.apache.org/docs/latest/tuning.html#level-of-parallelism
# MAGIC
# MAGIC 📄 Spark DataFrame API Docs: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/index.html
# MAGIC
# MAGIC Stages: https://books.japila.pl/apache-spark-internals/scheduler/DAGScheduler/
