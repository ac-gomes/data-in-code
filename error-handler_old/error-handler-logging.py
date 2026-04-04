# Databricks notebook source
# MAGIC %md
# MAGIC _[pt-br]_
# MAGIC #### Se o seu pipeline ou workflow é composto por vários notebooks, aqui está um exemplo de como você pode coletar os logs deles.
# MAGIC
# MAGIC _[en]_
# MAGIC #### If your pipeline or workflow is made up of multiple notebooks, here is an example of how you can collect logs from them.
# MAGIC __(Este é um exemplo com objetivo didatico)__

# COMMAND ----------

# MAGIC %run ./error-handler

# COMMAND ----------

###########Settings######################
debug_write_mode = None
tb_name = 'log.tb_log_v01'

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Vamos criar duas funções para testar 

# COMMAND ----------

# Função teste 1
def divisao_por_zero(a,b):
    try:        
        result = a/b
        return result
    except Exception as e:
        error_handler(e, debug_write_mode)

# COMMAND ----------

# Executar função 1
divisao_por_zero(2,0)

# COMMAND ----------

# Função teste 2
def read_file():
    try:
        with open('meu_arquivo.csv') as file:
            read_data = file.read()
    except Exception as e:
        error_handler(e, debug_write_mode)    

# COMMAND ----------

# Função teste 2
read_file()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Vamos criar um DB/SCHEMA para armazenar a tabela de logs

# COMMAND ----------

# Vamos criar a tabela de logs dentro do workspace do usuário atual...
#### Em um workspace de time, o ideal é deixar no proprio workspace apontando a um storage externo

from pyspark.sql import functions as F

current_user = (spark
                .createDataFrame([("",)], "user string")
                .withColumn("user", F.expr("current_user"))
                .head()["user"]
               )   


# COMMAND ----------

# Criar o diretório para escrever os logs
folder_location = 'log'
location = f"/FileStore/{current_user}/{folder_location}"

spark.sql(f"CREATE DATABASE IF NOT EXISTS log LOCATION '{location}'") # :-)


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from log.tb_log_v01;
