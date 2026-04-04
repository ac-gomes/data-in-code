# Databricks notebook source
####### This is not a good approach,this is just a demonstration #######
####### Please, use an Azure container or an AWS S3 bucket '/{year}/{month}/{day}.txt' #######

# Criar novos dataframe
def create_dataframe(data, tb_name):
    if data is not None:
        df = spark.read.json(sc.parallelize([data]))        
        create_log(df, tb_name)
  


# COMMAND ----------

####### This is not a good approach, this is just a demonstration #######
####### Please, use an Azure container or an AWS S3 bucket '/{year}/{month}/{day}.txt' #######

def create_log(df, tb_name):    
    
    if spark.catalog.tableExists(tb_name):
        df.write.format("delta").mode("append").saveAsTable(tb_name)
    else:        
        df.write.format("delta").mode("overwrite").saveAsTable(tb_name)


# COMMAND ----------

import logging
import json
import sys
import datetime

def error_handler(exception,debug_write_mode):

    # configuração para saída em JSON        
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.ERROR)
    

    # Configurar saido no notebook
    log_format = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    notebook_handler = logging.StreamHandler()
    notebook_handler.setLevel(logging.INFO)
    notebook_handler.setFormatter(log_format)
    logger.addHandler(notebook_handler)
    logger.propagate = False
    

    # Obtenha a pilha de chamadas de exceção
    exc_type, exc_value, exc_traceback = sys.exc_info()

    log_record = {
        'asctime': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        'levelname': logging.getLevelName(logging.ERROR),
        'function_name': exc_traceback.tb_frame.f_code.co_name,
        'line_number': exc_traceback.tb_lineno,
        'error_message': str(exception),
        'notebook_path': dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    }

    log_json = json.dumps(log_record, default=str, indent=2)

    if debug_write_mode == None: 
        logger.setLevel(logging.ERROR)

        if (logger.hasHandlers()):
            logger.handlers.clear()

        logger.addHandler(notebook_handler)        
        logger.error('Ocorreu um erro: %s ',log_json)
     

    elif debug_write_mode == True:
        logger.setLevel(logging.DEBUG)

        if (logger.hasHandlers()):
            logger.handlers.clear()

        logger.addHandler(notebook_handler)

        create_dataframe(log_json, tb_name)

        logger.info('Log armazenado em: %s ',tb_name)       
        


# COMMAND ----------

# MAGIC %md
# MAGIC #### Está disponivel a função ```error_handler()``` 
# MAGIC
# MAGIC #### Para registrar os erros em log ou apenas exibi-los parametro abaixo devem ser:
# MAGIC  ```python 
# MAGIC debug_write_mode = None # Apenas exibe os eroros
# MAGIC debug_write_mode = True # Logs são escritos em arquivo
# MAGIC ```
# MAGIC #### Defina o schema/db e nome da tabela onde os logs serão armazenados:
# MAGIC ```python 
# MAGIC tb_name = 'db.tb_name'
# MAGIC ```
# MAGIC
# MAGIC #### Para mais detalhes, consulte a documentação
# MAGIC - https://docs.python.org/3/howto/logging.html
