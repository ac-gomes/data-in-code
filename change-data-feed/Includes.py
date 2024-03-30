# Databricks notebook source
# MAGIC %md
# MAGIC ## Create dummy layers

# COMMAND ----------

def create_dummy_layers(layers_list: list):

    try:
        if len(layers_list) > 0:
            for layer in layers_list:
                path = f"/mnt/cdf/{layer}"
                dbutils.fs.mkdirs(path)

            print(dbutils.fs.ls('/mnt/cdf/'))
    except Exception as Err:
        print(f'Something went wrong: {Err}')
    

# COMMAND ----------

layers_list = ['silver', 'gold']

create_dummy_layers(layers_list)

# COMMAND ----------


def create_database(db_name, db_path) -> bool:
    isCreated_db = False
    if len(db_name) > 0 and len(db_path) > 0:
        spark.sql(f"""
        CREATE DATABASE IF NOT EXISTS {db_name} COMMENT '{db_name} zone database' LOCATION '{db_path}'
        """)
        isCreated_db = True
    else:
        print('Inser the name and path to database')
        return  
        

def show_database(db_name: str):
    if len(db_name) > 0:
        spark.sql(f"""
        DESCRIBE DATABASE EXTENDED {db_name}
        """).display()
        return   

# COMMAND ----------

if len(layers_list) > 0:
    for layer in layers_list:
      db_path = f'/mnt/cdf/{layer}/'
      db_name = layer
      
      create_database(db_name, db_path)

# COMMAND ----------

show_database('silver')

# COMMAND ----------

show_database('gold')

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP SCHEMA IF EXISTS silver CASCADE;
# MAGIC -- DROP SCHEMA IF EXISTS gold CASCADE;
