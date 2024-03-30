# Databricks notebook source
# MAGIC %run ./Includes

# COMMAND ----------

# MAGIC %run ./Initialize_table

# COMMAND ----------

# MAGIC %md
# MAGIC _[en]_
# MAGIC ## When to use CDF?
# MAGIC - CDF works well in case there are small batches of changes to datasets
# MAGIC
# MAGIC - When the pipeline moves data inefficiently through the data lake layers in a customized way, it may lead to maintenance difficulties
# MAGIC
# MAGIC - When you see a need to improve the efficiency of your incremental loads based on UPDATE, DELETE and MERGE rather than reprocessing all old table data
# MAGIC
# MAGIC - CDF is not recommended for 'append-only' operations as it monitors UPDATE, DELETE and MERGE operations, please make a better assessment of your scenario
# MAGIC
# MAGIC > ***Note:***<br> 
# MAGIC > _**To avoid intermediate failures between table version updates, you may want to handle and control the ``` _commit_version ``` in your metadata table used by your ingestion framework. This sounds like the checkpoint file from streaming mode**_ 
# MAGIC
# MAGIC _[pt-br]_
# MAGIC ### Quando usar CDF?
# MAGIC - O CDF funciona bem em caso onde há pequenos lotes de alterações nos conjuntos de dados 
# MAGIC
# MAGIC - Quando o pipeline move os dandos de maneira ineficiente atravez das camadas do data lake de forma custumizada, e talvez possa acarretar dificuldades na manutenção
# MAGIC
# MAGIC - Quando você observar a necessidade de melhorar a eficiencia de suas cargas incrementais baseadas em  UPDATE, DELETE e MERGE ao invés de reprocessar todos os dados antigos da tabela
# MAGIC
# MAGIC - O CDF não é recomendado para operações puramente 'append-only' uma vez que ele monitora as operações UPDATE, DELETE e MERGE, favor faça uma avaliação melhor do seu cenário
# MAGIC
# MAGIC > ***Note:***<br> 
# MAGIC > _**Para evitar falhas intermediárias entre atualizações de versão da tabela, talvez você queira manipular e controlar a ``` _commit_version ``` na tabela de metadados usada pelo framework de ingestão. Isto parece o arquivo de checkpoint usado no modo streaming.**_ 
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC _[en]_
# MAGIC ### Change data storage
# MAGIC Databricks records change data for UPDATE, DELETE, and MERGE operations in the _change_data folder under the table directory. Some operations, such as insert-only operations and full partition deletes, do not generate data in the _change_data directory because Databricks can efficiently compute the change data feed directly from the transaction log.
# MAGIC
# MAGIC The files in the _change_data folder follow the retention policy of the table. Therefore, if you run the [VACUUM](https://docs.databricks.com/en/delta/vacuum.html) command, change data feed data is also deleted.
# MAGIC
# MAGIC _[pt-br]_
# MAGIC ### Alterar armazenamento de dados
# MAGIC O Databricks registra dados alterados para operações UPDATE, DELETE, e MERGE na pasta _change_data no diretório da tabela. Algumas operações, como operações somente de inserção e exclusões de partições completas, não geram dados no diretório _change_data porque o Databricks pode calcular com eficiência o feed de dados alterados diretamente do log de transações.
# MAGIC
# MAGIC Os arquivos da pasta _change_data seguem a política de retenção da tabela. Portanto, se você executar o comando [VACUUM](https://docs.databricks.com/pt/delta/vacuum.html), os dados do feed de dados alterados também serão excluídos.

# COMMAND ----------

# MAGIC %md
# MAGIC _[en]_
# MAGIC ## View the data
# MAGIC
# MAGIC _[pt-br]_
# MAGIC ## Visualizar os dados
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM silver.tbl_silver_order;

# COMMAND ----------

# MAGIC %md
# MAGIC _[en]_
# MAGIC ## Check table history
# MAGIC
# MAGIC _[pt-br]_
# MAGIC ## Verifique o histórico da tabela
# MAGIC

# COMMAND ----------

spark.sql("DESCRIBE HISTORY silver.tbl_silver_order").display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC _[en]_
# MAGIC ## Enable CDF on silver table
# MAGIC
# MAGIC _[pt-br]_
# MAGIC ## Habilitar o CDF na tabela silver
# MAGIC

# COMMAND ----------

display(
    spark.sql("ALTER TABLE silver.tbl_silver_order SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
)

# COMMAND ----------

# MAGIC %md
# MAGIC _[en]_
# MAGIC ## Check table history
# MAGIC
# MAGIC _[pt-br]_
# MAGIC ## Verifique o histórico da tabela
# MAGIC

# COMMAND ----------

spark.sql("DESCRIBE HISTORY silver.tbl_silver_order").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create gold table

# COMMAND ----------

# MAGIC %run ./Create_gold_table

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC _[en]_
# MAGIC ## Insert new rows into the silver table
# MAGIC
# MAGIC _[pt-br]_
# MAGIC ## Inserir novas linhas na tabela silver

# COMMAND ----------

# MAGIC %run ./Insert_new_rows

# COMMAND ----------

# MAGIC %md
# MAGIC _[en]_
# MAGIC ## Check table history
# MAGIC
# MAGIC _[pt-br]_
# MAGIC ## Verifique o histórico da tabela
# MAGIC

# COMMAND ----------

spark.sql("DESCRIBE HISTORY silver.tbl_silver_order").display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC _[en]_
# MAGIC ## Update rows into the table
# MAGIC
# MAGIC _[pt-br]_
# MAGIC ## Atualizar linhas na tabela
# MAGIC

# COMMAND ----------

# MAGIC %run ./Update_rows

# COMMAND ----------

# MAGIC %md
# MAGIC _[en]_
# MAGIC ## Explore the changes
# MAGIC
# MAGIC _[pt-br]_
# MAGIC ## Explore as mudanças
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM table_changes('silver.tbl_silver_order',2, 10)
# MAGIC ORDER BY _commit_timestamp

# COMMAND ----------

# MAGIC %md
# MAGIC ## Note
# MAGIC _[en]_
# MAGIC - **preimage** is the value before the update
# MAGIC - **postimage** is the value after the update.
# MAGIC
# MAGIC _[pt-br]_
# MAGIC - **preimage** é o valor antes da atualização
# MAGIC - **postimage** é o valor após a atualização.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC _[en]_
# MAGIC ## Fetch only changed data from silver table
# MAGIC
# MAGIC _[pt-br]_
# MAGIC ## Busca apenas dados alterados da tabela silver

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC WITH silver_changed_data(
# MAGIC   SELECT 
# MAGIC     *, 
# MAGIC     rank() over (partition by Country order by _commit_version desc) as rank
# MAGIC   FROM table_changes('silver.tbl_silver_order', 2, 5)
# MAGIC   WHERE _change_type != 'update_preimage'
# MAGIC )
# MAGIC
# MAGIC SELECT * FROM silver_changed_data
# MAGIC WHERE rank=1

# COMMAND ----------

# MAGIC %md
# MAGIC _[en]_
# MAGIC ## Send changes from silver to gold table
# MAGIC
# MAGIC _[pt-br]_
# MAGIC ## Enviar alterações da tabela silver para gold

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gold.tbl_gold_order;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW orders_latest_version AS (
# MAGIC     WITH silver_changed_data(
# MAGIC     SELECT 
# MAGIC       *, 
# MAGIC       rank() over (partition by Country order by _commit_version desc) as rank
# MAGIC     FROM table_changes('silver.tbl_silver_order', 2, 4)
# MAGIC     WHERE _change_type != 'update_preimage'
# MAGIC   )
# MAGIC
# MAGIC   SELECT * FROM silver_changed_data
# MAGIC   WHERE rank=1
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO gold.tbl_gold_order AS T USING orders_latest_version AS S 
# MAGIC   ON S.order_number = T.order_number AND S.product_id = T.product_id
# MAGIC     WHEN MATCHED AND S._change_type = 'update_postimage' THEN UPDATE SET status = S.status
# MAGIC     WHEN NOT MATCHED THEN INSERT (
# MAGIC       order_number,
# MAGIC       product_id,
# MAGIC       order_date,
# MAGIC       Total_Order_Value,
# MAGIC       status,
# MAGIC       Country
# MAGIC     ) VALUES (
# MAGIC         S.order_number,
# MAGIC         S.product_id,
# MAGIC         S.order_date,
# MAGIC         S.qty_ordered * unit_price,
# MAGIC         S.status,
# MAGIC         S.Country
# MAGIC     )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold.tbl_gold_order;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Always get latest commit version from silver table based on gold latest commit version

# COMMAND ----------

from pyspark.sql.functions import max

# Get max version from silver
silver_max_version = (spark.sql("DESCRIBE HISTORY silver.tbl_silver_order")
                            .agg(
                                max("version").alias("version")                                
                            )
                        ).take(1)[0][0]

print('silver_max_version >> ',silver_max_version)


# # Get max version from gold
gold_max_version = (spark.sql("DESCRIBE HISTORY gold.tbl_gold_order")
                            .agg(
                                max("version").alias("version")                                
                            )
                        ).take(1)[0][0]


print('gold_max_version >> ',gold_max_version) #You can adjust this parameter according to your need/scenario | Você pode ajustar o parâmetro de acordo com sua necessidade ou cenario


# COMMAND ----------

# MAGIC %md
# MAGIC ## Collect only the most recent data version

# COMMAND ----------

spark.sql("SELECT * FROM table_changes('silver.tbl_silver_order', {}, {})".format(gold_max_version, silver_max_version)).display()

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY gold.tbl_gold_order

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gold.tbl_gold_order;
