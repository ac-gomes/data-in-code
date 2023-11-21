# Databricks notebook source
# MAGIC %md
# MAGIC ## >> How to "virtualize" a table from on-prem environment in Databricks?
# MAGIC ## >> Como "virtualizar" uma tabela do ambiente local no Databricks?
# MAGIC - Aqui será usado um banco de dados Azure SQL Server, mas pode muito bem ser um tabela local da sua rede on-prem
# MAGIC ## >> Como posso obeter dados do ambiente om-prem no Databricks
# MAGIC - Aqui será usado um banco de dados Azure SQL Server, mas pode muito bem ser um tabela local da sua rede on-prem 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Connect your Azure Databricks workspace to your on-premises network
# MAGIC ## Conectar o workspace do Azure Databricks à rede local
# MAGIC - https://learn.microsoft.com/en-us/azure/databricks/administration-guide/cloud-configurations/azure/on-prem-network
# MAGIC ![arquitetura](https://learn.microsoft.com/en-us/azure/databricks/_static/images/account-settings/azure-networking-transit-vnet.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load connection props - Please, use Key Vault and Secret scopes for that.

# COMMAND ----------

# It is used with jdbc driver connection only  -------> use key vault and secret scope
# Por motivos obvios sempre use key vault e secret scope <=
jdbcHostname = "hostname"
jdbcPort = "port"
jdbcDatabase = "database"
jdbcTable = "schema.table"
user = "user"
password = "password"


jdbcUrl = "jdbc:sqlserver://{0}:{1};database={2}".format(jdbcHostname,jdbcPort,jdbcDatabase)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create a schema to link the remote table in Databricks
# MAGIC - It seems that in Databricks community edition the variables Doesn’t work well with Spark SQL
# MAGIC - Parece que no Databricks community edition as variáveis não funcionam bem com Spark SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS virtualized_tables

# COMMAND ----------

# MAGIC %sql
# MAGIC --- Link remote table to our schema, ------> use key vault and secret scope
# MAGIC --- Por motivos obvios sempre use key vault and secret scope
# MAGIC --- LINK TO Product
# MAGIC DROP TABLE IF EXISTS virtualized_tables.Product;
# MAGIC CREATE TABLE virtualized_tables.Product
# MAGIC USING sqlserver
# MAGIC OPTIONS (
# MAGIC   dbtable "Production.Product",
# MAGIC   host "acg-sql-001.database.windows.net",
# MAGIC   database "DEV1",
# MAGIC   port '1433',
# MAGIC   user "acgadmin",
# MAGIC   password "H!vPrntidyCfRsGNdj68"
# MAGIC );

# COMMAND ----------

# Connection using variables and other options
# Conexão usando variaveis

spark.sql(f"""
          CREATE TABLE IF NOT EXISTS virtualized_tables.Product
          USING org.apache.spark.sql.jdbc
          OPTIONS (
            url '{jdbcUrl}',
            dbtable '{table}',
            driver '{driver}',
            user '{user}',
            password 'password',
            tablelock true          
          )"""
        )
#---------------- more options
# CREATE TEMPORARY VIEW employees_table_vw
# USING JDBC
# OPTIONS (
#   url "<jdbc-url>",
#   dbtable "<table-name>",
#   user '<username>',
#   password '<password>',
#   partitionColumn "<partition-key>",
#   lowerBound "<min-value>",
#   fetchSize 10000,
#   upperBound "<max-value>",
#   numPartitions 8
# )        

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from virtualized_tables.Product;

# COMMAND ----------

# MAGIC %sql
# MAGIC --- Link remote table to our schema, ------> use key vault and secret scope
# MAGIC --- Por motivos obvios sempre use key vault and secret scope
# MAGIC --- LINK TO ProductCategory
# MAGIC DROP TABLE IF EXISTS virtualized_tables.ProductCategory;
# MAGIC CREATE TABLE virtualized_tables.ProductCategory
# MAGIC USING sqlserver
# MAGIC OPTIONS (
# MAGIC   dbtable '<schema-name.table-name>',
# MAGIC   host '<host-name>',
# MAGIC   database '<database-name>',
# MAGIC   port '1433',
# MAGIC   user '<username>',
# MAGIC   password '<password>'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- here the query executed in on-prem db and retrieve the result
# MAGIC -- aqui a consulta está sendo executada no db local e tras apenas o resultado
# MAGIC select * from virtualized_tables.ProductCategory;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read data

# COMMAND ----------

# Read Table
# Documentation https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html
# AQUI A TABELA INTEIRA SERÁ IMPORTADA PARA O CLUSTER QUANDO FOR EFETUAR UM TRANFORMAÇÃO
# HERE THE ENTIRE TABLE WILL BE IMPORTED TO THE CLUSTER WHEN A TRANSFORMATION IS PERFORMED
remote_table = (spark.read
    .format("jdbc")
    .option("url", jdbcUrl)
    .option("dbtable", "Production.Product")
    .option("user", username)
    .option("password", password)
    .option("fetchSize", "10000")
    .option("numPartitions", "2")    #parallelism using two jdbc connections to read data | paralelismo usando duas conexões jdbc para ler dados
    .load())

# COMMAND ----------

# neste contexto os dados serão importados para a memoria do cluster
# in this context the data will be imported into the cluster memory
remote_table.select("*").display()

# COMMAND ----------

# Read with specific query
# Documentation https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html
# aqui busca apenas um subset dos dados por meio de um query previamente escrita.
# here fetch only a subset of data through a previously written query.
query_stm = '(SELECT TOP 100 * FROM Production.Product) AS ProductSubset'
remote_product_subset = (spark.read
    .format("jdbc")
    .option("url", jdbcUrl)
    .option("dbtable", query_stm)
    .option("user", username)
    .option("password", password)
    .option("fetchSize", "1000")
    .option("numPartitions", "2")
    .load())

# COMMAND ----------

remote_product_subset.select("*").display()
