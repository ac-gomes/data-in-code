# Databricks notebook source
# MAGIC %md
# MAGIC ### Simple Transformation in Pyspark
# MAGIC - CAST for Data Type Conversions
# MAGIC - Working with Dates and Times
# MAGIC
# MAGIC ### Transformações simples em Pyspark
# MAGIC - Conversão de tipos de dados
# MAGIC - Trabalhando com data e hora
# MAGIC

# COMMAND ----------

# MAGIC %run ./Create-Table

# COMMAND ----------

# Importing types and funtions
from pyspark.sql.types import StringType, StructType, LongType, DateType, TimestampType, DoubleType, IntegerType
from pyspark.sql.functions import current_timestamp, unix_timestamp, col, to_date, from_unixtime, to_timestamp

# COMMAND ----------

#Vamos observar o schema do dataframe
orders_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Como será tipo de dados das colunas
# MAGIC
# MAGIC ```sql
# MAGIC order_number:long
# MAGIC order_date:date
# MAGIC qty_ordered:integer
# MAGIC unit_price:double
# MAGIC status:string
# MAGIC product_id:string
# MAGIC product_line_id:long
# MAGIC country:string
# MAGIC created_date:timestamp
# MAGIC
# MAGIC ```

# COMMAND ----------

# Vamos converte alguns tipos de dados das colunas

orders_df = orders_df.select(
        col("order_number").cast(LongType()).alias("order_number"),
        to_date(col("order_date"),"yyyy-MM-dd").cast(DateType()).alias("order_date"),
        col("qty_ordered").cast(IntegerType()).alias("qty_ordered"),
        col("unit_price").cast(DoubleType()).alias("unit_price"),
        "status",
        "product_id",
        col("product_line_id").cast(LongType()).alias("product_line_id"),
        "country",
        to_timestamp(from_unixtime(col("created_date")),"yyyy-MM-dd HH:mm:ss").alias("created_date")
    )

orders_df.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Formatos suportados são os mesmos da linguagem Java
# MAGIC - veja os xemplos aqui https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html
# MAGIC
# MAGIC Formato-Simbolo | Significado | Exibição | Exemplo
# MAGIC :--------------:|:-----------:|:--------:|:------:
# MAGIC y| ano| ano| 2023
# MAGIC d| dia do mês | numero| 189
# MAGIC M/L|mês do ano| numero/texto "L"/"MM"/"MMM"/"MMMM" | 7; 07; Jul; July; J
# MAGIC E|dia da semana|texto|Tue; Tuesday; T
# MAGIC F| dia da semana mês inicio segunda| numero| 3
# MAGIC Q/q| trimestre | numero/texto "q"/"qq"/"QQ"/"QQQ"...| 3; 03; Q3; 3rd quarter

# COMMAND ----------

# MAGIC %md
# MAGIC ### Vamos importar algumas funções extras

# COMMAND ----------

from pyspark.sql.functions import date_format, dayofweek, to_date

# COMMAND ----------

# MAGIC %md
# MAGIC # Extrair mês, ano, dia, trimestre e dia da semana de um coluna de datas

# COMMAND ----------

# Extrair ano, mes, dia e dia da semana

orders_df.select(
    "order_date",
    date_format(col("order_date"),"yyy").alias("ano"),
    date_format(col("order_date"),"LL").alias("mes_num"),
    date_format(col("order_date"),"MMMM").alias("mes_desc"),
    date_format(col("order_date"),"d").alias("dia"),
    date_format(col("order_date"),"E").alias("dia_da_semana_abrev"),
    date_format(col("order_date"),"EEEE").alias("dia_da_semana_desc_longo"),
    date_format(col("order_date"),"F").alias("dia_da_semana_num"),
    dayofweek(col("order_date")).alias("dia_da_semana_num_1Domingo")
).show(5)


# COMMAND ----------

# Extrair: Trimestre|Quarter

orders_df.select(
    "order_date",
    date_format(col("order_date"),"q").alias("trimestre_num"),
    date_format(col("order_date"),"qq").alias("trimestre_num_zero_esquerda"),
    date_format(col("order_date"),"QQQ").alias("trimestre_num_texto_Abrev"),
    date_format(col("order_date"),"QQQQ").alias("trimestre_texto"),
)\
.orderBy(col("trimestre_num").desc())\
.show(5)



# COMMAND ----------

# Extrair: data e Hora  do tipo timestamp

orders_df.select(
    "created_date",
    to_date(col("created_date"),"yyyy-MM-dd").alias("Data"),
    date_format(col("created_date"),"HH:mm:ss").alias("hora"),
).show(5)
