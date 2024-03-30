# Databricks notebook source
# MAGIC %md
# MAGIC ## Generate Data

# COMMAND ----------

from pyspark.sql.types import StringType, StructField, StructType, LongType, DateType, TimestampType, DoubleType, IntegerType
from pyspark.sql.functions import current_timestamp, unix_timestamp

columns = [
    StructField("order_number", LongType(), True),
    StructField("order_date", StringType(), True),
    StructField("qty_ordered", LongType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("status", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("product_line_id", LongType(), True),
    StructField("country", StringType(), True)
]

data_values = [
    (10168,"2022-01-23",5,98.115,"Disputed","S10_1949",1002,"France"),    
    (10201,"2022-01-26",8,951.17,"On Hold","S10_4757",1221,"Finland"),
    (10223,"2022-02-05",7,956.84,"In Process","S12_1099",1221,"UK")  
]

# COMMAND ----------

schema = StructType(columns)
orders_df = spark.createDataFrame(data_values, schema)

# COMMAND ----------

## Write Silver Table on Database
(orders_df
    .write
    .format('delta')
    .mode('overwrite')
    .option('mergeSchema', 'true')
    .option('overwriteSchema', 'true')
    .option('path', '/mnt/cdf/silver/tbl_silver_order')
    .saveAsTable('silver.tbl_silver_order')
)
