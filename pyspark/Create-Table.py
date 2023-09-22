# Databricks notebook source
# MAGIC %md
# MAGIC ### __[en]__
# MAGIC ### Basic Pyspark - How to create DataFrame from hardcode
# MAGIC
# MAGIC ### __[pt-br]__
# MAGIC ### Pyspark básico - Como criar DataFrame apartir de valores fixos dentro do código fonte

# COMMAND ----------

# Importing types and struct Types
from pyspark.sql.types import StringType, StructField, StructType
from pyspark.sql.functions import current_timestamp, unix_timestamp


# COMMAND ----------

columns = [
    StructField("order_number", StringType(), True),
    StructField("order_date", StringType(), True),
    StructField("qty_ordered", StringType(), True),
    StructField("unit_price", StringType(), True),
    StructField("status", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("product_line_id", StringType(), True),
    StructField("country", StringType(), True)
]

# COMMAND ----------

data_values = [
    (10168,"2022-01-23",5,98.115,"Disputed","S10_1949",1002,"France"),
    (10180,"2022-01-22",1,951.87,"In Process","S10_2016",1002,"Norway"),
    (10188,"2022-01-21",65,95.202,"Cancelled","S10_4698",1002,"Australia"),
    (10201,"2022-01-26",8,951.17,"On Hold","S10_4757",1221,"Finland"),
    (10211,"2022-01-31",34,95.195,"Resolved","S10_4962",1221,"Austria"),
    (10223,"2022-02-05",7,956.84,"In Process","S12_1099",1221,"UK"),
    (10237,"2022-02-10",29,959.96,"Resolved","S12_1108",1221,"Spain"),
    (10251,"2022-02-15",54,960.41,"Resolved","S12_1666",1001,"Sweden"),
    (10263,"2022-02-16",5,961.98,"On Hold","S12_2823",1001,"Singapore"),
    (10275,"2022-02-17",12,96.142,"Shipped","S12_3148",1001,"Canada"),
    (10285,"2022-02-24",10,960.96,"In Process","S12_3380",1001,"Japan"),
    (10299,"2022-02-26",7,963.52,"In Process","S12_3891",1001,"Italy"),
    (10309,"2022-03-03",981,96.430,"Shipped","S12_3990",1001,"Denmark"),
    (10318,"2022-03-04",364,1043.22,"On Hold","S12_4473",1004,"Belgium"),
    (10329,"2022-03-08",25,1046.67,"Shipped","S12_4675",1004,"Philippines"),
    (10341,"2022-05-17",4,1047.34,"Shipped","S18_1097",1004,"Germany"),
    (10361,"2022-05-23",1,1047.99,"On Hold","S18_1129",1004,"Switzerland"),
    (10375,"2022-06-26",58,1048.47,"Disputed","S18_1342",1004,"Ireland"),
    (10388,"2022-07-11",247,1049.38,"Disputed","S18_1367",1004,"Brazil"),
]

# COMMAND ----------

#Create Sales DataFrame
schema = StructType(columns)
orders_df = spark.createDataFrame(data_values, schema)

# COMMAND ----------

# Adicionar uma nova coluna unix timestamp
orders_df = orders_df.withColumn("created_date",current_timestamp())
orders_df = orders_df.select(
    "order_number",
    "order_date",
    "qty_ordered",
    "unit_price",
    "status",
    "product_id",
    "product_line_id",
    "country",
    unix_timestamp("created_date").alias("created_date")
    )
orders_df.show(5)
