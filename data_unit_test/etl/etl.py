# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType

# COMMAND ----------

# MAGIC %md
# MAGIC ## Orders Events

# COMMAND ----------

import json

with open("../data/order_events.jsonl", encoding="utf-8") as f:
    orders_lines = [json.loads(line) for line in f]


# COMMAND ----------

# orders_path = "../data/order_events.jsonl"

order_schema = StructType([
    StructField("event_id", StringType(),  True),
    StructField("user_id", StringType(),  True),
    StructField("order_id", StringType(),  True),  
    StructField("product_id", StringType(),  True),
    StructField("event_type", StringType(),  True),  
    StructField("price", StringType(),  True),
    StructField("region", StringType(),  True),
    StructField("event_timestamp", StringType(),  True),
    StructField("is_delayed", StringType(),  True),
    StructField("is_returned", StringType(),  True),
])


# order_df = (
#     spark.read    
#     .schema(order_schema)             
#     .json(orders_path)
# )

order_df = spark.createDataFrame(orders_lines, schema=order_schema)

# COMMAND ----------

order_df.show(5)

# COMMAND ----------

(
    order_df
    .writeTo("dev.bronze.tbl_bronze_order_events")
    .createOrReplace()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Products Catalog

# COMMAND ----------

# Load product catalog from CSV
import csv

with open("../data/products.csv", newline="", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    product_catalog = list(reader)

# COMMAND ----------

# product_catalog_path = '../Data/products.csv'

product_catalog_schema = StructType([
    StructField("product_id", StringType(),  True),
    StructField("product_name", StringType(),  True),
    StructField("category", StringType(),  True),
    StructField("price", StringType(),  True),
])


# product_catalog_df = (
#     spark.read
#     .option("header", True)    
#     .option("delimiter", ",")   
#     .schema(product_catalog_schema)             
#     .csv(product_catalog_path)
# )

product_catalog_df = spark.createDataFrame(product_catalog, schema=product_catalog_schema)

# COMMAND ----------

product_catalog_df.show(5)

# COMMAND ----------

(
    product_catalog_df
    .writeTo("dev.bronze.tbl_bronze_product_catalog")
    .createOrReplace()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Camada Silver

# COMMAND ----------

spark.sql("""
    CREATE OR REPLACE TABLE dev.silver.tbl_silver_product_catalog
    AS
    SELECT
        CAST(product_id AS STRING)          AS product_id,
        CAST(product_name AS STRING)        AS product_name,
        CAST(category AS STRING)            AS category,
        CAST(price AS DOUBLE)               AS price
        
    FROM dev.bronze.tbl_bronze_product_catalog

""")
