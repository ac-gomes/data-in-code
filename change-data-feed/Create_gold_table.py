# Databricks notebook source
from pyspark.sql.types import StringType, LongType, DateType, DoubleType, IntegerType
import pyspark.sql.functions as F

orders_silver_tbl_df = spark.sql("SELECT * FROM silver.tbl_silver_order")

orders_gold_df = orders_silver_tbl_df.select(
    "order_number",
     "product_id",
    F.to_date(F.col("order_date"),"yyyy-MM-dd").cast(DateType()).alias("order_date"),
    (F.col("qty_ordered") * F.col("unit_price")).alias("Total_Order_Value"),    
    "status",      
    "Country"
    )

orders_gold_df.display()

# COMMAND ----------

(orders_gold_df
    .write
    .format('delta')
    .mode('overwrite')
    .option('mergeSchema', 'true')
    .option('overwriteSchema', 'true')
    .option('path', '/mnt/cdf/gold/tbl_silver_order')
    .saveAsTable('gold.tbl_gold_order')
)
