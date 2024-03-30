# Databricks notebook source
display(
    spark.sql(f"""
        UPDATE silver.tbl_silver_order
        SET status = 'In Process'

        WHERE country = "Finland"
            AND order_number = 10201             
    """)
)
