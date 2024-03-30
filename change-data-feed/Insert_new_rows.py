# Databricks notebook source
display(
    spark.sql(f"""
        INSERT INTO silver.tbl_silver_order(
            order_number,
            order_date,
            qty_ordered,
            unit_price,
            status,
            product_id,
            product_line_id,
            country
        ) VALUES 
            (10388,"2022-07-11",247,1049.38,"Disputed","S18_1367",1004,"Brazil"),
            (10237,"2022-02-10",29,959.96,"Resolved","S12_1108",1221,"Spain"),
            (10251,"2022-02-15",54,960.41,"Resolved","S12_1666",1001,"Sweden");              
    """)
)
