# Databricks notebook source
import pytest
from pyspark.sql import SparkSession

def _create_spark_session():
    return SparkSession.builder.getOrCreate()

@pytest.fixture(scope="session")
def spark():
    return _create_spark_session() 


def test_column_unique(spark, target_table):
    """ Test column unique values"""
    
    src= spark.sql(f"""
        SELECT 
            COUNT(*) 
        FROM 
            (SELECT product_id, COUNT(*) c 
        FROM 
            {target_table} 
        GROUP BY 
            product_id HAVING c > 1)
    """).take(1)[0][0]
    
    expected = 0
    
    assert src == expected, f"❌ Existem {src} IDs duplicados"


def test_column_not_null(spark, target_table):
    """ Test if column not null"""
    
    src = spark.sql(f"""
        SELECT 
            COUNT(*) as n 
        FROM 
            {target_table} 
        WHERE 
            product_name IS NULL
    """).take(1)[0]["n"]
    
    expected = 0
    
    assert src == expected, f"❌ Existem {src} product_name nulos"
    

def test_schema_validations(spark, target_table):
    """ Test table's schema"""
    
    src = spark.table(target_table).dtypes

    expected = [
        ('product_id', 'string'),
        ('product_name', 'string'),
        ('category', 'string'),
        ('price', 'double')
    ]

    assert src == expected, f"❌ Schema {src} é diferente do esperado"
