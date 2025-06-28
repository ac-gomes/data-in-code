# Databricks notebook source
import pytest
from pyspark.sql import SparkSession

def _create_spark_session():
    return SparkSession.builder.getOrCreate()

@pytest.fixture(scope="session")
def spark():
    return _create_spark_session()


def test_returned_orders(spark, target_table):
    """Test the percentage of returned orders"""

    src = spark.sql(f"""
        SELECT 
            ROUND(
                COUNT(DISTINCT CASE WHEN is_returned = 'true' THEN order_id END) / 
                COUNT(DISTINCT order_id),
            2) AS returned_percent
            
        FROM 
            {target_table}
    """).take(1)[0][0]

    expected = 0.02

    assert src == expected, f"❌ percentual de devolução {src} é diferente do esperado"   
