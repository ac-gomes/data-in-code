# Databricks notebook source
import unittest

class DataQualityTestSuite(unittest.TestCase):
    def __init__(self, methodName='runTest', target_table=None):
        super().__init__(methodName)
        self.target_table = target_table

    def test_returned_orders(self):
        """Test the percentage of returned orders"""
        src = spark.sql(f"""
            SELECT 
                ROUND(
                    COUNT(DISTINCT CASE WHEN is_returned = 'true' THEN order_id END) / 
                    COUNT(DISTINCT order_id),
                2) AS returned_percent
            FROM 
                {self.target_table}
        """).take(1)[0][0]

        expected = 0.02
        self.assertEqual(float(src), expected, f" Percentual de devolução é diferente do esperado")

