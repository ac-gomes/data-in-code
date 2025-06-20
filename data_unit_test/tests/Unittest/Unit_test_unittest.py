# Databricks notebook source
import unittest

class DataQualityTestSuite(unittest.TestCase):
    def __init__(self, methodName='runTest', target_table=None):
        super().__init__(methodName)
        self.target_table = target_table

    def test_column_unique(self):
        """ Test column unique values"""
        
        src= spark.sql(f"""
            SELECT 
                COUNT(*) 
            FROM 
                (SELECT product_id, COUNT(*) c 
            FROM 
                {self.target_table}
            GROUP BY 
                product_id HAVING c > 1)
        """).take(1)[0][0]
        
        expected = 0
        
        self.assertEqual(src, expected, f"Existem IDs duplicados")
        

    def test_column_not_null(self):
        """ Test if column not null values"""
        
        src = spark.sql(f"""
            SELECT 
                COUNT(*) as n 
            FROM 
                {self.target_table} 
            WHERE product_name IS NULL
        """).take(1)[0]["n"]
        
        expected = 0        
        
        self.assertEqual(src, expected, f" Existem product_name nulos")
        
    
    def test_schema_validations(self):
        """ Test table's schema"""
        
        src = spark.sql(f"""
            SELECT * FROM {self.target_table} 
        """).dtypes
    
        expected = [
            ('product_id', 'string'),
            ('product_name', 'string'),
            ('category', 'string'),
            ('price', 'string')
        ]

        self.assertListEqual(src, expected, f" Existem product_name nulos")
        
