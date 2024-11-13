import unittest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from pyspark.sql import Row
from src.pre_processing.process_country_column_functions import process_country_column

class TestProcessCountryColumn(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .master("local[1]") \
            .appName("TestProcessCountryColumn") \
            .getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_process_country_column(self):
        data = [
            ('USA|Canada or Mexico',),
            ('France|Germany or Italy',),
            ('Brazil',),
            ('India or China',),
            ('Spain|Portugal',),
            ('Australia',),
            ('',),      # Empty string case
            (None,),    # Null value case
        ]
        # Define the schema for the input DataFrame
        schema = StructType([StructField('Country', StringType(), True)])
        df_input = self.spark.createDataFrame(data, schema)

        # Expected output data after processing
        expected_data = [
            (['USA', 'Canada', 'Mexico'],),
            (['France', 'Germany', 'Italy'],),
            (['Brazil'],),
            (['India', 'China'],),
            (['Spain', 'Portugal'],),
            (['Australia'],),
            ([],),       # Empty array for empty string
            (None,),     # Null remains Null
        ]
        # Define the schema for the expected DataFrame
        expected_schema = StructType([StructField('Country', ArrayType(StringType(), True), True)])
        df_expected = self.spark.createDataFrame(expected_data, expected_schema)

        # Apply the function to the input DataFrame
        df_result = process_country_column(df_input)

        # Collect the results from both DataFrames
        result_data = df_result.collect()
        expected_data = df_expected.collect()

        self.assertEqual(result_data, expected_data)

if __name__ == '__main__':
    unittest.main()