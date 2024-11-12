import unittest
from pyspark.sql import SparkSession, Row
import pyspark.sql.functions as F

# The function under test
def unique_list_of_constituent_ids_per_country(df):
    unique_constituent_ids_df = df.groupBy('Country').agg(
        F.collect_set('Constituent ID').alias('unique_constituent_ids')
    )
    return unique_constituent_ids_df

class TestUniqueListOfConstituentIDsPerCountry(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Initialize a SparkSession
        cls.spark = SparkSession.builder \
            .appName("TestUniqueListOfConstituentIDsPerCountry") \
            .master("local[2]") \
            .getOrCreate()

    @classmethod
    def tearDownClass(cls):
        # Stop the SparkSession
        cls.spark.stop()

    def test_unique_list_of_constituent_ids_per_country(self):
        # Create sample data
        data = [
            Row(Country='USA', **{'Constituent ID': 1}),
            Row(Country='USA', **{'Constituent ID': 2}),
            Row(Country='USA', **{'Constituent ID': 1}),  # Duplicate ID
            Row(Country='France', **{'Constituent ID': 3}),
            Row(Country='France', **{'Constituent ID': 4}),
            Row(Country='France', **{'Constituent ID': 3}),  # Duplicate ID
            Row(Country='Germany', **{'Constituent ID': 5}),
            Row(Country='Germany', **{'Constituent ID': 5}),  # Duplicate ID
            Row(Country='Germany', **{'Constituent ID': 6}),
        ]
        df = self.spark.createDataFrame(data)

        # Expected results
        expected = {
            'USA': {1, 2},
            'France': {3, 4},
            'Germany': {5, 6},
        }

        # Run the function
        result_df = unique_list_of_constituent_ids_per_country(df)

        # Collect the results into a dictionary
        result = {}
        for row in result_df.collect():
            country = row['Country']
            ids_set = set(row['unique_constituent_ids'])
            result[country] = ids_set

        # Compare the results
        for country in expected.keys():
            self.assertEqual(
                result[country],
                expected[country],
                msg=f"Unique Constituent IDs mismatch for {country}"
            )

if __name__ == '__main__':
    unittest.main()