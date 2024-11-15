import unittest
from pyspark.sql import SparkSession, Row

from src.aggregations.agg_functions import get_number_of_artists_per_country


class TestGetArtistsPerCountry(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Initialize a SparkSession
        cls.spark = (
            SparkSession.builder.appName("TestGetArtistsPerCountry")
            .master("local[2]")
            .getOrCreate()
        )

    @classmethod
    def tearDownClass(cls):
        # Stop the SparkSession
        cls.spark.stop()

    def test_get_artists_per_country(self):
        # Create a sample DataFrame
        data = [
            Row(Country="USA", **{"Constituent ID": 1}),
            Row(Country="USA", **{"Constituent ID": 2}),
            Row(Country="USA", **{"Constituent ID": 1}),  # Duplicate artist
            Row(Country="France", **{"Constituent ID": 3}),
            Row(Country="France", **{"Constituent ID": 4}),
            Row(Country="France", **{"Constituent ID": 3}),  # Duplicate artist
            Row(Country="Germany", **{"Constituent ID": 5}),
            Row(Country="Germany", **{"Constituent ID": 5}),  # Duplicate artist
        ]
        df = self.spark.createDataFrame(data)

        # Call the function to test
        result_df = get_number_of_artists_per_country(df)

        # Expected DataFrame
        expected_data = [
            Row(Country="USA", **{"Number Of Artists": 2}),
            Row(Country="France", **{"Number Of Artists": 2}),
            Row(Country="Germany", **{"Number Of Artists": 1}),
        ]
        expected_df = self.spark.createDataFrame(expected_data)

        # Convert DataFrames to Dictionaries for easy comparison
        result_dict = {
            row["Country"]: row["Number Of Artists"] for row in result_df.collect()
        }
        expected_dict = {
            row["Country"]: row["Number Of Artists"] for row in expected_df.collect()
        }

        # Assert that the result matches the expected output
        self.assertEqual(result_dict, expected_dict)

    def test_get_artists_per_country_with_nulls(self):
        # Create a sample DataFrame with null Constituent IDs
        data = [
            Row(Country="USA", **{"Constituent ID": 1}),
            Row(Country="USA", **{"Constituent ID": 2}),
            Row(Country="USA", **{"Constituent ID": None}),  # Null Constituent ID
            Row(Country="USA", **{"Constituent ID": 2}),  # Duplicate artist
            Row(Country="France", **{"Constituent ID": 3}),
            Row(Country="France", **{"Constituent ID": None}),  # Null Constituent ID
            Row(Country="France", **{"Constituent ID": 3}),  # Duplicate artist
            Row(Country="Germany", **{"Constituent ID": None}),  # Null Constituent ID
            Row(Country="Germany", **{"Constituent ID": 5}),
        ]
        df = self.spark.createDataFrame(data)

        # Call the function to test
        result_df = get_number_of_artists_per_country(df)

        # Expected DataFrame
        expected_data = [
            Row(Country="USA", **{"Number Of Artists": 2}),
            Row(Country="France", **{"Number Of Artists": 1}),
            Row(Country="Germany", **{"Number Of Artists": 1}),
        ]
        expected_df = self.spark.createDataFrame(expected_data)

        # Convert DataFrames to Dictionaries for easy comparison
        result_dict = {
            row["Country"]: row["Number Of Artists"] for row in result_df.collect()
        }
        expected_dict = {
            row["Country"]: row["Number Of Artists"] for row in expected_df.collect()
        }

        # Assert that the result matches the expected output
        self.assertEqual(result_dict, expected_dict)
