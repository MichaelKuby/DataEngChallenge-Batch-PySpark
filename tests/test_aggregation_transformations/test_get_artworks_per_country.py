import unittest
from pyspark.sql import SparkSession
from pyspark.sql import Row

from src.aggregations.agg_functions import get_artworks_per_country


class TestGetArtworksPerCountry(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Initialize a SparkSession
        cls.spark = (
            SparkSession.builder.appName("TestGetArtworksPerCountry")
            .master("local[2]")
            .getOrCreate()
        )

    @classmethod
    def tearDownClass(cls):
        # Stop the SparkSession
        cls.spark.stop()

    def test_get_artworks_per_country(self):
        # Create a sample DataFrame
        data = [
            Row(Country="USA", Artwork="Art1"),
            Row(Country="USA", Artwork="Art2"),
            Row(Country="France", Artwork="Art3"),
            Row(Country="USA", Artwork="Art4"),
            Row(Country="France", Artwork="Art5"),
            Row(Country="Germany", Artwork="Art6"),
            Row(Country="Germany", Artwork="Art7"),
            Row(Country="Germany", Artwork="Art8"),
        ]
        df = self.spark.createDataFrame(data)

        # Call the function to test
        result_df = get_artworks_per_country(df)

        # Expected DataFrame
        expected_data = [
            Row(Country="USA", count=3),
            Row(Country="France", count=2),
            Row(Country="Germany", count=3),
        ]
        expected_df = self.spark.createDataFrame(expected_data)

        # Convert DataFrames to Dictionaries for easy comparison
        result_dict = {row["Country"]: row["count"] for row in result_df.collect()}
        expected_dict = {row["Country"]: row["count"] for row in expected_df.collect()}

        # Assert that the result matches the expected output
        self.assertEqual(result_dict, expected_dict)
