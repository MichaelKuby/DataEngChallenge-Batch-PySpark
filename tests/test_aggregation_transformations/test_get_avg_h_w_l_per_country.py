import unittest
from pyspark.sql import SparkSession, Row
from src.aggregations.agg_functions import get_avg_h_w_l_per_country


class TestGetAvgHWLPerCountry(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Initialize a SparkSession
        cls.spark = SparkSession.builder \
            .appName("TestGetAvgHWLPerCountry") \
            .master("local[2]") \
            .getOrCreate()

    @classmethod
    def tearDownClass(cls):
        # Stop the SparkSession
        cls.spark.stop()

    def test_get_avg_h_w_l_per_country(self):
        # Create sample data
        data = [
            Row(Country='USA', Height=10.0, Width=5.0, Length=2.0),
            Row(Country='USA', Height=20.0, Width=15.0, Length=12.0),
            Row(Country='USA', Height=30.0, Width=25.0, Length=22.0),
            Row(Country='France', Height=40.0, Width=35.0, Length=32.0),
            Row(Country='France', Height=50.0, Width=45.0, Length=42.0),
            Row(Country='Germany', Height=60.0, Width=55.0, Length=52.0),
        ]
        df = self.spark.createDataFrame(data)

        # Expected results
        expected = {
            'USA': {'avg_height': 20.0, 'avg_width': 15.0, 'avg_length': 12.0},
            'France': {'avg_height': 45.0, 'avg_width': 40.0, 'avg_length': 37.0},
            'Germany': {'avg_height': 60.0, 'avg_width': 55.0, 'avg_length': 52.0},
        }

        # Run the function
        result_df = get_avg_h_w_l_per_country(df)

        # Collect the results into a dictionary
        result = {}
        for row in result_df.collect():
            result[row['Country']] = {
                'avg_height': row['avg_height'],
                'avg_width': row['avg_width'],
                'avg_length': row['avg_length']
            }

        # Compare the results using assertAlmostEqual for floating-point numbers
        for country in expected.keys():
            self.assertAlmostEqual(
                result[country]['avg_height'],
                expected[country]['avg_height'],
                places=5,
                msg=f"Avg height mismatch for {country}"
            )
            self.assertAlmostEqual(
                result[country]['avg_width'],
                expected[country]['avg_width'],
                places=5,
                msg=f"Avg width mismatch for {country}"
            )
            self.assertAlmostEqual(
                result[country]['avg_length'],
                expected[country]['avg_length'],
                places=5,
                msg=f"Avg length mismatch for {country}"
            )


if __name__ == '__main__':
    unittest.main()
