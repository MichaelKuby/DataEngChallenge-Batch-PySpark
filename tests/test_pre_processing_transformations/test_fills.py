import unittest
from src.utils.spark_session import get_spark_session
from src.pre_processing.fill_functions import (
    back_fill,
    forward_fill,
    b_fill_and_f_fill_constituent_id_column,
)


class TestBackwardFill(unittest.TestCase):
    def setUp(self):
        self.spark = get_spark_session()
        self.columns = ["Title", "Constituent ID", "idx"]

        self.input_data = [
            ("Book1", 123, 1),
            ("Book1", None, 2),
            ("Book1", None, 3),
            ("Book2", None, 1),
            ("Book2", 789, 2),
            ("Book2", None, 3),
        ]
        self.df = self.spark.createDataFrame(self.input_data, self.columns)

        self.expected_backward_fill_data = [
            ("Book1", 123, 1),
            ("Book1", 123, 2),
            ("Book1", 123, 3),
            ("Book2", None, 1),
            ("Book2", 789, 2),
            ("Book2", 789, 3),
        ]
        self.df_bw_fill_expected = self.spark.createDataFrame(
            self.expected_backward_fill_data, self.columns
        )

        self.expected_forward_fill_data = [
            ("Book1", 123, 1),
            ("Book1", None, 2),
            ("Book1", None, 3),
            ("Book2", 789, 1),
            ("Book2", 789, 2),
            ("Book2", None, 3),
        ]
        self.df_fw_fill_expected = self.spark.createDataFrame(
            self.expected_forward_fill_data, self.columns
        )

        self.df_bw_and_fw_fill_data = [
            ("Book1", 123, 1),
            ("Book1", 123, 2),
            ("Book1", 123, 3),
            ("Book2", 789, 1),
            ("Book2", 789, 2),
            ("Book2", 789, 3),
        ]
        self.df_bw_and_fw_fill_expected = self.spark.createDataFrame(
            self.df_bw_and_fw_fill_data, self.columns
        )

    def test_backward_fill(self):
        result_df = back_fill(self.df)
        result_data = result_df.collect()
        expected_data = self.df_bw_fill_expected.collect()
        self.assertEqual(result_data, expected_data)

    def test_forward_fill(self):
        result_df = forward_fill(self.df)
        result_data = result_df.collect()
        expected_data = self.df_fw_fill_expected.collect()
        self.assertEqual(result_data, expected_data)

    def test_backward_and_forward_fill(self):
        result_df = b_fill_and_f_fill_constituent_id_column(self.df)
        result_data = result_df.collect()
        expected_data = self.df_bw_and_fw_fill_expected.collect()
        self.assertEqual(result_data, expected_data)
