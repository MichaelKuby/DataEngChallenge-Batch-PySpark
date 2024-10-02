import unittest

from src.utils.local.parse_dimension import parse_dimensions


class TestParseDimension(unittest.TestCase):
    def setUp(self):
        self.input_example_1 = 'Dimensions unavailable'
        self.input_example_2 = 'Diam. 11/16 in. (1.7 cm)'
        self.input_example_3 = 'Diam. 1/2 in. (1.3 cm)'
        self.input_example_4 = 'Diam. 1 1/8 in. (2.9 cm)'
        self.input_example_5 = '2 3/4 x 3 1/2 x 2 3/4 in. (7 x 8.9 x 7 cm)'
        self.input_example_6 = 'Overall: 19 7/16 x 13 x 9 1/4 in. (49.4 x 33 x 23.5 cm); 352 oz. 18 dwt. (10977 g)'
        self.input_example_7 = 'NULL'
        self.input_example_8 = 'H. 12 in. (30.5 cm)'
        self.input_example_9 = 'H. 12 3/8 in. (31.4 cm)'
        self.input_example_10 = '11 x 9 in. (27.9 x 22.9 cm)'
        self.input_example_11 = 'H. 6 9/16 in. (16.7 cm); Diam. 3 in. (7.6 cm)'
        self.input_example_12 = '30 7/8 x 25 x 13 7/8 in. (78.4 x 63.5 x 35.2 cm)'

    def test_parse_dimensions(self):
        self.assertEqual({'Height': None, 'Width': None, 'Length': None, 'Diameter': None, 'Unit': None},
                         parse_dimensions(self.input_example_1))
        self.assertEqual({'Height': None, 'Width': None, 'Length': None, 'Diameter': 0.6875, 'Unit': 'in.'},
                         parse_dimensions(self.input_example_2))
        self.assertEqual({'Height': None, 'Width': None, 'Length': None, 'Diameter': 0.5, 'Unit': 'in.'},
                         parse_dimensions(self.input_example_3))
        self.assertEqual({'Height': None, 'Width': None, 'Length': None, 'Diameter': 1.125, 'Unit': 'in.'},
                         parse_dimensions(self.input_example_4))
        self.assertEqual({'Height': 2.75, 'Width': 3.5, 'Length': 2.75, 'Diameter': None, 'Unit': 'in.'},
                         parse_dimensions(self.input_example_5))
        self.assertEqual({'Height': 19.4375, 'Width': 13.0, 'Length': 9.25, 'Diameter': None, 'Unit': 'in.'},
                         parse_dimensions(self.input_example_6))
        self.assertEqual({'Height': None, 'Width': None, 'Length': None, 'Diameter': None, 'Unit': None},
                         parse_dimensions(self.input_example_7))
        self.assertEqual({'Height': 12, 'Width': None, 'Length': None, 'Diameter': None, 'Unit': 'in.'},
                         parse_dimensions(self.input_example_8))
        self.assertEqual({'Height': 12.375, 'Width': None, 'Length': None, 'Diameter': None, 'Unit': "in."},
                         parse_dimensions(self.input_example_9))
        self.assertEqual({'Height': 11, 'Width': 9, 'Length': None, 'Diameter': None, 'Unit': "in."},
                         parse_dimensions(self.input_example_10))
        self.assertEqual({'Height': 6.5625, 'Width': None, 'Length': None, 'Diameter': None, 'Unit': "in."},
                         parse_dimensions(self.input_example_11))
        self.assertEqual({'Height': 30.875, 'Width': 25, 'Length': 13.875, 'Diameter': None, 'Unit': "in."},
                         parse_dimensions(self.input_example_12))
