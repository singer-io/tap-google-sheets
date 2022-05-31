import unittest
from tap_google_sheets.transform import transform_sheet_number_data

class TestNumberTransform(unittest.TestCase):
    """Verify that boolean values falls back as string"""
    def test_number_transform_boolean_as_string(self):
        """Verify that boolean values falls back as string"""
        value = True
        transformed_data = transform_sheet_number_data(value, sheet_title='test-sheet', col_name='test-column', col_letter='col', row_num=1, col_type='numberType')
        self.assertIsInstance(transformed_data, str)
        self.assertEqual(transformed_data, "True")

    def test_number_transform_int_value_as_int(self):
        """Verify that int values falls back as type int"""
        value = 1
        transformed_data = transform_sheet_number_data(value, sheet_title='test-sheet', col_name='test-column', col_letter='col', row_num=1, col_type='numberType')
        self.assertIsInstance(transformed_data, int)
        self.assertEqual(transformed_data, 1)

    def test_number_transform_float_value_as_float(self):
        """Verify that float values falls back as type float"""
        value = 1.0
        transformed_data = transform_sheet_number_data(value, sheet_title='test-sheet', col_name='test-column', col_letter='col', row_num=1, col_type='numberType')
        self.assertIsInstance(transformed_data, float)
        self.assertEqual(transformed_data, 1.0)