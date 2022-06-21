import unittest
from unittest import mock
from tap_google_sheets.transform import transform_sheet_number_data

class TestNumberTransform(unittest.TestCase):
    """Verify that boolean values falls back as string"""

    @mock.patch("tap_google_sheets.transform.LOGGER.info")
    def test_number_transform_boolean_as_string(self, mocked_logger_info):
        """Verify that boolean values falls back as string"""
        value = True
        transformed_data = transform_sheet_number_data("TRUE", value, sheet_title='test-sheet', col_name='test-column', col_letter='col', row_num=1, col_type='numberType')
        self.assertIsInstance(transformed_data, str)
        self.assertEqual(transformed_data, "True")
        # verify warning logger is called with expected params
        mocked_logger_info.assert_called_with("WARNING: POSSIBLE DATA TYPE ERROR: SHEET: {}, COL: {}, CELL: {}{}, TYPE: {} ".format(
                "test-sheet", "test-column", "col", 1, "numberType"))

    def test_number_transform_int_value_as_int(self):
        """Verify that int values falls back as type int"""
        value = 1
        transformed_data = transform_sheet_number_data("1", value, sheet_title='test-sheet', col_name='test-column', col_letter='col', row_num=1, col_type='numberType')
        self.assertIsInstance(transformed_data, int)
        self.assertEqual(transformed_data, 1)

    def test_number_transform_int_exponential_value_as_int(self):
        """Verify that exponential int values falls back as type int"""
        value = 1234
        transformed_data = transform_sheet_number_data("1.23E+03", value, sheet_title='test-sheet', col_name='test-column', col_letter='col', row_num=1, col_type='numberType')
        self.assertIsInstance(transformed_data, int)
        self.assertEqual(transformed_data, 1234)

    def test_number_transform_int_US_format_value_as_int(self):
        """Verify that US format int values falls back as type int"""
        value = 1234
        transformed_data = transform_sheet_number_data("1,234", value, sheet_title='test-sheet', col_name='test-column', col_letter='col', row_num=1, col_type='numberType')
        self.assertIsInstance(transformed_data, int)
        self.assertEqual(transformed_data, 1234)

    def test_number_transform_float_value_as_float(self):
        """Verify that float values falls back as type float"""
        value = 1.1
        transformed_data = transform_sheet_number_data("1.1", value, sheet_title='test-sheet', col_name='test-column', col_letter='col', row_num=1, col_type='numberType')
        self.assertIsInstance(transformed_data, float)
        self.assertEqual(transformed_data, 1.1)

    def test_number_transform_float_exponential_value_as_float(self):
        """Verify that exponential float values falls back as type float"""
        value = 5e-16
        transformed_data = transform_sheet_number_data("5.00E-16", value, sheet_title='test-sheet', col_name='test-column', col_letter='col', row_num=1, col_type='numberType')
        self.assertIsInstance(transformed_data, float)
        self.assertEqual(transformed_data, 5e-16)

    def test_number_transform_float_US_format_value_as_float(self):
        """Verify that US format float values falls back as type float"""
        value = 1234.1
        transformed_data = transform_sheet_number_data("1,234.1", value, sheet_title='test-sheet', col_name='test-column', col_letter='col', row_num=1, col_type='numberType')
        self.assertIsInstance(transformed_data, float)
        self.assertEqual(transformed_data, 1234.1)

    def test_number_transform_datetime_value_as_string(self):
        """Verify that datetime values falls back as type string"""

        datetime_expected_value = transform_sheet_number_data("01/01/2022 0:00:00", 44562, "test_sheet", "Number Column", "A", 4, "numberType")

        self.assertEqual(datetime_expected_value, "01/01/2022 0:00:00")
        self.assertIsInstance(datetime_expected_value, str)

    def test_number_transform_time_value_as_string(self):
        """Verify that time values falls back as type string"""

        time_expected_value = transform_sheet_number_data("5:00 PM", 0.7083333333333334, "test_sheet", "Number Column", "A", 5, "numberType")

        self.assertEqual(time_expected_value, "5:00 PM")
        self.assertIsInstance(time_expected_value, str)
