import unittest
from unittest import mock
from tap_google_sheets.transform import transform_sheet_number_data

class TestNumberTransform(unittest.TestCase):
    """Verify that boolean values falls back as string"""
    def test_number_transform_boolean_as_string(self):
        """Verify that boolean values falls back as string"""
        value = True
        transformed_data = transform_sheet_number_data("TRUE", value, sheet_title='test-sheet', col_name='test-column', col_letter='col', row_num=1, col_type='numberType')
        self.assertIsInstance(transformed_data, str)
        self.assertEqual(transformed_data, "True")

    def test_number_transform_int_value_as_int(self):
        """Verify that int values falls back as type int"""
        value = 1
        transformed_data = transform_sheet_number_data("1", value, sheet_title='test-sheet', col_name='test-column', col_letter='col', row_num=1, col_type='numberType')
        self.assertIsInstance(transformed_data, int)
        self.assertEqual(transformed_data, 1)

    def test_number_transform_float_value_as_float(self):
        """Verify that float values falls back as type float"""
        value = 1.1
        transformed_data = transform_sheet_number_data("1.1", value, sheet_title='test-sheet', col_name='test-column', col_letter='col', row_num=1, col_type='numberType')
        self.assertIsInstance(transformed_data, float)
        self.assertEqual(transformed_data, 1.1)

    @mock.patch("tap_google_sheets.transform.LOGGER.info")
    def test_datetime(self, mocked_logger_info):
        """Verify that datetime values falls back as type string"""

        datetime_type = transform_sheet_number_data("01/01/2022 0:00:00", 44562, "test_sheet", "Number Column", "A", 4, "numberType")

        self.assertEqual(datetime_type, "01/01/2022 0:00:00")
        self.assertIsInstance(datetime_type, str)
        # verify the LOGGER is called with appropriate message
        mocked_logger_info.assert_called_with("WARNING: POSSIBLE DATA TYPE ERROR: SHEET: {}, COL: {}, CELL: {}{}, TYPE: {} ".format(
                "test_sheet", "Number Column", "A", 4, "numberType"))

    @mock.patch("tap_google_sheets.transform.LOGGER.info")
    def test_time(self, mocked_logger_info):
        """Verify that time values falls back as type string"""

        time_type = transform_sheet_number_data("5:00 PM", 0.7083333333333334, "test_sheet", "Number Column", "A", 5, "numberType")

        self.assertEqual(time_type, "5:00 PM")
        self.assertIsInstance(time_type, str)
        # verify the LOGGER is called with appropriate message
        mocked_logger_info.assert_called_with("WARNING: POSSIBLE DATA TYPE ERROR: SHEET: {}, COL: {}, CELL: {}{}, TYPE: {} ".format(
                "test_sheet", "Number Column", "A", 5, "numberType"))
