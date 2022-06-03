import unittest
from unittest import mock
from tap_google_sheets.transform import transform_sheet_number_data

class TestDatetimeInNumberColumn(unittest.TestCase):
    """
        Test cases to verify datetime values are falling back to string in number types column
    """

    def test_integer(self):
        """
            Test case to verify we are getting integer type data if we are passing in 'numberType' column
        """
        int_type = transform_sheet_number_data("1", 1, "test_sheet", "Number Column", "A", 2, "numberType")
        self.assertEqual(int_type, 1)

    def test_float(self):
        """
            Test case to verify we are getting float type data if we are passing in 'numberType' column
        """
        float_type = transform_sheet_number_data("1.1", 1.1, "test_sheet", "Number Column", "A", 3, "numberType")
        self.assertEqual(float_type, 1.1)

    @mock.patch("tap_google_sheets.transform.LOGGER.info")
    def test_datetime(self, mocked_logger_info):
        """
            Test case to verify datetime values are falling back to string type for 'numberType' column
        """
        datetime_type = transform_sheet_number_data("01/01/2022 0:00:00", 44562, "test_sheet", "Number Column", "A", 4, "numberType")

        self.assertEqual(datetime_type, "01/01/2022 0:00:00")
        # verify the LOGGER is called with appropriate message
        mocked_logger_info.assert_called_with("WARNING: POSSIBLE DATA TYPE ERROR: SHEET: {}, COL: {}, CELL: {}{}, TYPE: {} ".format(
                "test_sheet", "Number Column", "A", 4, "numberType"))

    @mock.patch("tap_google_sheets.transform.LOGGER.info")
    def test_time(self, mocked_logger_info):
        """
            Test case to verify time values are falling back to string type for 'numberType' column
        """
        time_type = transform_sheet_number_data("5:00 PM", 0.7083333333333334, "test_sheet", "Number Column", "A", 5, "numberType")

        self.assertEqual(time_type, "5:00 PM")
        # verify the LOGGER is called with appropriate message
        mocked_logger_info.assert_called_with("WARNING: POSSIBLE DATA TYPE ERROR: SHEET: {}, COL: {}, CELL: {}{}, TYPE: {} ".format(
                "test_sheet", "Number Column", "A", 5, "numberType"))
