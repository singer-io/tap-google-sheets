import unittest
from tap_google_sheets.transform import transform_sheet_datetime_data, excel_to_dttm_str

class TestDateTimeTransform(unittest.TestCase):
    """Verify that the out of range dates does not throw any exception while converting"""
    def test_correct_string_values_when_out_of_range_max(self):
        """Verify that the out of range date maximum does not throw any exception while converting"""
        excel_relative_ts = 5000000
        expected_dttm_str = '13/07/15589 00:00:00'
        return_dttm_str, _ = excel_to_dttm_str(expected_dttm_str, excel_relative_ts)
        self.assertEqual(return_dttm_str, expected_dttm_str) # Verify that the ts is correctly transformed

    def test_correct_string_values_when_out_of_range_min(self):
        """Verify that the out of range dates minimum does not throw any exception while converting"""
        excel_relative_ts = -694000
        expected_dttm_str = '11/21/00-1 00:00:00'
        return_dttm_str, _ = excel_to_dttm_str(expected_dttm_str, excel_relative_ts)
        self.assertEqual(return_dttm_str, expected_dttm_str) # Verify that the ts is correctly transformed 

    def test_correct_datetime_string_values_in_range(self):
        """Verify that in range datetime values returns converted datetime string"""
        excel_relative_ts = 44604.520833333336
        datetime_str = '12/02/2022 00:00:00'
        return_dttm_str, _ = excel_to_dttm_str(datetime_str, excel_relative_ts)
        expected_dttm_str = "2022-02-12T12:30:00.000000Z"
        self.assertEqual(return_dttm_str, expected_dttm_str) # Verify that the ts is correctly transformed 

    def test_string_values(self):
        """Verify that string date in a cell returns the result in same format without converting"""
        expected_datetime_str = '12/02/2022 00:00:00'
        return_dttm_str = transform_sheet_datetime_data(expected_datetime_str, expected_datetime_str, "test", "datetime", "A", 1, " numberType.DATE_TIME")
        self.assertEqual(return_dttm_str, expected_datetime_str) # Verify that the ts is correctly transformed 