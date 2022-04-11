import unittest
import cftime
from tap_google_sheets.transform import excel_to_dttm_str

class TestDateTimeTransform(unittest.TestCase):
    """Verify that the out of range dates does not throw any exception while converting"""
    def test_correct_string_values_when_out_of_range_max(self):
        """Verify that the out of range date maximum does not throw any exception while converting"""
        excel_relative_ts = 5000000
        return_dttm_str = excel_to_dttm_str(excel_relative_ts)
        expected_dttm_str = cftime.num2date((excel_relative_ts-25569)*86400, "seconds since 1970-01-01T00:00:00Z", calendar='proleptic_gregorian', only_use_cftime_datetimes=True, only_use_python_datetimes=False, has_year_zero=True).strftime()
        self.assertEqual(return_dttm_str, expected_dttm_str) # Verify that the ts is correctly transformed

    def test_correct_string_values_when_out_of_range_min(self):
        """Verify that the out of range dates minimum does not throw any exception while converting"""
        excel_relative_ts = -694000
        return_dttm_str = excel_to_dttm_str(excel_relative_ts)
        expected_dttm_str = cftime.num2date((excel_relative_ts-25569)*86400, "seconds since 1970-01-01T00:00:00Z", calendar='proleptic_gregorian', only_use_cftime_datetimes=True, only_use_python_datetimes=False, has_year_zero=True).strftime()
        self.assertEqual(return_dttm_str, expected_dttm_str) # Verify that the ts is correctly transformed