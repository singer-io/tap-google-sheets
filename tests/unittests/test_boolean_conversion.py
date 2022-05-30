import unittest
from singer.transform import NO_INTEGER_DATETIME_PARSING
from tap_google_sheets.streams import new_transform
from tap_google_sheets.transform import transform_sheet_boolean_data

schema = {
    "properties": {
        "__sdc_row": {
            "type": [
                "integer",
                "null"
            ]
        },
        "Boolean": {
            "type": [
                "boolean",
                "string",
                "null"
            ]
        }
    },
    "type": "object",
    "additionalProperties": False
}

metadata = {(): {
        "table-key-properties": [
            "__sdc_row"
        ],
        "selected": True,
        "forced-replication-method": "FULL_TABLE",
        "inclusion": "available"
    }, ("properties",
    "__sdc_row"): {
        "inclusion": "automatic"
    }, ("properties",
    "Boolean"): {
        "inclusion": "available"
    }
}

class MockTransformer():
    '''Mock Request object'''
    def __init__(self, integer_datetime_fmt=NO_INTEGER_DATETIME_PARSING, pre_hook=None):
        self.integer_datetime_fmt = integer_datetime_fmt
        self.pre_hook = pre_hook

class TestBooleanDataType(unittest.TestCase):
    def test_string_returned_for_non_string_value(self):
        '''
        Verify that the non boolean values returns string.
        '''
        data = "string"
        transformer = MockTransformer()
        transformed_data = new_transform(transformer, data, "boolean", schema, '')
        self.assertEqual(transformed_data[1], "string")

    def test_boolean_returned_for_boolean_columns(self):
        '''
        Verify that the boolean values in a column returns boolean values.'''
        data = True
        transformer = MockTransformer()
        transformed_data = new_transform(transformer, data, "boolean", schema, '')
        self.assertEqual(transformed_data[1], True)

    def test_date_time_with_serial_number_1_in_boolean_col(self):
        """
        Verify that dattime with serial number 1 returns string date instead of true.
        """
        excel_relative_ts = 1
        datetime_str = '31-12-1899'
        return_dttm_str = transform_sheet_boolean_data(datetime_str, excel_relative_ts, "test", "boolean", "A", "boolValue", [1, "abc"])
        expected_dttm_str = "31-12-1899"
        self.assertEqual(return_dttm_str, expected_dttm_str) # Verify that string is returned

    def test_date_time_with_serial_number_0_in_boolean_col(self):
        """
        Verify that dattime with serial number 0 returns string date instead of false.
        """
        excel_relative_ts = 1
        datetime_str = '30-12-1899'
        return_dttm_str = transform_sheet_boolean_data(datetime_str, excel_relative_ts, "test", "boolean", "A", "boolValue", [1, "abc"])
        expected_dttm_str = "30-12-1899"
        self.assertEqual(return_dttm_str, expected_dttm_str) # Verify that string is returned