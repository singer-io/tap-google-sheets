import unittest
from singer.transform import NO_INTEGER_DATETIME_PARSING
from tap_google_sheets.streams import new_transform

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
        print(transformed_data)
        self.assertEqual(transformed_data[1], "string")

    def test_boolean_returned_for_boolean_columns(self):
        '''
        Verify that the boolean values in a column returns boolean values.'''
        data = True
        transformer = MockTransformer()
        transformed_data = new_transform(transformer, data, "boolean", schema, '')
        print(transformed_data)
        self.assertEqual(transformed_data[1], True)