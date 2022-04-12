import unittest
from tap_google_sheets.singer_transform import transform

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
class TestBooleanDataType(unittest.TestCase):
    def test_string_returned_for_non_boolean_columns(self):
        '''
        Verify that the non boolean values returns string.
        '''
        record = {
            "__sdc_row": 1,
            "Boolean": "string",
        }
        transformed_record = transform(record, schema, metadata)
        self.assertEqual(transformed_record.get("Boolean"), record.get("Boolean"))

    def test_string_returned_for_boolean_columns(self):
        '''
        Verify that the boolean values in a column returns boolean values.'''
        record = {
            "__sdc_row": 1,
            "Boolean": True,
        }
        transformed_record = transform(record, schema, metadata)
        self.assertEqual(transformed_record.get("Boolean"), record.get("Boolean"))