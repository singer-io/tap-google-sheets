import unittest
from tap_google_sheets import schema

SHEET = {
    "properties":{
        "sheetId":1822945984,
        "title":"Sheet26",
        "index":1,
        "sheetType":"GRID",
        "gridProperties":{
            "rowCount":20,
            "columnCount":1
        }
    },
    "data":[
        {
            "rowData":[
                {
                    "values":[
                        {
                            "formattedValue":"Column1",
                        }
                    ]
                },
                {
                    "values":[
                        {  
                            "effectiveFormat": {
                                "numberFormat": {
                                    "type": None,
                                }
                            }
                        }
                    ]
                }
            ]
        }
    ]
}

class TestNullCellFormat(unittest.TestCase):

    def test_null_datetime_effectiveFormat(self):
        """
        Test when no value is given in second row of date-time, discovery is locking at 'effectiveFormat'.
        And returns type and format in schema according to that.
        """
        sheet = SHEET
        sheet["data"][0]["rowData"][1]["values"][0]["effectiveFormat"]["numberFormat"]["type"] = "DATE_TIME"
        expected_format = {
                            "type": [
                                "null",
                                "string"
                            ],
                            "format": "date-time"
                        }

        sheet_json_schema, columns = schema.get_sheet_schema_columns(sheet)
        returned_formats = sheet_json_schema["properties"]["Column1"]["anyOf"]

        # verify the returned schema has expected field types and format
        self.assertIn(expected_format,returned_formats)

    def test_null_date_effectiveFormat(self):
        """
        Test when no value is given in second row of date, discovery is locking at 'effectiveFormat'.
        And returns type and format in schema according to that.
        """
        sheet = SHEET
        sheet["data"][0]["rowData"][1]["values"][0]["effectiveFormat"]["numberFormat"]["type"] = "DATE"

        expected_format = {
                            "type": [
                                "null",
                                "string"
                            ],
                            "format": "date"
                        }

        sheet_json_schema, columns = schema.get_sheet_schema_columns(sheet)
        returned_formats = sheet_json_schema["properties"]["Column1"]["anyOf"]

        # verify the returned schema has expected field types and format
        self.assertIn(expected_format,returned_formats)

    def test_null_time_effectiveFormat(self):
        """
        Test when no value is given in second row of time, discovery is locking at 'effectiveFormat'.
        And returns type and format in schema according to that.
        """
        sheet = SHEET
        sheet["data"][0]["rowData"][1]["values"][0]["effectiveFormat"]["numberFormat"]["type"] = "TIME"

        expected_format = {
                            "type": [
                                "null",
                                "string"
                            ]
                        }

        sheet_json_schema, columns = schema.get_sheet_schema_columns(sheet)
        returned_formats = sheet_json_schema["properties"]["Column1"]["anyOf"]

        # verify the returned schema has expected field types and format
        self.assertIn(expected_format,returned_formats)

    def test_null_currency_effectiveFormat(self):
        """
        Test when no value is given in second row of currency, discovery is locking at 'effectiveFormat'.
        And returns type and format in schema according to that.
        """

        sheet = SHEET
        sheet["data"][0]["rowData"][1]["values"][0]["effectiveFormat"]["numberFormat"]["type"] = "CURRENCY"
        
        expected_format = {"type": ["null", "string"]}

        sheet_json_schema, columns = schema.get_sheet_schema_columns(sheet)
        returned_formats = sheet_json_schema["properties"]["Column1"]

        # verify returned schema has expected field types and format
        self.assertEqual(expected_format,returned_formats)
