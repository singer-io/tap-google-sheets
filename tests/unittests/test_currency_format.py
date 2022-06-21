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
                    "values":[{
                        "userEnteredValue": {
                            "numberValue": 878
                        },
                        "effectiveValue": {
                        "numberValue": 878
                        },
                        "formattedValue": "£878.00",
                        "userEnteredFormat": {
                            "numberFormat": {
                                "type": "CURRENCY",
                                "pattern": "[$£-809]#,##0.00"
                            }
                        },
                        "effectiveFormat": {
                            "numberFormat": {
                                "type": "CURRENCY",
                                "pattern": "[$£-809]#,##0.00"
                            }
                        }
                    }]
                }
            ]
        }
    ]
}

class TestNullCellFormat(unittest.TestCase):

    def test_null_currency_effectiveFormat(self):
        """
        Test when number format currency value is given in the cell, the schema return string type
        """

        sheet = SHEET
        expected_format = {"type": ["null", "string"]}

        sheet_json_schema, columns = schema.get_sheet_schema_columns(sheet)
        print(sheet_json_schema)
        returned_formats = sheet_json_schema["properties"]["Column1"]

        # verify returned schema has expected field types and format
        self.assertEqual(expected_format, returned_formats)
