import unittest
from unittest.case import TestCase
from unittest import mock
import tap_google_sheets
from tap_google_sheets import schema


class TestLogger(unittest.TestCase):
    @mock.patch('tap_google_sheets.schema.LOGGER.warn')
    def test_logger_message(self, mocked_logger):
        """
        Test if the logger statement is printed when the header row is empty and the sheet is being skipped.
        """
        sheet_data = {
            "properties": {
                "sheetId": 0,
                "title":"Sheet1",
                "index":0,
                "sheetType":"GRID",
                "gridProperties":{
                    "rowCount":3,
                    "columnCount":26
                }
            },
            "data": [{
                "rowData": [{
                    "values": [
                        {
                            "effectiveFormat":{
                                "fontFamily":"Calibri"
                            }
                        },
                        {
                            "effectiveFormat":{
                                "fontFamily":"Calibri"
                            }
                        },
                        {
                            "effectiveFormat":{
                                "fontFamily":"Calibri"
                            }
                        }
                    ]
                },
                {
                    "values":[{
                        "userEnteredValue":{
                            "stringValue":"A"
                        }
                    },
                    {
                        "userEnteredValue":{
                            "stringValue":"B"
                        }
                    },
                    {
                        "userEnteredValue":{
                            "stringValue":"C"
                        }
                    }]
                }
                ]
            }]
        }
        sheet_schema, columns = schema.get_sheet_schema_columns(sheet_data)
        # check if the logger is called with correct logger message
        mocked_logger.assert_called_with('SKIPPING THE SHEET AS FOUND TWO CONSECUTIVE EMPTY HEADERS. SHEET: Sheet1')
