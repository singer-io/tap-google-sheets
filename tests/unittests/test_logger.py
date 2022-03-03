import unittest
from unittest import mock
import tap_google_sheets
from tap_google_sheets import schema


class TestLogger(unittest.TestCase):
    @mock.patch('tap_google_sheets.schema.LOGGER.warning')
    def test_logger_message(self, mocked_logger):
        """
        Test if the logger statement is printed when the header row is empty and the sheet is being skipped.
        """
        sheet_data = {
            "properties":{
                "sheetId":2074712559,
                "title":"Sheet5",
                "index":1,
                "sheetType":"GRID",
                "gridProperties":{
                    "rowCount":1000,
                    "columnCount":26
                }
            },
            "data":[
                {
                    "rowData":[
                        {},
                        {
                        "values":[
                            {
                                "userEnteredValue":{
                                    "numberValue":1
                                },
                                "effectiveValue":{
                                    "numberValue":1
                                },
                                "formattedValue":"1",
                            },
                            {
                                "userEnteredValue":{
                                    "numberValue":2
                                },
                                "effectiveValue":{
                                    "numberValue":2
                                },
                                "formattedValue":"2"
                            },
                            {
                                "userEnteredValue":{
                                    "numberValue":3
                                },
                                "effectiveValue":{
                                    "numberValue":3
                                },
                                "formattedValue":"3",
                            }
                        ]
                        }
                    ],
                    "rowMetadata":[
                        {
                        "pixelSize":21
                        }
                    ],
                    "columnMetadata":[
                        {
                        "pixelSize":100
                        }
                    ]
                }
            ]
        }
        # retrieve the sheet title from the `sheet_data`
        sheet_title = sheet_data.get('properties', {}).get('title')
        sheet_schema, columns = schema.get_sheet_schema_columns(sheet_data)
        # check if the logger is called with correct logger message
        mocked_logger.assert_called_with('SKIPPING THE SHEET AS HEADERS ROW IS EMPTY. SHEET: {}'.format(sheet_title))
