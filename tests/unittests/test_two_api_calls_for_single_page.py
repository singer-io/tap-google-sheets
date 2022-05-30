import unittest
from unittest import mock
from tap_google_sheets.streams import SheetsLoadData
from tap_google_sheets.client import GoogleClient

sheet_schema = {'type': 'object', 'additionalProperties': False, 'properties': {'__sdc_spreadsheet_id': {'type': ['null', 'string']}, '__sdc_sheet_id': {'type': ['null', 'integer']}, '__sdc_row': {'type': ['null', 'integer']}, 'date': {'type': ['null', 'string']}, 'value': {'anyOf': [{'type': ['null', 'string'], 'format': 'date'}, {'type': ['null', 'string']}]}}}
columns = [{'columnIndex': 1, 'columnLetter': 'A', 'columnName': 'date', 'columnType': 'stringValue', 'columnSkipped': False}, {'columnIndex': 2, 'columnLetter': 'B', 'columnName': 'value', 'columnType': 'numberType.DATE', 'columnSkipped': False}]

class TestUnsupportedFields(unittest.TestCase):
    @mock.patch('tap_google_sheets.client.GoogleClient.get')
    @mock.patch('tap_google_sheets.streams.schema.get_sheet_metadata', return_value = [sheet_schema, columns])
    @mock.patch('tap_google_sheets.streams.get_selected_fields', return_value = [])
    @mock.patch('tap_google_sheets.streams.write_schema')
    @mock.patch('tap_google_sheets.streams.GoogleSheets.process_records')
    def test_two_api_calls(self, mock_process_records, mock_write_schema, mocked_get_selected_fields, mocked_sheet_metadata, mocked_get):
        """
        Verify that we make 2 API calls instead of 1 for a single page of data
        """
        config = {
            "spreadsheet_id": "id",
            "start_date": "2019-01-01T00:00:00Z"
        }
        sheets = [{
            "properties": {
            "sheetId": 1260142713,
            "title": "Sheet13",
            "index": 15,
            "sheetType": "GRID",
            "gridProperties": {
                "rowCount": 100,
                "columnCount": 5
            }
            }
        }]
        client = GoogleClient("dummy_client_id", "dummy_client_secret", "dummy_refresh_token", 300)
        sheets_load_data = SheetsLoadData(client, config.get("spreadsheet_id"), config.get("start_date"))
        sheets_load_data.load_data({}, {}, ["Sheet13"], sheets, "time")
        self.assertEqual(mock.call(api='sheets', endpoint='Sheet13', params='dateTimeRenderOption=SERIAL_NUMBER&valueRenderOption=FORMATTED_VALUE&majorDimension=ROWS', path="spreadsheets/id/values/'Sheet13'!A2:B100"), mocked_get.mock_calls[0])
        self.assertEqual(mock.call(api='sheets', endpoint='Sheet13', params='dateTimeRenderOption=SERIAL_NUMBER&valueRenderOption=UNFORMATTED_VALUE&majorDimension=ROWS', path="spreadsheets/id/values/'Sheet13'!A2:B100"), mocked_get.mock_calls[2]) # because also calling sheet.get('values', []) in the code
        # Verify that the get() is called 2 times
        self.assertEqual(mocked_get.call_count, 2)