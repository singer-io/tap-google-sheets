config = {
    "test_name": "tap_google_sheets_combined_test",
    "tap_name": "tap-google-sheets",
    "type": "platform.google-sheets",
    "properties": {
        "spreadsheet_id": "TAP_GOOGLE_SHEETS_SPREADSHEET_ID",
        "start_date": "TAP_GOOGLE_SHEETS_START_DATE"
    },
    "credentials": {
        "client_id": "TAP_GOOGLE_SHEETS_CLIENT_ID",
        "client_secret": "TAP_GOOGLE_SHEETS_CLIENT_SECRET",
        "refresh_token": "TAP_GOOGLE_SHEETS_REFRESH_TOKEN",
    },
    "bookmark": {
        "bookmark_key": "file_metadata",
        "bookmark_timestamp": "2019-12-03T23:09:01.380000Z"
    },
    "streams" : {
        "file_metadata": {"id"},
        "sheet_metadata": {"sheetId"},
        "sheets_loaded": {"spreadsheetId", "sheetId", "loadDate"},
        "spreadsheet_metadata": {"spreadsheetId"},
        "Test-1": {"__sdc_row"},
        "Test 2": {"__sdc_row"},
        "SKU COGS": {"__sdc_row"},
        "Item Master": {"__sdc_row"},
        "Retail Price": {"__sdc_row"},
        "Retail Price NEW": {"__sdc_row"},
        "Forecast Scenarios": {"__sdc_row"},
        "Promo Type": {"__sdc_row"},
        "Shipping Method": {"__sdc_row"}
    }
} 
