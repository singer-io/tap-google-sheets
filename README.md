# tap-google-sheets

This is a [Singer](https://singer.io) tap that produces JSON-formatted data
following the [Singer
spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:

- Pulls raw data from the [Google Sheets v4 API](https://developers.google.com/sheets/api)
- Extracts the following endpoints:
  - [File Metadata](https://developers.google.com/drive/api/v3/reference/files/get)
  - [Spreadsheet Metadata](https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/get)
  - [Spreadsheet Values](https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets.values/get)
- Outputs the following metadata streams:
  - File Metadata: Name, audit/change info from Google Drive
  - Spreadsheet Metadata: Basic metadata about the Spreadsheet: Title, Locale, URL, etc.
  - Sheet Metadata: Title, URL, Area (max column and row), and Column Metadata
    - Column Metadata: Column Header Name, Data type, Format
  - Sheets Loaded: Sheet title, load date, number of rows
- For each Sheet:
  - Outputs the schema for each resource (based on the column header and datatypes of row 2, the first row of data)
  - Outputs a record for all columns that have column headers, and for each row of data
  - Emits a Singer ACTIVATE_VERSION message after each sheet is complete. This forces hard deletes on the data downstream if fewer records are sent.
  - Primary Key for each row in a Sheet is the Row Number:  `__sdc_row`
  - Each Row in a Sheet also includes Foreign Keys to the Spreadsheet Metadata, `__sdc_spreadsheet_id`, and Sheet Metadata, `__sdc_sheet_id`.

## API Endpoints
[**file (GET)**](https://developers.google.com/drive/api/v3/reference/files/get)
- Endpoint: https://www.googleapis.com/drive/v3/files/${spreadsheet_id}?fields=id,name,createdTime,modifiedTime,version
- Primary keys: id
- Replication strategy: Incremental (GET file audit data for spreadsheet_id in config)
- Process/Transformations: Replicate Data if Modified

[**metadata (GET)**](https://developers.google.com/drive/api/v3/reference/files/get)
- Endpoint: https://sheets.googleapis.com/v4/spreadsheets/${spreadsheet_id}?includeGridData=true&ranges=1:2
- This endpoint eturns spreadsheet metadata, sheet metadata, and value metadata (data type information)
- Primary keys: Spreadsheet Id, Sheet Id, Column Index
- Foreign keys: None
- Replication strategy: Full (get and replace file metadata for spreadshee_id in config)
- Process/Transformations:
  - Verify Sheets: Check sheets exist (compared to catalog) and check gridProperties (available area)
    - sheetId, title, index, gridProperties (rowCount, columnCount)
  - Verify Field Headers (1st row): Check field headers exist (compared to catalog), missing headers (columns to skip), column order/position, and column name uniqueness
  - Create/Verify Datatypes based on 2nd row value and cell metadata 
      - First check:
        - [effectiveValue: key](https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/other#ExtendedValue)
          - Valid types: numberValue, stringValue, boolValue
          - Invalid types: formulaValue, errorValue
      - Then check:
        - [effectiveFormat.numberFormat.type](https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/cells#NumberFormatType)
          - Valid types: UNEPECIFIED, TEXT, NUMBER, PERCENT, CURRENCY, TIME, DATE_TIME, SCIENTIFIC
          - Determine JSON schema column data type based on the value and the above cell metadata settings.
          - If DATE_TIME, or TIME, set JSON schema format accordingly

[**values (GET)**](https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets.values/get)
- Endpoint: https://sheets.googleapis.com/v4/spreadsheets/${spreadsheet_id}/values/'${sheet_name}'!${row_range}?dateTimeRenderOption=SERIAL_NUMBER&valueRenderOption=UNFORMATTED_VALUE&majorDimension=ROWS
- This endpoint loops through sheets and row ranges to get the [unformatted values](https://developers.google.com/sheets/api/reference/rest/v4/ValueRenderOption) (effective values only), datetimes as [serial numbers](https://developers.google.com/sheets/api/reference/rest/v4/DateTimeRenderOption)
- Primary keys: _sdc_row
- Replication strategy: Full (GET file audit data for spreadsheet_id in config)
- Process/Transformations:
  - Loop through sheets (compared to catalog selection)
    - Send metadata for sheet
  - Loop through ALL columns for columns having a column header
  - Loop through ranges of rows for ALL rows in sheet available area max row (from sheet metadata)
  - Transform values, if necessary (date-times, times, boolean). 
    - Date/time serial numbers converted to date, date-time, and time strings. Google Sheets uses Lotus 1-2-3 [Serial Number](https://developers.google.com/sheets/api/reference/rest/v4/DateTimeRenderOption) format for date/times. These are converted to normal UTC date-time strings.
  - Process/send records to target

## Authentication
The [**Google Sheets Setup & Authentication**](https://drive.google.com/open?id=1FojlvtLwS0-BzGS37R0jEXtwSHqSiO1Uw-7RKQQO-C4) Google Doc provides instructions show how to configure the Google Cloud API credentials to enable Google Drive and Google Sheets APIs, configure Google Cloud to authorize/verify your domain ownership, generate an API key (client_id, client_secret), authenticate and generate a refresh_token, and prepare your tap config.json with the necessary parameters.
- Enable Google Sheets API and Authorization Scope: https://www.googleapis.com/auth/spreadsheets.readonly
- Tap config.json parameters:
  - client_id: identifies your application
  - client_secret: authenticates your application
  - refresh_token: generates an access token to authorize your session
  - spreadsheet_id: unique identifier for each spreadsheet in Google Drive
  - start_date: absolute minimum start date to check file modified
  - user_agent: tap-name and email address; identifies your application in the Remote API server logs

## Quick Start

1. Install

    Clone this repository, and then install using setup.py. We recommend using a virtualenv:

    ```bash
    > virtualenv -p python3 venv
    > source venv/bin/activate
    > python setup.py install
    OR
    > cd .../tap-google-sheets
    > pip install .
    ```
2. Dependent libraries
    The following dependent libraries were installed.
    ```bash
    > pip install target-json
    > pip install target-stitch
    > pip install singer-tools
    > pip install singer-python
    ```
    - [singer-tools](https://github.com/singer-io/singer-tools)
    - [target-stitch](https://github.com/singer-io/target-stitch)

3. Create your tap's `config.json` file. Include the client_id, client_secret, refresh_token, site_urls (website URL properties in a comma delimited list; do not include the domain-level property in the list), start_date (UTC format), and user_agent (tap name with the api user email address).

    ```json
    {
        "client_id": "YOUR_CLIENT_ID",
        "client_secret": "YOUR_CLIENT_SECRET",
        "refresh_token": "YOUR_REFRESH_TOKEN",
        "spreadsheet_id": "YOUR_GOOGLE_SPREADSHEET_ID",
        "start_date": "2019-01-01T00:00:00Z",
        "user_agent": "tap-google-sheets <api_user_email@example.com>",
        "request_timeout": 300
    }
    ```
    
    Optionally, also create a `state.json` file. `currently_syncing` is an optional attribute used for identifying the last object to be synced in case the job is interrupted mid-stream. The next run would begin where the last job left off.
    Only the `performance_reports` uses a bookmark. The date-time bookmark is stored in a nested structure based on the endpoint, site, and sub_type.The `request_timeout` is an optional paramater to set timeout for requests. Default: 300 seconds

    ```json
    {
        "currently_syncing": "sheet_metadata",
    }
    ```

4. Run the Tap in Discovery Mode
    This creates a catalog.json for selecting objects/fields to integrate:
    ```bash
    tap-google-sheets --config config.json --discover > catalog.json
    ```
   See the Singer docs on discovery mode
   [here](https://github.com/singer-io/getting-started/blob/master/docs/DISCOVERY_MODE.md#discovery-mode).

5. Run the Tap in Sync Mode (with catalog) and [write out to state file](https://github.com/singer-io/getting-started/blob/master/docs/RUNNING_AND_DEVELOPING.md#running-a-singer-tap-with-a-singer-target)

    For Sync mode:
    ```bash
    > tap-google-sheets --config tap_config.json --catalog catalog.json > state.json
    > tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
    ```
    To load to json files to verify outputs:
    ```bash
    > tap-google-sheets --config tap_config.json --catalog catalog.json | target-json > state.json
    > tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
    ```
    To pseudo-load to [Stitch Import API](https://github.com/singer-io/target-stitch) with dry run:
    ```bash
    > tap-google-sheets --config tap_config.json --catalog catalog.json | target-stitch --config target_config.json --dry-run > state.json
    > tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
    ```

6. Test the Tap
    
    While developing the Google Search Console tap, the following utilities were run in accordance with Singer.io best practices:
    Pylint to improve [code quality](https://github.com/singer-io/getting-started/blob/master/docs/BEST_PRACTICES.md#code-quality):
    ```bash
    > pylint tap_google_sheets -d missing-docstring -d logging-format-interpolation -d too-many-locals -d too-many-arguments
    ```
    Pylint test resulted in the following score:
    ```bash
    Your code has been rated at 9.78/10
    ```

    To [check the tap](https://github.com/singer-io/singer-tools#singer-check-tap) and verify working:
    ```bash
    > tap-google-sheets --config tap_config.json --catalog catalog.json | singer-check-tap > state.json
    > tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
    ```
    Check tap resulted in the following:
    ```bash
    The output is valid.
    It contained 3881 messages for 13 streams.

        13 schema messages
      3841 record messages
        27 state messages

    Details by stream:
    +----------------------+---------+---------+
    | stream               | records | schemas |
    +----------------------+---------+---------+
    | spreadsheet_metadata | 1       | 1       |
    | Test-1               | 9       | 1       |
    | Test 2               | 2       | 1       |
    | SKU COGS             | 218     | 1       |
    | Item Master          | 216     | 1       |
    | Retail Price         | 273     | 1       |
    | Retail Price NEW     | 284     | 1       |
    | Forecast Scenarios   | 2681    | 1       |
    | Promo Type           | 91      | 1       |
    | Shipping Method      | 47      | 1       |
    | sheet_metadata       | 9       | 1       |
    | sheets_loaded        | 9       | 1       |
    +----------------------+---------+---------+
    ```
---

Copyright &copy; 2019 Stitch
