from collections import OrderedDict

# streams: API URL endpoints to be called
# properties:
#   <root node>: Plural stream name for the endpoint
#   path: API endpoint relative path, when added to the base URL, creates the full path,
#       default = stream_name
#   key_properties: Primary key fields for identifying an endpoint record.
#   replication_method: INCREMENTAL or FULL_TABLE
#   replication_keys: bookmark_field(s), typically a date-time, used for filtering the results
#       and setting the state
#   params: Query, sort, and other endpoint specific parameters; default = {}
#   data_key: JSON element containing the results list for the endpoint;
#       default = root (no data_key)

# file_metadata: Queries Google Drive API to get file information and see if file has been modified
#    Provides audit info about who and when last changed the file.
FILE_METADATA = {
    "api": "files",
    "path": "files/{spreadsheet_id}",
    "key_properties": ["id"],
    "replication_method": "INCREMENTAL",
    "replication_keys": ["modifiedTime"],
    "params": {
        "fields": "id,name,createdTime,modifiedTime,version,teamDriveId,driveId,lastModifyingUser"
    }
}

# spreadsheet_metadata: Queries spreadsheet to get basic information on spreadhsheet and sheets
SPREADSHEET_METADATA = {
    "api": "sheets",
    "path": "spreadsheets/{spreadsheet_id}",
    "key_properties": ["spreadsheetId"],
    "replication_method": "FULL_TABLE",
    "params": {
        "includeGridData": "false"
    }
}

# sheet_metadata: Get Header Row and 1st data row (Rows 1 & 2) from a Sheet on Spreadsheet.
# This endpoint includes detailed metadata about each cell in the header and first data row
#   incl. data type, formatting, etc.
SHEET_METADATA = {
    "api": "sheets",
    "path": "spreadsheets/{spreadsheet_id}",
    "key_properties": ["sheetId"],
    "replication_method": "FULL_TABLE",
    "params": {
        "includeGridData": "true",
        "ranges": "'{sheet_title}'!1:2"
    }
}

# sheets_loaded: Queries a batch of Rows for each Sheet in the Spreadsheet.
# Each query uses the `values` endpoint, to get data-only, w/out the formatting/type metadata.
SHEETS_LOADED = {
    "api": "sheets",
    "path": "spreadsheets/{spreadsheet_id}/values/'{sheet_title}'!{range_rows}",
    "data_key": "values",
    "key_properties": ["spreadsheetId", "sheetId", "loadDate"],
    "replication_method": "FULL_TABLE",
    "params": {
        "dateTimeRenderOption": "SERIAL_NUMBER",
        "valueRenderOption": "UNFORMATTED_VALUE",
        "majorDimension": "ROWS"
    }
}

# Ensure streams are ordered sequentially, logically.
STREAMS = OrderedDict()
STREAMS['file_metadata'] = FILE_METADATA
STREAMS['spreadsheet_metadata'] = SPREADSHEET_METADATA
STREAMS['sheet_metadata'] = SHEET_METADATA
STREAMS['sheets_loaded'] = SHEETS_LOADED
