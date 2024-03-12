import re
import urllib.parse
from collections import OrderedDict
import singer
from tap_google_sheets.streams import STREAMS

LOGGER = singer.get_logger()

# Reference:
# https://github.com/singer-io/getting-started/blob/master/docs/DISCOVERY_MODE.md#Metadata

# Convert column index to column letter
def colnum_string(num):
    string = ""
    while num > 0:
        num, remainder = divmod(num - 1, 26)
        string = chr(65 + remainder) + string
    return string


def pad_default_effective_values(headers, first_values):
    for i in range(len(headers) - len(first_values)):
        first_values.append(OrderedDict())


# Create sheet_metadata_json with columns from sheet
def get_sheet_schema_columns(sheet):
    sheet_title = sheet.get('properties', {}).get('title')
    sheet_json_schema = OrderedDict()
    data = next(iter(sheet.get('data', [])), {})
    row_data = data.get('rowData', [])
    if row_data == [] or len(row_data) == 1:
        # Empty sheet or empty first row, SKIP
        LOGGER.info('SKIPPING Empty Sheet: {}'.format(sheet_title))
        return None, None

    # spreadsheet is an OrderedDict, with orderd sheets and rows in the repsonse
    headers = row_data[0].get('values', [])
    first_values = row_data[1].get('values', [])
    # Pad first row values with default if null
    if len(first_values) < len(headers):
        pad_default_effective_values(headers, first_values)

    sheet_json_schema = {
        'type': 'object',
        'additionalProperties': False,
        'properties': {
            '__sdc_spreadsheet_id': {
                'type': ['null', 'string']
            },
            '__sdc_sheet_id': {
                'type': ['null', 'integer']
            },
            '__sdc_row': {
                'type': ['null', 'integer']
            }
        }
    }

    header_list = [] # used for checking uniqueness
    columns = []
    prior_header = None
    i = 0
    skipped = 0

    # if no headers are present, log the message that sheet is skipped
    if not headers:
        LOGGER.warning('SKIPPING THE SHEET AS HEADERS ROW IS EMPTY. SHEET: {}'.format(sheet_title))

    # Read column headers until end or 2 consecutive skipped headers
    for header in headers:
        # LOGGER.info('header = {}'.format(json.dumps(header, indent=2, sort_keys=True)))
        column_index = i + 1
        column_letter = colnum_string(column_index)
        header_value = header.get('formattedValue')
        if header_value: # if the column is NOT to be skipped
            column_is_skipped = False
            skipped = 0
            column_name = '{}'.format(header_value)
            if column_name in header_list:
                raise Exception('DUPLICATE HEADER ERROR: SHEET: {}, COL: {}, CELL: {}1'.format(
                    sheet_title, column_name, column_letter))
            header_list.append(column_name)

            first_value = None
            try:
                first_value = first_values[i]
            except IndexError as err:
                LOGGER.info('NO VALUE IN 2ND ROW FOR HEADER. SHEET: {}, COL: {}, CELL: {}2. {}'.format(
                    sheet_title, column_name, column_letter, err))
                first_value = {}
                first_values.append(first_value)
                pass

            column_effective_value = first_value.get('effectiveValue', {})

            col_val = None
            if column_effective_value == {}:
                if ("numberFormat" in first_value.get('effectiveFormat', {})):
                    column_effective_value_type = "numberValue"
                else:
                    column_effective_value_type = 'stringValue'
                    LOGGER.info('WARNING: NO VALUE IN 2ND ROW FOR HEADER. SHEET: {}, COL: {}, CELL: {}2.'.format(
                        sheet_title, column_name, column_letter))
                    LOGGER.info('   Setting column datatype to STRING')
            else:
                for key, val in column_effective_value.items():
                    if key in ('numberValue', 'stringValue', 'boolValue'):
                        column_effective_value_type = key
                        col_val = str(val)
                    elif key in ('errorType', 'formulaType'):
                        col_val = str(val)
                        raise Exception('DATA TYPE ERROR 2ND ROW VALUE: SHEET: {}, COL: {}, CELL: {}2, TYPE: {}'.format(
                            sheet_title, column_name, column_letter, key))

            column_number_format = first_values[i].get('effectiveFormat', {}).get(
                'numberFormat', {})
            column_number_format_type = column_number_format.get('type')

            # Determine datatype for sheet_json_schema
            #
            # column_effective_value_type = numberValue, stringValue, boolValue;
            #  INVALID: errorType, formulaType
            #  https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/other#ExtendedValue
            #
            # column_number_format_type = UNEPECIFIED, TEXT, NUMBER, PERCENT, CURRENCY,
            #   TIME, DATE_TIME, SCIENTIFIC
            #  https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/cells#NumberFormatType
            #
            column_format = None # Default

            if column_effective_value_type == 'stringValue':
                col_properties = {'type': ['null', 'string']}
                column_gs_type = 'stringValue'
            elif column_effective_value_type == 'boolValue':
                col_properties = {'type': ['null', 'boolean', 'string']}
                column_gs_type = 'boolValue'
            elif column_effective_value_type == 'numberValue':
                if column_number_format_type in ['DATE_TIME', 'DATE']:
                    col_properties = {
                        'type': ['null', 'string'],
                        'format': 'date-time'
                    }
                    column_gs_type = 'numberType.DATE_TIME'
                elif column_number_format_type == 'TIME':
                    col_properties = {
                        'type': ['null', 'string'],
                        'format': 'time'
                    }
                    column_gs_type = 'numberType.TIME'
                elif column_number_format_type == 'TEXT':
                    col_properties = {'type': ['null', 'string']}
                    column_gs_type = 'stringValue'
                elif column_number_format_type == 'CURRENCY':
                    col_properties = {'type': ['null', 'string']}
                    column_gs_type = 'stringValue'
                else:
                    # Interesting - order in the anyOf makes a difference.
                    # Number w/ singer.decimal must be listed last, otherwise errors occur.
                    col_properties = {
                        'type': ['null', 'string'],
                        'format': 'singer.decimal'
                    }
                    column_gs_type = 'numberType'
            # Catch-all to deal with other types and set to string
            # column_effective_value_type: formulaValue, errorValue, or other
            else:
                col_properties = {'type': ['null', 'string']}
                column_gs_type = 'unsupportedValue'
                LOGGER.info('WARNING: UNSUPPORTED 2ND ROW VALUE: SHEET: {}, COL: {}, CELL: {}2, TYPE: {}'.format(
                        sheet_title, column_name, column_letter, column_effective_value_type))
                LOGGER.info('Converting to string.')
        else: # if the column is to be skipped
            column_is_skipped = True
            skipped = skipped + 1
            column_index_str = str(column_index).zfill(2)
            column_name = '__sdc_skip_col_{}'.format(column_index_str)
            # unsupported field description if the field is to be skipped
            col_properties = {'type': ['null', 'string'], 'description': 'Column is unsupported and would be skipped because header is not available'}
            column_gs_type = 'stringValue'
            LOGGER.info('WARNING: SKIPPED COLUMN; NO COLUMN HEADER. SHEET: {}, COL: {}, CELL: {}1'.format(
                sheet_title, column_name, column_letter))
            LOGGER.info('  This column will be skipped during data loading.')

        if skipped >= 2:
            # skipped = 2 consecutive skipped headers
            # Remove prior_header column_name
            # stop scanning the sheet and break
            sheet_json_schema['properties'].pop(prior_header, None)
            # prior index is the index of the column prior to the currently column
            prior_index = column_index - 1
            # added a new boolean key `prior_column_skipped` to check if the column is one of the two columns with consecutive headers 
            # as due to consecutive empty headers both the columns should not be included in the schema as well as the metadata
            columns[prior_index-1]['prior_column_skipped'] = True
            LOGGER.info('TWO CONSECUTIVE SKIPPED COLUMNS. STOPPING SCAN AT: SHEET: {}, COL: {}, CELL {}1'.format(
                sheet_title, column_name, column_letter))
            break

        else:
            # skipped < 2 prepare `columns` dictionary with index, letter, column name, column type and 
            # if the column is to be skipped or not for each column in the list
            column = {}
            column = {
                'columnIndex': column_index,
                'columnLetter': column_letter,
                'columnName': column_name,
                'columnType': column_gs_type,
                'columnSkipped': column_is_skipped
            }
            columns.append(column)

            if column_gs_type in {'numberType.DATE_TIME', 'numberType.TIME', 'numberType'}:
                col_properties = {
                    'anyOf': [
                        col_properties,
                        {'type': ['null', 'string']} # all the time has string types in schema
                    ]
                }
            # add the column properties in the `properties` in json schema for the respective column name
            sheet_json_schema['properties'][column_name] = col_properties

        prior_header = column_name
        i = i + 1

    return sheet_json_schema, columns


# Get Header Row and 1st data row (Rows 1 & 2) from a Sheet on Spreadsheet w/ sheet_metadata query
#   endpoint: spreadsheets/{spreadsheet_id}
#   params: includeGridData = true, ranges = '{sheet_title}'!1:2
# This endpoint includes detailed metadata about each cell - incl. data type, formatting, etc.
def get_sheet_metadata(sheet, spreadsheet_id, client):
    sheet_id = sheet.get('properties', {}).get('sheetId')
    sheet_title = sheet.get('properties', {}).get('title')
    LOGGER.info('sheet_id = {}, sheet_title = {}'.format(sheet_id, sheet_title))

    stream_name = 'sheet_metadata'
    stream_obj = STREAMS.get(stream_name)(client, spreadsheet_id)
    api = stream_obj.api
    sheet_title_encoded = urllib.parse.quote_plus(sheet_title)
    sheet_title_escaped = re.escape(sheet_title)
    path, _ = stream_obj.get_path(sheet_title_encoded)

    sheet_md_results = client.get(path=path, api=api, endpoint=sheet_title_escaped)
    # sheet_metadata: 1st `sheets` node in results
    sheet_metadata = sheet_md_results.get('sheets')[0]

    # Create sheet_json_schema (for discovery/catalog) and columns (for sheet_metadata results)
    try:
        sheet_json_schema, columns = get_sheet_schema_columns(sheet_metadata)
    except Exception as err:
        LOGGER.warning('{}'.format(err))
        LOGGER.warning('SKIPPING Malformed sheet: {}'.format(sheet_title))
        sheet_json_schema, columns = None, None

    return sheet_json_schema, columns
