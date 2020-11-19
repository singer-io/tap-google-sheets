import os
import json
import re
import urllib.parse
from collections import OrderedDict
import singer
from singer import metadata
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


# Create sheet_metadata_json with columns from sheet
def get_sheet_schema_columns(sheet):
    sheet_title = sheet.get('properties', {}).get('title')
    sheet_json_schema = OrderedDict()
    data = next(iter(sheet.get('data', [])), {})
    row_data = data.get('rowData', [])
    if row_data == []:
        # Empty sheet, SKIP
        LOGGER.info('SKIPPING Empty Sheet: {}'.format(sheet_title))
        return None, None
    # spreadsheet is an OrderedDict, with orderd sheets and rows in the repsonse
    headers = row_data[0].get('values', [])
    first_values = row_data[1].get('values', [])
    # LOGGER.info('first_values = {}'.format(json.dumps(first_values, indent=2, sort_keys=True)))

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
    # Read column headers until end or 2 consecutive skipped headers
    for header in headers:
        # LOGGER.info('header = {}'.format(json.dumps(header, indent=2, sort_keys=True)))
        column_index = i + 1
        column_letter = colnum_string(column_index)
        header_value = header.get('formattedValue')
        if header_value: # NOT skipped
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
                        raise Exception('DATA TYPE ERROR 2ND ROW VALUE: SHEET: {}, COL: {}, CELL: {}2, TYPE: {}, VALUE: {}'.format(
                            sheet_title, column_name, column_letter, key, col_val))

            column_number_format = first_values[i].get('effectiveFormat', {}).get(
                'numberFormat', {})
            column_number_format_type = column_number_format.get('type')

            # Determine datatype for sheet_json_schema
            #
            # column_effective_value_type = numberValue, stringValue, boolValue;
            #  INVALID: errorType, formulaType
            #  https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/other#ExtendedValue
            #
            # column_number_format_type = UNEPECIFIED, TEXT, NUMBER, PERCENT, CURRENCY, DATE,
            #   TIME, DATE_TIME, SCIENTIFIC
            #  https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/cells#NumberFormatType
            #
            column_format = None # Default
            if column_effective_value == {}:
                col_properties = {'type': ['null', 'string']}
                column_gs_type = 'stringValue'
                LOGGER.info('WARNING: 2ND ROW VALUE IS BLANK: SHEET: {}, COL: {}, CELL: {}2'.format(
                        sheet_title, column_name, column_letter))
                LOGGER.info('   Setting column datatype to STRING')
            elif column_effective_value_type == 'stringValue':
                col_properties = {'type': ['null', 'string']}
                column_gs_type = 'stringValue'
            elif column_effective_value_type == 'boolValue':
                col_properties = {'type': ['null', 'boolean', 'string']}
                column_gs_type = 'boolValue'
            elif column_effective_value_type == 'numberValue':
                if column_number_format_type == 'DATE_TIME':
                    col_properties = {
                        'type': ['null', 'string'],
                        'format': 'date-time'
                    }
                    column_gs_type = 'numberType.DATE_TIME'
                elif column_number_format_type == 'DATE':
                    col_properties = {
                        'type': ['null', 'string'],
                        'format': 'date'
                    }
                    column_gs_type = 'numberType.DATE'
                elif column_number_format_type == 'TIME':
                    col_properties = {
                        'type': ['null', 'string'],
                        'format': 'time'
                    }
                    column_gs_type = 'numberType.TIME'
                elif column_number_format_type == 'TEXT':
                    col_properties = {'type': ['null', 'string']}
                    column_gs_type = 'stringValue'
                else:
                    # Interesting - order in the anyOf makes a difference.
                    # Number w/ multipleOf must be listed last, otherwise errors occur.
                    col_properties =  {
                        'anyOf': [
                            {
                                'type': 'null'
                            },
                            {
                                'type': 'number',
                                'multipleOf': 1e-15
                            },
                            {
                                'type': 'string'
                            }
                        ]
                    }
                    column_gs_type = 'numberType'
            # Catch-all to deal with other types and set to string
            # column_effective_value_type: formulaValue, errorValue, or other
            else:
                col_properties = {'type': ['null', 'string']}
                column_gs_type = 'unsupportedValue'
                LOGGER.info('WARNING: UNSUPPORTED 2ND ROW VALUE: SHEET: {}, COL: {}, CELL: {}2, TYPE: {}, VALUE: {}'.format(
                        sheet_title, column_name, column_letter, column_effective_value_type, col_val))
                LOGGER.info('Converting to string.')
        else: # skipped
            column_is_skipped = True
            skipped = skipped + 1
            column_index_str = str(column_index).zfill(2)
            column_name = '__sdc_skip_col_{}'.format(column_index_str)
            col_properties = {'type': ['null', 'string']}
            column_gs_type = 'stringValue'
            LOGGER.info('WARNING: SKIPPED COLUMN; NO COLUMN HEADER. SHEET: {}, COL: {}, CELL: {}1'.format(
                sheet_title, column_name, column_letter))
            LOGGER.info('  This column will be skipped during data loading.')

        if skipped >= 2:
            # skipped = 2 consecutive skipped headers
            # Remove prior_header column_name
            sheet_json_schema['properties'].pop(prior_header, None)
            LOGGER.info('TWO CONSECUTIVE SKIPPED COLUMNS. STOPPING SCAN AT: SHEET: {}, COL: {}, CELL {}1'.format(
                sheet_title, column_name, column_letter))
            break

        else:
            column = {}
            column = {
                'columnIndex': column_index,
                'columnLetter': column_letter,
                'columnName': column_name,
                'columnType': column_gs_type,
                'columnSkipped': column_is_skipped
            }
            columns.append(column)

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
    stream_metadata = STREAMS.get(stream_name)
    params = stream_metadata.get('params', {})

    # GET sheet_metadata
    sheet_md_results = client.request(endpoint=stream_name,
                                      spreadsheet_id=spreadsheet_id,
                                      sheet_title=sheet_title,
                                      params=params)
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


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)

def get_schemas(client, spreadsheet_id):
    schemas = {}
    field_metadata = {}

    for stream_name, stream_metadata in STREAMS.items():
        schema_path = get_abs_path('schemas/{}.json'.format(stream_name))
        with open(schema_path) as file:
            schema = json.load(file)
        schemas[stream_name] = schema
        mdata = metadata.new()

        # Documentation:
        # https://github.com/singer-io/getting-started/blob/master/docs/DISCOVERY_MODE.md#singer-python-helper-functions
        # Reference:
        # https://github.com/singer-io/singer-python/blob/master/singer/metadata.py#L25-L44
        mdata = metadata.get_standard_metadata(
            schema=schema,
            key_properties=stream_metadata.get('key_properties', None),
            valid_replication_keys=stream_metadata.get('replication_keys', None),
            replication_method=stream_metadata.get('replication_method', None)
        )
        field_metadata[stream_name] = mdata

        if stream_name == 'spreadsheet_metadata':
            params = stream_metadata.get('params', {})

            # GET spreadsheet_metadata, which incl. sheets (basic metadata for each worksheet)
            spreadsheet_md_results = client.request(endpoint=stream_name,
                                                    spreadsheet_id=spreadsheet_id,
                                                    params=params)

            sheets = spreadsheet_md_results.get('sheets')
            if sheets:
                # Loop thru each worksheet in spreadsheet
                for sheet in sheets:
                    # GET sheet_json_schema for each worksheet (from function above)
                    sheet_json_schema, columns = get_sheet_metadata(sheet, spreadsheet_id, client)

                    # SKIP empty sheets (where sheet_json_schema and columns are None)
                    if sheet_json_schema and columns:
                        sheet_title = sheet.get('properties', {}).get('title')
                        schemas[sheet_title] = sheet_json_schema
                        sheet_mdata = metadata.new()
                        sheet_mdata = metadata.get_standard_metadata(
                            schema=sheet_json_schema,
                            key_properties=['__sdc_row'],
                            valid_replication_keys=None,
                            replication_method='FULL_TABLE'
                        )
                        field_metadata[sheet_title] = sheet_mdata

    return schemas, field_metadata
