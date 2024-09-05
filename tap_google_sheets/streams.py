import json
import os
import time
import re
import simplejson as json
from collections import OrderedDict
import urllib.parse
import singer
import decimal
from singer import metrics, metadata, Transformer, utils, messages
from singer.utils import strptime_to_utc, strftime
from singer.messages import RecordMessage
from singer.transform import SchemaKey
import tap_google_sheets.transform as internal_transform
import tap_google_sheets.schema as schema

LOGGER = singer.get_logger()

def update_currently_syncing(state, stream_name):
    """
    Currently syncing sets the stream currently being delivered in the state.
    If the integration is interrupted, this state property is used to identify
        the starting point to continue from.
    Reference: https://github.com/singer-io/singer-python/blob/master/singer/bookmarks.py#L41-L46
    """
    if (stream_name is None) and ("currently_syncing" in state):
        del state["currently_syncing"]
    else:
        singer.set_currently_syncing(state, stream_name)
    singer.write_state(state)

def write_schema(catalog, stream_name):
    """
    Write schema from the stream
    """
    stream = catalog.get_stream(stream_name)
    schema = stream.schema.to_dict()
    try:
        singer.write_schema(stream_name, schema, stream.key_properties)
        LOGGER.info('Writing schema for: {}'.format(stream_name))
    except OSError as err:
        LOGGER.info('OS Error writing schema for: {}'.format(stream_name))
        raise err

def write_record(stream_name, record, time_extracted, version=None):
    """
    Write records for the stream with extracted time
    """
    try:
        # use "write_message" if version is found as
        # "write_record" params does not contain "version"
        if version:
            singer.messages.write_message(
                RecordMessage(
                    stream=stream_name,
                    record=record,
                    version=version,
                    time_extracted=time_extracted))
        else:
            singer.messages.write_record(
                stream_name=stream_name,
                record=record,
                time_extracted=time_extracted)
    except OSError as err:
        LOGGER.info('OS Error writing record for: {}'.format(stream_name))
        raise err

def get_bookmark(state, stream, default):
    """
    Get bookmark for the stream
    """
    if (state is None) or ('bookmarks' not in state):
        return default
    return (
        state
        .get('bookmarks', {})
        .get(stream, default)
    )

def write_bookmark(state, stream, value):
    """
    Write bookmark for the stream
    """
    if 'bookmarks' not in state:
        state['bookmarks'] = {}
    state['bookmarks'][stream] = value
    LOGGER.info('Write state for stream: {}, value: {}'.format(stream, value))
    singer.write_state(state)

def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)

def get_selected_fields(catalog, stream_name):
    """
    Get the list of selected fields for a stream from the catalog
    """
    stream = catalog.get_stream(stream_name)
    mdata = metadata.to_map(stream.metadata)
    mdata_list = singer.metadata.to_list(mdata)
    selected_fields = []
    for entry in mdata_list:
        field = None
        try:
            field = entry['breadcrumb'][1]
            if entry.get('metadata', {}).get('selected', False):
                selected_fields.append(field)
        except IndexError:
            pass
    return selected_fields

def new_format_message(message):
    """To override the ensure_ascii param, overwitten this function"""
    return json.dumps(message.asdict(), ensure_ascii=False, use_decimal=True)

# To override the ensure_ascii param as while writing record the currency symbols were written as ascii values,
# overwitten this function of messages file of the singer module
messages.format_message = new_format_message

class GoogleSheets:
    stream_name = None
    api = None
    path = None
    key_properties = None
    replication_method = None
    replication_keys = None
    params = None
    state = None

    def __init__(self, client, spreadsheet_id, start_date=None):
        self.client = client
        self.config_start_date = start_date
        self.spreadsheet_id = spreadsheet_id

    def get_path(self, sheet_title_encoded=""):
        """
        return path and query string for API Call
        """
        # Add in querystring parameters and replace {placeholder} variables
        # querystring function ensures parameters are added but not encoded causing API errors
        # create querystring for preparing the request
        querystring = '&'.join(['%s=%s' % (key, value) for (key, value) in self.params.items()]).replace('{sheet_title}', sheet_title_encoded)
        # create path for preparing the request
        path = '{}?{}'.format(self.path.replace('{spreadsheet_id}', self.spreadsheet_id), querystring)
        # return path and query string
        return path, querystring

    def get_schemas(self):
        """
        return schema for streams
        """
        schemas = {}
        field_metadata = {}

        schema_path = get_abs_path('schemas/{}.json'.format(self.stream_name))
        with open(schema_path) as file:
            schema = json.load(file)
        schemas[self.stream_name] = schema
        mdata = metadata.new()

        # Documentation:
        # https://github.com/singer-io/getting-started/blob/master/docs/DISCOVERY_MODE.md#singer-python-helper-functions
        # Reference:
        # https://github.com/singer-io/singer-python/blob/master/singer/metadata.py#L25-L44
        mdata = metadata.get_standard_metadata(
            schema=schema,
            key_properties=self.key_properties,
            valid_replication_keys=self.replication_keys,
            replication_method=self.replication_method
        )
        field_metadata[self.stream_name] = mdata

        return schemas, field_metadata

    def process_records(self, catalog, stream_name, records, time_extracted, version=None):
        """
        Transform/validate batch of records with schema and sent to target
        """
        stream = catalog.get_stream(stream_name)
        schema = stream.schema.to_dict()
        stream_metadata = metadata.to_map(stream.metadata)
        with metrics.record_counter(stream_name) as counter:
            for record in records:
                # Transform record for Singer.io
                with Transformer() as transformer:
                    try:
                        transformed_record = transformer.transform(
                            record,
                            schema,
                            stream_metadata)
                    except Exception as err:
                        LOGGER.error('{}'.format(err))
                        raise RuntimeError(err)
                    write_record(
                        stream_name=stream_name,
                        record=transformed_record,
                        time_extracted=time_extracted,
                        version=version)
                    counter.increment()
            return counter.value

    def get_data(self, stream_name, range_rows=None):
        """
        Call API for the steram and return response
        """
        if not range_rows:
            range_rows = ''
        # Replace {placeholder} variables in path
        # Encode stream_name: fixes issue w/ special characters in sheet name
        stream_name_escaped = re.escape(stream_name)
        stream_name_encoded = urllib.parse.quote_plus(stream_name)
        path = self.path.replace(
            '{spreadsheet_id}', self.spreadsheet_id).replace('{sheet_title}', stream_name_encoded).replace(
                '{range_rows}', range_rows)
        api = self.api
        _, querystring = self.get_path(stream_name_encoded)
        LOGGER.info('URL: {}/{}?{}'.format(self.client.base_url, path, querystring))
        data = {}
        time_extracted = utils.now()
        data = self.client.get(
            path=path,
            api=api,
            params=querystring,
            endpoint=stream_name_escaped)
        return data, time_extracted

    def sync_stream(self, records, catalog, time_extracted=None):
        """
        sync stream and write records
        """
        # Should sheets_loaded be synced?
        LOGGER.info('STARTED Syncing {}'.format(self.stream_name))
        update_currently_syncing(self.state, self.stream_name)
        selected_fields = get_selected_fields(catalog, self.stream_name)
        LOGGER.info('Stream: {}, selected_fields: {}'.format(self.stream_name, selected_fields))
        write_schema(catalog, self.stream_name)
        if not time_extracted:
            time_extracted = utils.now()
        record_count = self.process_records(
            catalog=catalog,
            stream_name=self.stream_name,
            records=records,
            time_extracted=time_extracted)
        LOGGER.info('FINISHED Syncing {}, Total Records: {}'.format(self.stream_name, record_count))
        update_currently_syncing(self.state, None)

class SpreadSheetMetadata(GoogleSheets):
    stream_name = "spreadsheet_metadata"
    api = "sheets"
    path = "spreadsheets/{spreadsheet_id}"
    key_properties = ["spreadsheetId"]
    replication_method = "FULL_TABLE"
    params = {
        "includeGridData": "false"
    }

    def get_schemas(self):
        """
        Get schema for spreadsheet and generate schema for the sheets in the spreadsheet
        """
        # get schema of spreadsheet metadata
        schemas, field_metadata = super().get_schemas()

        # prepare schema for sheets in the spreadsheet
        api = self.api
        path, querystring = self.get_path()

        # GET spreadsheet_metadata, which incl. sheets (basic metadata for each worksheet)
        spreadsheet_md_results = self.client.get(path=path, params=querystring, api=api, endpoint=self.stream_name)

        sheets = spreadsheet_md_results.get('sheets')
        if sheets:
            # Loop thru each worksheet in spreadsheet
            for sheet in sheets:
                # GET sheet_json_schema for each worksheet (from function above)
                sheet_json_schema, columns = schema.get_sheet_metadata(sheet, self.spreadsheet_id, self.client)

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
                    # for each column check if the `columnSkipped` value is true and the `prior_column_skipped` is false or None
                    # in the columns dict. The `prior_column_skipped` would be true  when it is the first column of the two
                    # consecutive empty headers column if true: update the incusion property to `unsupported`
                    for column in columns:
                        if column.get('columnSkipped') and not column.get('prior_column_skipped'):
                            mdata = metadata.to_map(sheet_mdata)
                            sheet_mdata = metadata.write(mdata, ('properties', column.get('columnName')), 'inclusion', 'unsupported')
                            sheet_mdata = metadata.to_list(mdata)
                    field_metadata[sheet_title] = sheet_mdata

        return schemas, field_metadata

    def sync(self, catalog, state, spreadsheet_metadata, time_extracted):
        """
        sync spreadsheet's metadata
        """
        self.state = state
        LOGGER.info('GET spreadsheet_metadata')

        # Transform spreadsheet_metadata
        LOGGER.info('Transform spreadsheet_metadata')
        spreadsheet_metadata_transformed = internal_transform.transform_spreadsheet_metadata(spreadsheet_metadata)

        # Sync spreadsheet_metadata if selected
        self.sync_stream(spreadsheet_metadata_transformed, catalog, time_extracted)

def new_transform(self, data, typ, schema, path):
    '''The new _transform function to replace in the singer module'''
    if self.pre_hook:
        data = self.pre_hook(data, typ, schema)

    if typ == "null":
        if data is None or data == "":
            return True, None
        else:
            return False, None

    elif schema.get("format") == "date-time":
        data = self._transform_datetime(data)
        if data is None:
            return False, None

        return True, data
    elif schema.get("format") == "singer.decimal":
        if data is None:
            return False, None

        if isinstance(data, (str, float, int)):
            try:
                return True, str(decimal.Decimal(str(data)))
            except:
                return False, None
        elif isinstance(data, decimal.Decimal):
            try:
                if data.is_snan():
                    return True, 'NaN'
                else:
                    return True, str(data)
            except:
                return False, None

        return False, None
    elif typ == "object":
        # Objects do not necessarily specify properties
        return self._transform_object(data,
                                        schema.get("properties", {}),
                                        path,
                                        schema.get(SchemaKey.pattern_properties))

    elif typ == "array":
        return self._transform_array(data, schema["items"], path)

    elif typ == "string":
        if data is not None:
            try:
                return True, str(data)
            except:
                return False, None
        else:
            return False, None

    elif typ == "integer":
        if isinstance(data, str):
            data = data.replace(",", "")

        try:
            return True, int(data)
        except:
            return False, None

    elif typ == "number":
        if isinstance(data, str):
            data = data.replace(",", "")
        try:
            return True, float(data)
        except:
            return False, None

    elif typ == "boolean":
        if data is None: # returns "null" if data is none
            return True, None
        if isinstance(data, str):  # return the data as string itself if the value is of type string
            return True, data
        try:
            return True, bool(data)
        except:
            return False, None

    else:
        return False, None

# To cast the boolean values differently, overwriting this function of Transformer class of 
# the singer module
Transformer._transform = new_transform

class SheetsLoadData(GoogleSheets):
    api = "sheets"
    path = "spreadsheets/{spreadsheet_id}/values/'{sheet_title}'!{range_rows}"
    data_key = "values"
    key_properties = ["spreadsheetId", "sheetId", "loadDate"]
    replication_method = "FULL_TABLE"
    params = {}

    def load_data(self, catalog, state, selected_streams, sheets, spreadsheet_time_extracted):
        """
        Load sheet's records if that sheet is selected for sync
        """
        self.state = state
        sheet_metadata = []
        sheets_loaded = []
        if sheets:
            # Loop through sheets (worksheet tabs) in spreadsheet
            for sheet in sheets:
                sheet_title = sheet.get('properties', {}).get('title')
                sheet_id = sheet.get('properties', {}).get('sheetId')

                # GET sheet_metadata and columns
                sheet_schema, columns = schema.get_sheet_metadata(sheet, self.spreadsheet_id, self.client)
                # LOGGER.info('sheet_schema: {}'.format(sheet_schema))

                # SKIP empty sheets (where sheet_schema and columns are None)
                if not sheet_schema or not columns:
                    LOGGER.info('SKIPPING Empty Sheet: {}'.format(sheet_title))
                else:
                    # Transform sheet_metadata
                    sheet_metadata_transformed = internal_transform.transform_sheet_metadata(self.spreadsheet_id, sheet, columns)
                    # LOGGER.info('sheet_metadata_transformed = {}'.format(sheet_metadata_transformed))
                    sheet_metadata.append(sheet_metadata_transformed)

                    # SHEET_DATA
                    # Should this worksheet tab be synced?
                    if sheet_title in selected_streams:
                        LOGGER.info('STARTED Syncing Sheet {}'.format(sheet_title))
                        update_currently_syncing(self.state, sheet_title)
                        selected_fields = get_selected_fields(catalog, sheet_title) # --------------------
                        LOGGER.info('Stream: {}, selected_fields: {}'.format(sheet_title, selected_fields))
                        write_schema(catalog, sheet_title)

                        # Emit a Singer ACTIVATE_VERSION message before initial sync (but not subsequent syncs)
                        # everytime after each sheet sync is complete.
                        # This forces hard deletes on the data downstream if fewer records are sent.
                        # https://github.com/singer-io/singer-python/blob/master/singer/messages.py#L137
                        last_integer = int(get_bookmark(self.state, sheet_title, 0))
                        activate_version = int(time.time() * 1000)
                        activate_version_message = singer.ActivateVersionMessage(
                                stream=sheet_title,
                                version=activate_version)
                        if last_integer == 0:
                            # initial load, send activate_version before AND after data sync
                            singer.write_message(activate_version_message)
                            LOGGER.info('INITIAL SYNC, Stream: {}, Activate Version: {}'.format(sheet_title, activate_version))

                        # Determine max range of columns and rows for "paging" through the data
                        sheet_last_col_index = 1
                        sheet_last_col_letter = 'A'
                        for col in columns:
                            col_index = col.get('columnIndex')
                            col_letter = col.get('columnLetter')
                            if col_index > sheet_last_col_index:
                                sheet_last_col_index = col_index
                                sheet_last_col_letter = col_letter
                        sheet_max_row = sheet.get('properties').get('gridProperties', {}).get('rowCount')

                        # Initialize paging for 1st batch
                        is_last_row = False
                        batch_rows = 200
                        from_row = 2
                        if sheet_max_row < batch_rows:
                            to_row = sheet_max_row
                        else:
                            to_row = batch_rows

                        # Loop thru batches (each having 200 rows of data)
                        while not is_last_row and from_row < sheet_max_row and to_row <= sheet_max_row:
                            range_rows = 'A{}:{}{}'.format(from_row, sheet_last_col_letter, to_row)

                            self.params = {
                                "dateTimeRenderOption": "SERIAL_NUMBER",
                                "valueRenderOption": "FORMATTED_VALUE",
                                "majorDimension": "ROWS"
                            }
                            # GET sheet_data for a worksheet tab
                            sheet_data, time_extracted = self.get_data(stream_name=sheet_title, range_rows=range_rows)
                            # Data is returned as a list of arrays, an array of values for each row
                            sheet_data_rows = sheet_data.get('values', [])
                            self.params = {
                                "dateTimeRenderOption": "SERIAL_NUMBER",
                                "valueRenderOption": "UNFORMATTED_VALUE",
                                "majorDimension": "ROWS"
                            }
                            unformatted_sheet_data, _ = self.get_data(stream_name=sheet_title, range_rows=range_rows)
                            unformatted_sheet_data_rows = unformatted_sheet_data.get('values', [])

                            # Transform batch of rows to JSON with keys for each column
                            sheet_data_transformed, row_num = internal_transform.transform_sheet_data(
                                spreadsheet_id=self.spreadsheet_id,
                                sheet_id=sheet_id,
                                sheet_title=sheet_title,
                                from_row=from_row,
                                columns=columns,
                                sheet_data_rows=sheet_data_rows, 
                                unformatted_rows = unformatted_sheet_data_rows)

                            # Here row_num is the addition of from_row and total records get in response(per batch).
                            # Condition row_num < to_row was checking that if records on the current page are less than expected(to_row) or not.
                            # If the condition returns true then it was breaking the loop.
                            # API does not return the last empty rows in response.
                            # For example, rows 199 and 200 are empty, and a total of 400 rows are there in the sheet. So, in 1st iteration,
                            # to_row = 200, from_row = 2, row_num = 2(from_row) + 197 = 199(1st row contain header value)
                            # So, the above condition become true and breaks the loop without syncing records from 201 to 400.
                            # sheet_data_rows is no of records return in the current page. If it's a whole blank page then stop looping.
                            # So, in the above case, it syncs records 201 to 400 also even if rows 199 and 200 are blank.
                            # Then when the next batch 401 to 600 is empty, it breaks the loop.
                            if not sheet_data_rows: # If a whole blank page found, then stop looping.
                                is_last_row = True

                            # Process records, send batch of records to target
                            record_count = self.process_records(
                                catalog=catalog,
                                stream_name=sheet_title,
                                records=sheet_data_transformed,
                                time_extracted=spreadsheet_time_extracted,
                                version=activate_version)
                            LOGGER.info('Sheet: {}, records processed: {}'.format(
                                sheet_title, record_count))

                            # Update paging from/to_row for next batch
                            from_row = to_row + 1
                            if to_row + batch_rows > sheet_max_row:
                                to_row = sheet_max_row
                            else:
                                to_row = to_row + batch_rows

                        # End of Stream: Send Activate Version and update State
                        singer.write_message(activate_version_message)
                        write_bookmark(self.state, sheet_title, activate_version)
                        LOGGER.info('COMPLETE SYNC, Stream: {}, Activate Version: {}'.format(sheet_title, activate_version))
                        LOGGER.info('FINISHED Syncing Sheet {}, Total Rows: {}'.format(
                            sheet_title, row_num - 2)) # subtract 1 for header row
                        update_currently_syncing(self.state, None)

                        # SHEETS_LOADED
                        # Add sheet to sheets_loaded
                        sheet_loaded = {}
                        sheet_loaded['spreadsheetId'] = self.spreadsheet_id
                        sheet_loaded['sheetId'] = sheet_id
                        sheet_loaded['title'] = sheet_title
                        sheet_loaded['loadDate'] = strftime(utils.now())
                        sheet_loaded['lastRowNumber'] = row_num
                        sheets_loaded.append(sheet_loaded)

        return sheet_metadata, sheets_loaded

class SheetMetadata(GoogleSheets):
    stream_name = "sheet_metadata"
    api = "sheets"
    path = "spreadsheets/{spreadsheet_id}"
    key_properties = ["sheetId"]
    replication_method = "FULL_TABLE"
    params = {
        "includeGridData": "true",
        "ranges": "'{sheet_title}'!1:2"
    }

    def sync(self, catalog, state, sheet_metadata_records):
        """
        Write sheet's metadata records
        """
        self.state = state
        self.sync_stream(sheet_metadata_records, catalog)

class SheetsLoaded(GoogleSheets):
    stream_name = "sheets_loaded"
    api = "sheets"
    path = "spreadsheets/{spreadsheet_id}/values/'{sheet_title}'!{range_rows}"
    data_key = "values"
    key_properties = ["spreadsheetId", "sheetId", "loadDate"]
    replication_method = "FULL_TABLE"
    params = {
        "dateTimeRenderOption": "SERIAL_NUMBER",
        "valueRenderOption": "UNFORMATTED_VALUE",
        "majorDimension": "ROWS"
    }

    def sync(self, catalog, state, sheets_loaded_records):
        """
        Write sheets loaded records
        """
        self.state = state
        self.sync_stream(sheets_loaded_records, catalog)


# create OrderDict, as the order matters for syncing the streams
# "spreadsheet_metadata" -> get sheets in the spreadsheet and load sheet's records
#       and prepare records for "sheet_metadata" and "sheets_loaded" streams
STREAMS = OrderedDict()
STREAMS['spreadsheet_metadata'] = SpreadSheetMetadata
STREAMS['sheet_metadata'] = SheetMetadata
STREAMS['sheets_loaded'] = SheetsLoaded
