import time
import math
import singer
import json
from collections import OrderedDict
from singer import metrics, metadata, Transformer, utils
from singer.utils import strptime_to_utc, strftime
from tap_google_sheets.streams import STREAMS
from tap_google_sheets.schema import get_sheet_metadata

LOGGER = singer.get_logger()


def write_schema(catalog, stream_name):
    stream = catalog.get_stream(stream_name)
    schema = stream.schema.to_dict()
    try:
        singer.write_schema(stream_name, schema, stream.key_properties)
    except OSError as err:
        LOGGER.info('OS Error writing schema for: {}'.format(stream_name))
        raise err


def write_record(stream_name, record, time_extracted):
    try:
        singer.messages.write_record(stream_name, record, time_extracted=time_extracted)
    except OSError as err:
        LOGGER.info('OS Error writing record for: {}'.format(stream_name))
        LOGGER.info('record: {}'.format(record))
        raise err


def get_bookmark(state, stream, default):
    if (state is None) or ('bookmarks' not in state):
        return default
    return (
        state
        .get('bookmarks', {})
        .get(stream, default)
    )


def write_bookmark(state, stream, value):
    if 'bookmarks' not in state:
        state['bookmarks'] = {}
    state['bookmarks'][stream] = value
    LOGGER.info('Write state for stream: {}, value: {}'.format(stream, value))
    singer.write_state(state)


# def transform_datetime(this_dttm):
def transform_datetime(this_dttm):
    with Transformer() as transformer:
        new_dttm = transformer._transform_datetime(this_dttm)
    return new_dttm


def process_records(catalog, #pylint: disable=too-many-branches
                    stream_name,
                    records,
                    time_extracted,
                    bookmark_field=None,
                    bookmark_type=None,
                    max_bookmark_value=None,
                    last_datetime=None,
                    last_integer=None,
                    parent=None,
                    parent_id=None):
    stream = catalog.get_stream(stream_name)
    schema = stream.schema.to_dict()
    stream_metadata = metadata.to_map(stream.metadata)

    with metrics.record_counter(stream_name) as counter:
        for record in records:
            # If child object, add parent_id to record
            if parent_id and parent:
                record[parent + '_id'] = parent_id

            # Transform record for Singer.io
            with Transformer() as transformer:
                transformed_record = transformer.transform(
                    record,
                    schema,
                    stream_metadata)
                # Reset max_bookmark_value to new value if higher
                if transformed_record.get(bookmark_field):
                    if max_bookmark_value is None or \
                        transformed_record[bookmark_field] > transform_datetime(max_bookmark_value):
                        max_bookmark_value = transformed_record[bookmark_field]

                if bookmark_field and (bookmark_field in transformed_record):
                    if bookmark_type == 'integer':
                        # Keep only records whose bookmark is after the last_integer
                        if transformed_record[bookmark_field] >= last_integer:
                            write_record(stream_name, transformed_record, \
                                time_extracted=time_extracted)
                            counter.increment()
                    elif bookmark_type == 'datetime':
                        last_dttm = transform_datetime(last_datetime)
                        bookmark_dttm = transform_datetime(transformed_record[bookmark_field])
                        # Keep only records whose bookmark is after the last_datetime
                        if bookmark_dttm >= last_dttm:
                            write_record(stream_name, transformed_record, \
                                time_extracted=time_extracted)
                            counter.increment()
                else:
                    write_record(stream_name, transformed_record, time_extracted=time_extracted)
                    counter.increment()

        return max_bookmark_value, counter.value


# Currently syncing sets the stream currently being delivered in the state.
# If the integration is interrupted, this state property is used to identify
#  the starting point to continue from.
# Reference: https://github.com/singer-io/singer-python/blob/master/singer/bookmarks.py#L41-L46
def update_currently_syncing(state, stream_name):
    if (stream_name is None) and ('currently_syncing' in state):
        del state['currently_syncing']
    else:
        singer.set_currently_syncing(state, stream_name)
    singer.write_state(state)


# List selected fields from stream catalog
def get_selected_fields(catalog, stream_name):
    stream = catalog.get_stream(stream_name)
    mdata = metadata.to_map(stream.metadata)
    mdata_list = singer.metadata.to_list(mdata)
    selected_fields = []
    for entry in mdata_list:
        field =  None
        try:
            field =  entry['breadcrumb'][1]
            if entry.get('metadata', {}).get('selected', False):
                selected_fields.append(field)
        except IndexError:
            pass
    return selected_fields


def get_data(stream_name,
             endpoint_config,
             client,
             spreadsheet_id,
             range_rows=None):
    if not range_rows:
        range_rows = ''
    path = endpoint_config.get('path', stream_name).replace(
        '{spreadsheet_id}', spreadsheet_id).replace('{sheet_title}', stream_name).replace(
            '{range_rows}', range_rows)
    params = endpoint_config.get('params', {})
    api = endpoint_config.get('api', 'sheets')
    querystring = '&'.join(['%s=%s' % (key, value) for (key, value) in params.items()]).replace(
        '{sheet_title}', stream_name)
    data = {}
    data = client.get(
        path=path,
        api=api,
        params=querystring,
        endpoint=stream_name)
    return data


def transform_file_metadata(file_metadata):
    # Convert to dict
    file_metadata_tf = json.loads(json.dumps(file_metadata))
    # Remove keys
    if file_metadata_tf.get('lastModifyingUser'):
        file_metadata_tf['lastModifyingUser'].pop('photoLink', None)
        file_metadata_tf['lastModifyingUser'].pop('me', None)
        file_metadata_tf['lastModifyingUser'].pop('permissionId', None)
    # Add record to an array of 1
    file_metadata_arr = []
    file_metadata_arr.append(file_metadata_tf)
    return file_metadata_arr


def transform_spreadsheet_metadata(spreadsheet_metadata):
    # Convert to dict
    spreadsheet_metadata_tf = json.loads(json.dumps(spreadsheet_metadata))
    # Remove keys
    if spreadsheet_metadata_tf.get('properties'):
        spreadsheet_metadata_tf['properties'].pop('defaultFormat', None)
    spreadsheet_metadata_tf.pop('sheets', None)
    # Add record to an array of 1
    spreadsheet_metadata_arr = []
    spreadsheet_metadata_arr.append(spreadsheet_metadata_tf)
    return spreadsheet_metadata_arr


def transform_sheet_metadata(spreadsheet_id, sheet, columns):
    # Convert to properties to dict
    sheet_metadata = sheet.get('properties')
    sheet_metadata_tf = json.loads(json.dumps(sheet_metadata)) 
    sheet_id = sheet_metadata_tf.get('sheetId')
    sheet_url = 'https://docs.google.com/spreadsheets/d/{}/edit#gid={}'.format(
        spreadsheet_id, sheet_id)
    sheet_metadata_tf['spreadsheetId'] = spreadsheet_id
    sheet_metadata_tf['sheetUrl'] = sheet_url
    sheet_metadata_tf['columns'] = columns
    return sheet_metadata_tf


def sync(client, config, catalog, state):
    start_date = config.get('start_date')
    spreadsheet_id = config.get('spreadsheet_id')

    # Get selected_streams from catalog, based on state last_stream
    #   last_stream = Previous currently synced stream, if the load was interrupted
    last_stream = singer.get_currently_syncing(state)
    LOGGER.info('last/currently syncing stream: {}'.format(last_stream))
    selected_streams = []
    for stream in catalog.get_selected_streams(state):
        selected_streams.append(stream.stream)
    LOGGER.info('selected_streams: {}'.format(selected_streams))

    if not selected_streams:
        return

    # Get file_metadata
    file_metadata = {}
    file_metadata_config = STREAMS.get('file_metadata')
    file_metadata = get_data('file_metadata', file_metadata_config, client, spreadsheet_id)
    file_metadata_tf = transform_file_metadata(file_metadata)
    # LOGGER.info('file_metadata_tf = {}'.format(file_metadata_tf))
    last_datetime = strptime_to_utc(get_bookmark(state, 'file_metadata', start_date))
    this_datetime = strptime_to_utc(file_metadata.get('modifiedTime'))
    LOGGER.info('last_datetime = {}, this_datetime = {}'.format(last_datetime, this_datetime))
    if this_datetime <= last_datetime:
        LOGGER.info('this_datetime <= last_datetime, FILE NOT CHANGED. EXITING.')
        return 0
    
    # Get spreadsheet_metadata
    spreadsheet_metadata = {}
    spreadsheet_metadata_config = STREAMS.get('spreadsheet_metadata')
    spreadsheet_metadata = get_data('spreadsheet_metadata', spreadsheet_metadata_config, client, spreadsheet_id)
    spreadsheet_metadata_tf = transform_spreadsheet_metadata(spreadsheet_metadata)
    # LOGGER.info('spreadsheet_metadata_tf = {}'.format(spreadsheet_metadata_tf))

    # Get sheet_metadata
    sheets = spreadsheet_metadata.get('sheets')
    sheet_metadata = []
    sheets_loaded = []
    sheets_loaded_config = STREAMS['sheets_loaded']
    if sheets:
        for sheet in sheets:
            sheet_title = sheet.get('properties', {}).get('title')
            sheet_schema, columns = get_sheet_metadata(sheet, spreadsheet_id, client)
            sheet_metadata_tf = transform_sheet_metadata(spreadsheet_id, sheet, columns)
            # LOGGER.info('sheet_metadata_tf = {}'.format(sheet_metadata_tf))
            sheet_metadata.append(sheet_metadata_tf)

            # Determine range of rows and columns for "paging" through batch rows of data
            sheet_last_col_index = 1
            sheet_last_col_letter = 'A'
            for col in columns:
                col_index = col.get('columnIndex')
                col_letter = col.get('columnLetter')
                if col_index > sheet_last_col_index:
                    sheet_last_col_index = col_index
                    sheet_last_col_letter = col_letter
            sheet_max_row = sheet.get('gridProperties', {}).get('rowCount')
            is_empty_row = False
            batch_rows = 200
            from_row = 2
            if sheet_max_row < batch_rows:
                to_row = sheet_max_row
            else:
                to_row = batch_rows

            while not is_empty_row and to_row <= sheet_max_row:
                range_rows = 'A2:{}{}'.format(sheet_last_col_letter, to_row)
                
                sheet_data = get_data(
                    stream_name=sheet_title,
                    endpoint_config=sheets_loaded_config,
                    client=client,
                    spreadsheet_id=spreadsheet_id,
                    range_rows=range_rows)
