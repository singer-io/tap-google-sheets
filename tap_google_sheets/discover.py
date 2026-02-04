from singer.catalog import Catalog, CatalogEntry, Schema
from tap_google_sheets.schema import STREAMS
import singer
from singer import metadata

LOGGER = singer.get_logger()


def discover(client, config):
    catalog = Catalog([])
    spreadsheet_id = config.get('spreadsheet_id')
    sheet_names = config.get('sheet_names', [])

    # Log sheet filtering info
    if sheet_names:
        LOGGER.info('Filtering sheets to: %s', sheet_names)
    else:
        LOGGER.info('No sheet_names filter specified, discovering all sheets')

    for stream, stream_obj in STREAMS.items():
        stream_object = stream_obj(client, spreadsheet_id)
        schemas, field_metadata = stream_object.get_schemas(sheet_names_filter=sheet_names)

        # loop over the schema and prepare catalog
        for stream_name, schema_dict in schemas.items():

            schema = Schema.from_dict(schema_dict)
            mdata = field_metadata[stream_name]

            # If sheet_names filter is specified, automatically select matching sheets
            # This makes meltano el work without needing manual catalog selection
            if sheet_names and len(sheet_names) > 0:
                # Check if this stream is a sheet (not metadata streams)
                if stream_name not in ['spreadsheet_metadata', 'sheet_metadata', 'sheets_loaded']:
                    # Mark sheet as selected if it's in the filter
                    if stream_name in sheet_names:
                        mdata_map = metadata.to_map(mdata)
                        mdata = metadata.write(mdata_map, (), 'selected', True)
                        mdata = metadata.to_list(mdata_map)
                        LOGGER.info('Auto-selecting sheet (in sheet_names filter): %s', stream_name)

            # get the primary keys for the stream
            #   if the stream is from STREAM, then get the key_properties
            #   else use the "table-key-properties" from the metadata
            if not STREAMS.get(stream_name):
                key_props = None
                # get primary key for the stream
                for mdt in mdata:
                    table_key_properties = mdt.get('metadata', {}).get('table-key-properties')
                    if table_key_properties:
                        key_props = table_key_properties
            else:
                stream_obj = STREAMS.get(stream_name)(client, spreadsheet_id)
                key_props = stream_obj.key_properties

            catalog.streams.append(CatalogEntry(
                stream=stream_name,
                tap_stream_id=stream_name,
                key_properties=key_props,
                schema=schema,
                metadata=mdata
            ))

    return catalog
