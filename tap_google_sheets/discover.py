from singer.catalog import Catalog, CatalogEntry, Schema
from tap_google_sheets.schema import STREAMS


def discover(client, spreadsheet_id):
    catalog = Catalog([])

    for stream, stream_obj in STREAMS.items():
        stream_object = stream_obj(client, spreadsheet_id)
        schemas, field_metadata = stream_object.get_schemas()

        # loop over the schema and prepare catalog
        for stream_name, schema_dict in schemas.items():

            schema = Schema.from_dict(schema_dict)
            mdata = field_metadata[stream_name]

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
