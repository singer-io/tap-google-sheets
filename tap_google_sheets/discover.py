import singer
from singer.catalog import Catalog, CatalogEntry, Schema
from tap_google_sheets.schema import STREAMS
from tap_google_sheets.client import (
    GoogleUnauthorizedError,
    GoogleForbiddenError,
    GoogleNotFoundError,
    GoogleMethodNotAllowedError,
)

LOGGER = singer.get_logger()


def check_stream_access(client, spreadsheet_id) -> bool:
    """Probe the spreadsheet endpoint to verify the credentials can access it.
    Returns False on 401/403/404/405; returns True on success; re-raises other API errors.
    """
    path = 'spreadsheets/{}?includeGridData=false'.format(spreadsheet_id)
    LOGGER.info("Checking spreadsheet access for spreadsheet_id '%s'", spreadsheet_id)
    try:
        client.get(path=path, api='sheets', endpoint='spreadsheet_metadata')
        return True
    except (GoogleUnauthorizedError, GoogleForbiddenError,
            GoogleNotFoundError, GoogleMethodNotAllowedError):
        return False


def discover(client, spreadsheet_id):
    if not check_stream_access(client, spreadsheet_id):
        raise Exception(
           "Spreadsheet '{}' was not found or the credentials do not have access to it. "
            "Verify the spreadsheet ID and that the API credentials have the required permissions.".format(spreadsheet_id)
        )

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
