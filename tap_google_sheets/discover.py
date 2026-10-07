import singer
from singer.catalog import Catalog, CatalogEntry, Schema
from tap_google_sheets.schema import STREAMS
from tap_google_sheets.client import GoogleForbiddenError, GoogleNotFoundError

LOGGER = singer.get_logger()


def check_stream_access(client, spreadsheet_id) -> bool:
    """Probe spreadsheet access and return True if the credentials can read it.

    - 401 unauthorized and 405 method not allowed are intentionally NOT
      caught here and propagate unchanged. Neither indicates "this
      spreadsheet doesn't exist or isn't shared with these credentials":
      * 401 means the credentials themselves are invalid/expired -- an
        authentication problem that picking a different spreadsheet ID
        can't fix.
      * 405 means the probe request itself isn't supported by the API -- a
        client/contract issue, not a permissions problem.
      Letting these propagate as-is preserves their real HTTP status and
      message instead of misdirecting the user toward checking the
      spreadsheet ID/sharing settings.
    - 403 forbidden and 404 not found ARE genuine "no access to this
      spreadsheet" conditions. They're re-raised as a new exception of the
      same type, enriched with the spreadsheet id and remediation guidance,
      while still including the original HTTP-error-code/message so no API
      context is discarded.
    """
    path = 'spreadsheets/{}?includeGridData=false'.format(spreadsheet_id)
    LOGGER.info("Checking spreadsheet access for spreadsheet_id '%s'", spreadsheet_id)
    try:
        client.get(path=path, api='sheets', endpoint='spreadsheet_metadata')
        return True
    except (GoogleForbiddenError, GoogleNotFoundError) as err:
        LOGGER.critical(
            "Spreadsheet '%s' was not found or the credentials do not have access to it. "
            "HTTP-Error-Message: '%s'",
            spreadsheet_id,
            str(err),
        )
        raise type(err)(
            "Spreadsheet '{}' was not found or the credentials do not have access to it. "
            "Verify the spreadsheet ID and that the API credentials have the required "
            "permissions. HTTP-Error-Message: '{}'".format(spreadsheet_id, str(err))
        ) from err


def discover(client, spreadsheet_id):
    check_stream_access(client, spreadsheet_id)

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
