import singer
from tap_google_sheets.streams import STREAMS, SheetsLoadData, write_bookmark, strftime

LOGGER = singer.get_logger()

def sync(client, config, catalog, state):
    """
    Sync the streams, loop over STREAMS
        "spreadsheet_metadata" -> get the spreadsheet's metadata
            - sync the spreadsheet_metadata stream if selected
            - get the sheets in the spreadsheet and loop over the sheets and sync the sheet's records if selected
                - create 2 lists containing the data related the sheet's metadata and sheets loaded/synced during the sync
        "sheets_loaded" & "sheet_metadata" -> get the data lists from the "spreadsheet_metadata" stream and sync the records if selected
    """
    last_stream = singer.get_currently_syncing(state)
    LOGGER.info("last/currently syncing stream: %s", last_stream)

    selected_streams = []
    for stream in catalog.get_selected_streams(state):
        selected_streams.append(stream.stream)
    LOGGER.info("selected_streams: %s", selected_streams)

    if not selected_streams:
        # return if no stream is selected
        LOGGER.info("No stream is selected.")
        return

    # loop through main streams
    for stream_name, stream_obj in STREAMS.items():

        # get the stream object
        stream_obj = stream_obj(client, config.get("spreadsheet_id"), config.get("start_date"))

        # to sync the sheet's data, we need to get "spreadsheet_metadata"
        if stream_name == "spreadsheet_metadata":
            # get the metadata for the whole spreadsheet
            spreadsheet_metadata, time_extracted = stream_obj.get_data(stream_name=stream_obj.stream_name)

            # if the "spreadsheet_metadata" is selected, then do sync
            if stream_name in selected_streams:
                stream_obj.sync(catalog, state, spreadsheet_metadata, time_extracted)

            # get sheets from the metadata
            sheets = spreadsheet_metadata.get("sheets")
            # class to load sheet's data
            sheets_load_data = SheetsLoadData(client, config.get("spreadsheet_id"), config.get("start_date"))

            # perform sheet's sync and get sheet's metadata and sheet loaded records for "sheet_metadata" and "sheets_loaded" streams
            sheet_metadata_records, sheets_loaded_records = sheets_load_data.load_data(catalog=catalog,
                                                                                        state=state,
                                                                                        selected_streams=selected_streams,
                                                                                        sheets=sheets,
                                                                                        spreadsheet_time_extracted=time_extracted)

        # sync "sheet_metadata" and "sheets_loaded" based on the records from spreadsheet metadata
        elif stream_name in ["sheet_metadata", "sheets_loaded"] and stream_name in selected_streams:
            if stream_name == "sheet_metadata":
                stream_obj.sync(catalog, state, sheet_metadata_records)
            else:
                stream_obj.sync(catalog, state, sheets_loaded_records)

        LOGGER.info("FINISHED Syncing: %s", stream_name)
