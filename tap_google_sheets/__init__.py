#!/usr/bin/env python3

import sys
import json
import argparse
import singer
from singer import metadata, utils
from tap_google_sheets.client import GoogleClient
from tap_google_sheets.discover import discover
from tap_google_sheets.sync import sync

LOGGER = singer.get_logger()

REQUIRED_CONFIG_KEYS = [
    'client_id',
    'client_secret',
    'refresh_token',
    'spreadsheet_id',
    'start_date',
    'user_agent'
]

def do_discover(client, spreadsheet_id):

    LOGGER.info('Starting discover')
    catalog = discover(client, spreadsheet_id)
    json.dump(catalog.to_dict(), sys.stdout, indent=2)
    LOGGER.info('Finished discover')


@singer.utils.handle_top_exception(LOGGER)
def main():

    parsed_args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)

    with GoogleClient(parsed_args.config['client_id'],
                      parsed_args.config['client_secret'],
                      parsed_args.config['refresh_token'],
                      parsed_args.config.get('request_timeout'),
                      parsed_args.config['user_agent']
                      ) as client:

        state = {}
        if parsed_args.state:
            state = parsed_args.state

        config = parsed_args.config
        spreadsheet_id = config.get('spreadsheet_id')

        if parsed_args.discover:
            do_discover(client, spreadsheet_id)
        else:
            sync(client=client,
                 config=config,
                 catalog=parsed_args.catalog or discover(client, spreadsheet_id),
                 state=state)

if __name__ == '__main__':
    main()
