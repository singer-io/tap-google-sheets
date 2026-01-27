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

# Base required config keys (auth keys validated separately)
REQUIRED_CONFIG_KEYS = [
    'spreadsheet_id',
    'start_date',
    'user_agent'
]


def validate_auth_config(config):
    """
    Validate authentication configuration.

    Either OAuth2 credentials (client_id, client_secret, refresh_token) OR
    service account credentials (credentials_file OR credentials_json) must be provided,
    but not both.

    Returns:
        str: 'oauth2' or 'service_account' indicating which auth type to use

    Raises:
        Exception: If config is invalid
    """
    # Check for OAuth2 credentials
    has_oauth2 = all(key in config for key in ['client_id', 'client_secret', 'refresh_token'])

    # Check for service account credentials
    has_credentials_file = 'credentials_file' in config and config['credentials_file']
    has_credentials_json = 'credentials_json' in config and config['credentials_json']
    has_service_account = has_credentials_file or has_credentials_json

    # Validate mutual exclusivity
    if has_oauth2 and has_service_account:
        raise Exception(
            'Invalid config: Cannot specify both OAuth2 credentials (client_id, client_secret, '
            'refresh_token) and service account credentials (credentials_file or credentials_json). '
            'Please use one authentication method only.'
        )

    if not has_oauth2 and not has_service_account:
        raise Exception(
            'Invalid config: Must specify either OAuth2 credentials (client_id, client_secret, '
            'refresh_token) OR service account credentials (credentials_file or credentials_json).'
        )

    if has_credentials_file and has_credentials_json:
        raise Exception(
            'Invalid config: Cannot specify both credentials_file and credentials_json. '
            'Please use one service account credential source only.'
        )

    if has_service_account:
        LOGGER.info('Using service account authentication')
        return 'service_account'
    else:
        LOGGER.info('Using OAuth2 authentication')
        return 'oauth2'

def do_discover(client, spreadsheet_id):

    LOGGER.info('Starting discover')
    catalog = discover(client, spreadsheet_id)
    json.dump(catalog.to_dict(), sys.stdout, indent=2)
    LOGGER.info('Finished discover')


@singer.utils.handle_top_exception(LOGGER)
def main():

    parsed_args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)
    config = parsed_args.config

    # Validate authentication config
    auth_type = validate_auth_config(config)

    # Build GoogleClient kwargs based on auth type
    client_kwargs = {
        'request_timeout': config.get('request_timeout'),
        'user_agent': config['user_agent']
    }

    if auth_type == 'oauth2':
        client_kwargs['client_id'] = config['client_id']
        client_kwargs['client_secret'] = config['client_secret']
        client_kwargs['refresh_token'] = config['refresh_token']
    else:  # service_account
        client_kwargs['credentials_file'] = config.get('credentials_file')
        client_kwargs['credentials_json'] = config.get('credentials_json')

    with GoogleClient(**client_kwargs) as client:

        state = {}
        if parsed_args.state:
            state = parsed_args.state

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
