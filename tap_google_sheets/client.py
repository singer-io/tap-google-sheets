from datetime import datetime, timedelta
from collections import OrderedDict
import backoff
import singer
import logging
import pickle
import json
import os
from singer import metrics
from singer import utils
from google.oauth2 import service_account
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.errors import HttpError
import googleapiclient.discovery

LOGGER = singer.get_logger()

class Server5xxError(Exception):
    pass


class Server429Error(Exception):
    pass


class GoogleError(Exception):
    pass


class GoogleBadRequestError(GoogleError):
    pass


class GoogleUnauthorizedError(GoogleError):
    pass


class GooglePaymentRequiredError(GoogleError):
    pass


class GoogleNotFoundError(GoogleError):
    pass


class GoogleMethodNotAllowedError(GoogleError):
    pass


class GoogleConflictError(GoogleError):
    pass


class GoogleGoneError(GoogleError):
    pass


class GooglePreconditionFailedError(GoogleError):
    pass


class GoogleRequestEntityTooLargeError(GoogleError):
    pass


class GoogleRequestedRangeNotSatisfiableError(GoogleError):
    pass


class GoogleExpectationFailedError(GoogleError):
    pass


class GoogleForbiddenError(GoogleError):
    pass


class GoogleUnprocessableEntityError(GoogleError):
    pass


class GooglePreconditionRequiredError(GoogleError):
    pass


class GoogleInternalServiceError(GoogleError):
    pass


# Error Codes: https://developers.google.com/webmaster-tools/search-console-api-original/v3/errors
ERROR_CODE_EXCEPTION_MAPPING = {
    400: GoogleBadRequestError,
    401: GoogleUnauthorizedError,
    402: GooglePaymentRequiredError,
    403: GoogleForbiddenError,
    404: GoogleNotFoundError,
    405: GoogleMethodNotAllowedError,
    409: GoogleConflictError,
    410: GoogleGoneError,
    412: GooglePreconditionFailedError,
    413: GoogleRequestEntityTooLargeError,
    416: GoogleRequestedRangeNotSatisfiableError,
    417: GoogleExpectationFailedError,
    422: GoogleUnprocessableEntityError,
    428: GooglePreconditionRequiredError,
    500: GoogleInternalServiceError}

class GoogleClient: # pylint: disable=too-many-instance-attributes
    SCOPES = [
        "https://www.googleapis.com/auth/drive.metadata.readonly",
        "https://www.googleapis.com/auth/spreadsheets.readonly"
    ]

    def __init__(self, credentials_file):
        self.__credentials = self.fetchCredentials(credentials_file)
        self.__sheets_service = googleapiclient.discovery.build(
            'sheets',
            'v4',
            credentials=self.__credentials,
            cache_discovery=False
        )
        self.__drive_service = googleapiclient.discovery.build(
            'drive',
            'v3',
            credentials=self.__credentials,
            cache_discovery=False
        )

    def fetchCredentials(self, credentials_file):
        LOGGER.debug('authenticate with google')
        data = None

        # Check a credentials file exist
        if not os.path.exists(credentials_file):
            raise Exception("The configured Google credentials file {} doesn't exist".format(credentials_file))

        # Load credentials json file
        with open(credentials_file) as json_file:
            data = json.load(json_file)

        if data.get('type', '') == 'service_account':
            return self.fetchServiceAccountCredentials(credentials_file)
        elif data.get('installed'):
            return self.fetchInstalledOAuthCredentials(credentials_file)
        else:
            raise Exception("""This Google credentials file is not yet recognize.

            Please use either:
            - a Service Account (https://github.com/googleapis/google-api-python-client/blob/d0110cf4f7aaa93d6f56fc028cd6a1e3d8dd300a/docs/oauth-server.md)
            - an installed OAuth client (https://github.com/googleapis/google-api-python-client/blob/d0110cf4f7aaa93d6f56fc028cd6a1e3d8dd300a/docs/oauth-installed.md)"""
            )

    def fetchServiceAccountCredentials(self, credentials_file):
        # The service account credentials file can be used for server-to-server applications
        return service_account.Credentials.from_service_account_file(
            credentials_file, scopes=GoogleClient.SCOPES)

    def fetchInstalledOAuthCredentials(self, credentials_file):
        creds = None

        # The file token.pickle stores the user's access and refresh tokens, and is
        # created automatically when the authorization flow completes for the first
        # time.
        if os.path.exists('token.pickle'):
            with open('token.pickle', 'rb') as token:
                creds = pickle.load(token)

        # If there are no (valid) credentials available, let the user log in.
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    credentials_file, GoogleClient.SCOPES)
                creds = flow.run_local_server(port=0)
            # Save the credentials for the next run
            with open('token.pickle', 'wb') as token:
                pickle.dump(creds, token)

        return creds

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        LOGGER.debug('exiting google client')

    # Rate Limit: https://developers.google.com/sheets/api/limits
    #   100 request per 100 seconds per User
    @backoff.on_exception(backoff.expo,
                          (Server5xxError, ConnectionError, Server429Error),
                          max_tries=7,
                          factor=3)
    @utils.ratelimit(100, 100)
    def request(self, endpoint=None, params={}, **kwargs):
        formatted_params = {}
        for (key, value) in params.items():
            # API parameters interpolation
            # will raise a KeyError in case a necessary argument is missing
            formatted_params[key] = value.format(**kwargs)

        # Call the correct Google API depending on the stream name
        if endpoint == 'spreadsheet_metadata' or endpoint == 'sheet_metadata':
            # https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/get
            request = self.__sheets_service.spreadsheets().get(**formatted_params)
        elif endpoint == 'sheets_loaded':
            # https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets.values/get
            request = self.__sheets_service.spreadsheets().values().get(**formatted_params)
        elif endpoint == 'file_metadata':
            # https://developers.google.com/drive/api/v3/reference/files/get
            request = self.__drive_service.files().get(**formatted_params)
        else:
            raise Exception('{} not implemented yet!'.format(endpoint))

        with metrics.http_request_timer(endpoint) as timer:
            error = None
            status_code = 400

            try:
                response = request.execute()
                status_code = 200
            except HttpError as e:
                status_code = e.resp.status or status_code
                error = e

            timer.tags[metrics.Tag.http_status_code] = status_code

        if status_code >= 500:
            raise Server5xxError()

        # Use retry functionality in backoff to wait and retry if
        # response code equals 429 because rate limit has been exceeded
        if status_code == 429:
            raise Server429Error()

        if status_code != 200:
            raise error

        return response
