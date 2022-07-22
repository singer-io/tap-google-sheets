from datetime import datetime, timedelta
from collections import OrderedDict
import backoff
import requests
import singer
from singer import metrics
from singer import utils
from requests.exceptions import Timeout, ConnectionError

BASE_URL = 'https://www.googleapis.com'
GOOGLE_TOKEN_URI = 'https://oauth2.googleapis.com/token'
LOGGER = singer.get_logger()
REQUEST_TIMEOUT = 300

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


def get_exception_for_error_code(error_code):
    return ERROR_CODE_EXCEPTION_MAPPING.get(error_code, GoogleError)

def raise_for_error(response):
    try:
        response.raise_for_status()
    except (requests.HTTPError, requests.ConnectionError) as error:
        try:
            content_length = len(response.content)
            if content_length == 0:
                # There is nothing we can do here since Google has neither sent
                # us a 2xx response nor a response content.
                return
            # Fetch the status code from the response object itself.
            status_code = response.status_code
            response = response.json()
            if ('error' in response) or ('errorCode' in response):
                # To form the error message, first, check for the message. If the message is not available, check for `error_description` in response.
                # If both are not available, raise an Unknown Error.
                message = 'HTTP-error-code: %s %s: %s' % (status_code, response.get('error', str(error)),
                                      response.get('message',  response.get('error_description', 'Unknown Error')))
                ex = get_exception_for_error_code(status_code)
                raise ex(message)
            raise GoogleError(error)
        except (ValueError, TypeError):
            raise GoogleError(error)

class GoogleClient: # pylint: disable=too-many-instance-attributes
    def __init__(self,
                 client_id,
                 client_secret,
                 refresh_token,
                 request_timeout=REQUEST_TIMEOUT,
                 user_agent=None):
        self.__client_id = client_id
        self.__client_secret = client_secret
        self.__refresh_token = refresh_token
        self.__user_agent = user_agent
        self.__access_token = None
        self.__expires = None
        self.__session = requests.Session()
        self.base_url = None

        # if request_timeout is other than 0,"0" or "" then use request_timeout
        if request_timeout and float(request_timeout):
            request_timeout = float(request_timeout)
        else: # If value is 0,"0" or "" then set default to 300 seconds.
            request_timeout = REQUEST_TIMEOUT
        self.request_timeout = request_timeout

    # Backoff request for 5 times at an interval of 10 seconds in case of Timeout or Connection error
    @backoff.on_exception(backoff.constant,
                          (Timeout, ConnectionError),
                          max_tries=5,
                          interval=10,
                          jitter=None) # Interval value not consistent if jitter not None
    def __enter__(self):
        self.get_access_token()
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.__session.close()

    @backoff.on_exception(backoff.expo,
                          Server5xxError,
                          max_tries=5,
                          factor=2)
    def get_access_token(self):
        # The refresh_token never expires and may be used many times to generate each access_token
        # Since the refresh_token does not expire, it is not included in get access_token response
        if self.__access_token is not None and self.__expires > datetime.utcnow():
            return

        headers = {}
        if self.__user_agent:
            headers['User-Agent'] = self.__user_agent

        response = self.__session.post(
            url=GOOGLE_TOKEN_URI,
            headers=headers,
            data={
                'grant_type': 'refresh_token',
                'client_id': self.__client_id,
                'client_secret': self.__client_secret,
                'refresh_token': self.__refresh_token,
            },
            timeout=self.request_timeout)

        if response.status_code >= 500:
            raise Server5xxError()

        if response.status_code != 200:
            raise_for_error(response)

        data = response.json()
        self.__access_token = data['access_token']
        self.__expires = datetime.utcnow() + timedelta(seconds=data['expires_in'])
        LOGGER.info('Authorized, token expires = {}'.format(self.__expires))


    # Backoff request for 5 times at an interval of 10 seconds when we get Timeout error
    @backoff.on_exception(backoff.constant,
                          (Timeout), 
                          max_tries=5,
                          interval=10,
                          jitter=None) # Interval value not consistent if jitter not None
    # Rate Limit: https://developers.google.com/sheets/api/limits
    #   100 request per 100 seconds per User
    @backoff.on_exception(backoff.expo,
                          (Server5xxError, ConnectionError, Server429Error),
                          max_tries=7,
                          factor=3,
                          jitter=None)
    @utils.ratelimit(100, 100)
    def request(self, method, path=None, url=None, api=None, **kwargs):
        self.get_access_token()
        self.base_url = 'https://sheets.googleapis.com/v4'
        if api == 'files':
            self.base_url = 'https://www.googleapis.com/drive/v3'

        if not url and path:
            url = '{}/{}'.format(self.base_url, path)

        # endpoint = stream_name (from sync.py API call)
        if 'endpoint' in kwargs:
            endpoint = kwargs['endpoint']
            del kwargs['endpoint']
        else:
            endpoint = None
        LOGGER.info('{} URL = {}'.format(endpoint, url))

        if 'headers' not in kwargs:
            kwargs['headers'] = {}
        kwargs['headers']['Authorization'] = 'Bearer {}'.format(self.__access_token)

        if self.__user_agent:
            kwargs['headers']['User-Agent'] = self.__user_agent

        if method == 'POST':
            kwargs['headers']['Content-Type'] = 'application/json'

        with metrics.http_request_timer(endpoint) as timer:
            
            response = self.__session.request(method, url, timeout=self.request_timeout, **kwargs)
            timer.tags[metrics.Tag.http_status_code] = response.status_code

        if response.status_code >= 500:
            raise Server5xxError()

        #Use retry functionality in backoff to wait and retry if
        #response code equals 429 because rate limit has been exceeded
        if response.status_code == 429:
            raise Server429Error(response.json().get("error",{}).get("message", "Rate limit exceeded"))

        if response.status_code != 200:
            raise_for_error(response)

        # Ensure keys and rows are ordered as received from API
        return response.json(object_pairs_hook=OrderedDict)

    def get(self, path, api, **kwargs):
        return self.request(method='GET', path=path, api=api, **kwargs)

    def post(self, path, api, **kwargs):
        return self.request(method='POST', path=path, api=api, **kwargs)
