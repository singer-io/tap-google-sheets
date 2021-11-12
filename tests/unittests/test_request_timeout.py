from tap_google_sheets.client import GoogleClient
import unittest
from unittest import mock
from unittest.case import TestCase
from requests.exceptions import Timeout
class TestBackoffError(unittest.TestCase):
    '''
    Test that backoff logic works properly.
    '''
    @mock.patch('tap_google_sheets.client.requests.Session.request')
    @mock.patch('tap_google_sheets.client.GoogleClient.get_access_token')
    def test_request_timeout_and_backoff(self, mock_get_token, mock_request):
        """
        Check whether the request backoffs properly for request() for 5 times in case of Timeout error.
        """
        mock_request.side_effect = Timeout
        with self.assertRaises(Timeout):
            client = GoogleClient("dummy_client_id", "dummy_client_secret", "dummy_refresh_token", 300)
            client.request("GET")
        self.assertEquals(mock_request.call_count, 5)

    @mock.patch('tap_google_sheets.client.requests.Session.post')
    def test_get_access_token_timeout_and_backoff(self, mock_post):
        """
        Check whether the request backoffs properly for get_access_token() for 5 times in case of Timeout error.
        """
        mock_post.side_effect = Timeout
        with self.assertRaises(Timeout):
            client = GoogleClient("dummy_client_id", "dummy_client_secret", "dummy_refresh_token", 300)
            client.get_access_token()
        self.assertEquals(mock_post.call_count, 5)

class MockResponse():
    '''
    Mock response  object for the requests call 
    '''
    def __init__(self, resp, status_code, content=[""], headers=None, raise_error=False, text={}):
        self.json_data = resp
        self.status_code = status_code
        self.content = content
        self.headers = headers
        self.raise_error = raise_error
        self.text = text
        self.reason = "error"

    def prepare(self):
        return (self.json_data, self.status_code, self.content, self.headers, self.raise_error)

    def json(self, object_pairs_hook):
        return self.text

class TestRequestTimeoutValue(unittest.TestCase):
    '''
    Test that request timeout parameter works properly in various cases
    '''
    @mock.patch('tap_google_sheets.client.requests.Session.request', return_value = MockResponse("", status_code=200))
    @mock.patch('tap_google_sheets.client.GoogleClient.get_access_token')
    def test_config_provided_request_timeout(self, mock_get, mock_request):
        """ 
            Unit tests to ensure that request timeout is set based on config value
        """
        config = { "refresh_token": "dummy_token", "client_id": "dummy_client_id", "client_secret": "dummy_client_secret", "user_agent": "dummy_ua", "request_timeout": 100}
        client = GoogleClient(**config)
        client.request("GET", "dummy_path")
        
        mock_request.assert_called_with('GET', 'https://sheets.googleapis.com/v4/dummy_path', headers={'Authorization': 'Bearer None', 'User-Agent': 'dummy_ua'}, timeout=100.0)

    @mock.patch('tap_google_sheets.client.requests.Session.request', return_value = MockResponse("", status_code=200))
    @mock.patch('tap_google_sheets.client.GoogleClient.get_access_token')
    def test_default_value_request_timeout(self, mock_get, mock_request):
        """ 
            Unit tests to ensure that request timeout is set based default value
        """
        config = { "refresh_token": "dummy_token", "client_id": "dummy_client_id", "client_secret": "dummy_client_secret", "user_agent": "dummy_ua"}
        client = GoogleClient(**config)
        client.request("GET", "dummy_path")
        
        mock_request.assert_called_with('GET', 'https://sheets.googleapis.com/v4/dummy_path', headers={'Authorization': 'Bearer None', 'User-Agent': 'dummy_ua'}, timeout=300.0)

    @mock.patch('tap_google_sheets.client.requests.Session.request', return_value = MockResponse("", status_code=200))
    @mock.patch('tap_google_sheets.client.GoogleClient.get_access_token')
    def test_config_provided_empty_request_timeout(self, mock_get, mock_request):
        """ 
            Unit tests to ensure that request timeout is set based on default value if empty value is given in config
        """
        config = { "refresh_token": "dummy_token", "client_id": "dummy_client_id", "client_secret": "dummy_client_secret", "user_agent": "dummy_ua", "request_timeout": ""}
        client = GoogleClient(**config)
        client.request("GET", "dummy_path")
        
        mock_request.assert_called_with('GET', 'https://sheets.googleapis.com/v4/dummy_path', headers={'Authorization': 'Bearer None', 'User-Agent': 'dummy_ua'}, timeout=300)

    @mock.patch('tap_google_sheets.client.requests.Session.request', return_value = MockResponse("", status_code=200))
    @mock.patch('tap_google_sheets.client.GoogleClient.get_access_token')
    def test_config_provided_string_request_timeout(self, mock_get, mock_request):
        """ 
            Unit tests to ensure that request timeout is set based on config string value
        """
        config = { "refresh_token": "dummy_token", "client_id": "dummy_client_id", "client_secret": "dummy_client_secret", "user_agent": "dummy_ua", "request_timeout": "100"}
        client = GoogleClient(**config)
        client.request("GET", "dummy_path")
        
        mock_request.assert_called_with('GET', 'https://sheets.googleapis.com/v4/dummy_path', headers={'Authorization': 'Bearer None', 'User-Agent': 'dummy_ua'}, timeout=100.0)

    @mock.patch('tap_google_sheets.client.requests.Session.request', return_value = MockResponse("", status_code=200))
    @mock.patch('tap_google_sheets.client.GoogleClient.get_access_token')
    def test_config_provided_float_request_timeout(self, mock_get, mock_request):
        """ 
            Unit tests to ensure that request timeout is set based on config float value
        """
        config = { "refresh_token": "dummy_token", "client_id": "dummy_client_id", "client_secret": "dummy_client_secret", "user_agent": "dummy_ua", "request_timeout": 100.8}
        client = GoogleClient(**config)
        client.request("GET", "dummy_path")
        
        mock_request.assert_called_with('GET', 'https://sheets.googleapis.com/v4/dummy_path', headers={'Authorization': 'Bearer None', 'User-Agent': 'dummy_ua'}, timeout=100.8)
