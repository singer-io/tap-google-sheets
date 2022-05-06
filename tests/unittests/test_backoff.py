from tap_google_sheets.client import GoogleClient, Server429Error
import unittest
from unittest import mock
from datetime import datetime

class TestBackoffError(unittest.TestCase):
    '''
    Test that backoff logic works properly.
    '''
    @mock.patch('tap_google_sheets.client.requests.Session.request')
    @mock.patch('tap_google_sheets.client.GoogleClient.get_access_token')
    def test_request_timeout_and_backoff(self, mock_get_token, mock_request):
        """
        Check whether the request backoffs properly for request() for more than a minute for Server429Error.
        """
        mock_request.side_effect = Server429Error
        client = GoogleClient("dummy_client_id", "dummy_client_secret", "dummy_refresh_token")
        before_time = datetime.now()
        with self.assertRaises(Server429Error):
            client.request("GET")
        after_time = datetime.now()
        # verify that the tap backoff for more than 60 seconds
        time_difference = (after_time - before_time).total_seconds()
        self.assertGreaterEqual(time_difference, 60)
