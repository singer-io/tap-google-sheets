import unittest
from unittest import mock
from tap_google_sheets import main


class MockedParseArgs:
    discover = False
    config = {"client_id":"", "client_secret": "", "refresh_token": "", "user_agent": ""}
    state = False
    catalog = "test"
    
@mock.patch("tap_google_sheets.discover")
@mock.patch("tap_google_sheets.singer.utils.parse_args", return_value=MockedParseArgs)
@mock.patch("tap_google_sheets.GoogleClient.__enter__", return_value="test")
@mock.patch("tap_google_sheets.sync")
class TestCatalog(unittest.TestCase):
    def test_catalog_is_given_in_sync(self, mocked_sync, mocked_google_client, mocked_parse_args, mocked_discover):
        """
        To verify that if catalog is given in sync mode then run with catalog file
        """
        main()
        mocked_sync.assert_called_with(client="test", config=MockedParseArgs.config, catalog="test", state={})
        self.assertEqual(mocked_discover.call_count, 0, "discover function is not called expected times")
        
    def test_catalog_is_not_given_in_sync(self, mocked_sync, mocked_google_client, mocked_parse_args, mocked_discover):
        """
        To verify that if catalog is not given in sync mode then run discover mode to generate catalog 
        """
        # mocking discover function
        MockedParseArgs.catalog = ""
        mocked_discover.return_value = "test"
        main()
        mocked_sync.assert_called_with(client="test", config=MockedParseArgs.config, catalog="test", state={})
        self.assertEqual(mocked_discover.call_count, 1, "discover function is not called expected times")