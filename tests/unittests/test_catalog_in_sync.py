from shutil import ExecError
import unittest
from unittest import mock
from tap_google_sheets import main


class MockedParseArgs:
    discover = False
    config = {"client_id":"", "client_secret": "", "refresh_token": "", "user_agent": ""}
    state = False
    catalog = "test"
    
@mock.patch("tap_google_sheets.discover")
@mock.patch("tap_google_sheets.singer.utils.parse_args")
@mock.patch("tap_google_sheets.GoogleClient.__enter__")
@mock.patch("tap_google_sheets.sync")
class TestCatalog(unittest.TestCase):
    def test_catalog_is_given_in_sync(self, mocked_sync, mocked_google_client, mocked_parse_args, mocked_discover):
        """
        To verify that if catalog is given in sync mode then run with catalog file
        """
        MockedParseArgs.catalog = "test"
        mocked_parse_args.return_value = MockedParseArgs
        mocked_google_client.return_value = "test"
        main()
        mocked_sync.assert_called_with(client="test", config=MockedParseArgs.config, catalog="test", state={})
        
    def test_catalog_is_not_given_in_sync(self, mocked_sync, mocked_google_client, mocked_parse_args, mocked_discover):
        """
        To verify that if catalog is not given in sync mode then run discover mode to generate catalog 
        """
        # mocking discover function
        MockedParseArgs.catalog = ""
        mocked_discover.return_value = "test"
        mocked_parse_args.return_value = MockedParseArgs
        mocked_google_client.return_value = "test"
        main()
        mocked_sync.assert_called_with(client="test", config=MockedParseArgs.config, catalog="test", state={})