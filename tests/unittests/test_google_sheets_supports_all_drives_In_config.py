import unittest
from unittest import mock
from tap_google_sheets import _sync
import datetime

class Mocked:
    stream = "file_metadata"
    params = {}
    def __init__(self, client, spreadsheet_id, start_date):
        pass
    def sync(self, catalog, state, selected_streams):
        return "test", datetime.datetime.now()
    def get_selected_streams(state):
        return [Mocked]

@mock.patch("tap_google_sheets.sync.strftime", return_value="test")
@mock.patch("tap_google_sheets.sync.STREAMS.items", return_value={("file_metadata", Mocked)})
class Testsupports_all_drives(unittest.TestCase):
    config = {
        "start_date": "test", 
        "spreadsheet_id": "test"
    }
    def test_supports_all_drives_not_in_config_file(self, mocked_STREAMS, mocked_strftime):
        """To verify that when the supports_all_drives value is not given in config.json then set default value as False"""
        
        _sync("test_client", self.config, Mocked, {})
        self.assertEqual(Mocked.params["supportsAllDrives"], False, "supportsAllDrives got unexpected value" )
        
    def test_supports_all_drives_other_than_true_in_config(self, mocked_STREAMS, mocked_strftime):
        """To verify that when the supports_all_drives value is given other than True in config.json then use False"""
        
        config = self.config | {"supports_all_drives": False}
        _sync("test_client", config, Mocked, {})
        self.assertEqual(Mocked.params["supportsAllDrives"], False, "supportsAllDrives got unexpected value" )
        
    def test_supports_all_drives_boolean_true_in_config(self,  mocked_STREAMS, mocked_strftime):
        """To verify that when the supports_all_drives value is given True as boolean in config.json then use supports_all_drives"""
        
        _sync("test_client", self.config | {"supports_all_drives": True}, Mocked, {})
        self.assertEqual(Mocked.params["supportsAllDrives"], True, "supportsAllDrives got unexpected value" )
