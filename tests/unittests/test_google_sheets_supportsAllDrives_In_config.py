import unittest
from unittest import mock
from tap_google_sheets import GoogleClient

class TestSupportsAllDrives(unittest.TestCase):
    def test_supportsAllDrives_not_in_config_file(self):
        """To verify that when supportsAllDrives are not given in config.json then set default value True"""
        # supportsAllDrives not in config.json so 
        supportsAllDrives = None
        client = GoogleClient('test', 'test', 'test', None, 'test', supportsAllDrives)
        self.assertEqual(client.supportsAllDrives, True, "supportsAllDrives got unexpected value")
        
    def test_supportsAllDrives_other_than_false_in_config(self):
        """To verify that when supportsAllDrives are given other than false value in config.json then set default value True"""
        
        # provide supportsAllDrives other then boolean value
        supportsAllDrives = 123
        client =  GoogleClient('test', 'test', 'test', None, 'test', supportsAllDrives)
        self.assertEqual(client.supportsAllDrives, True, "supportsAllDrives got unexpected value")
        
    def test_supportsAllDrives_str_false_in_config(self):
        """To verify that when supportsAllDrives are given false value string in config.json then use supportsAllDrives"""
        
        # provide supportsAllDrives false as string value
        supportsAllDrives = 'false'
        client =  GoogleClient('test', 'test', 'test', None, 'test', supportsAllDrives)
        self.assertEqual(client.supportsAllDrives, supportsAllDrives, "supportsAllDrives got unexpected value")
        
    def test_supportsAllDrives_boolean_false_in_config(self):
        """To verify that when supportsAllDrives are given false value boolean in config.json then use supportsAllDrives"""
        
        # provide supportsAllDrives boolean value
        supportsAllDrives = False
        client =  GoogleClient('test', 'test', 'test', None, 'test', supportsAllDrives)
        self.assertEqual(client.supportsAllDrives, supportsAllDrives, "supportsAllDrives got unexpected value")
