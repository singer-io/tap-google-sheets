import unittest
from unittest import mock
from tap_google_sheets import GoogleClient

class TestSupportsAllDrives(unittest.TestCase):
    def test_supportsAllDrives_not_in_config_file(self):
        """To verify that when supportsAllDrives are not given in config.json then set default value True"""
        # supportsAllDrives not in config.json so 
        supportsAllDrives = None
        client = GoogleClient('test', 'test', 'test', None, 'test', supportsAllDrives)
        self.assertEqual(client.supportsAllDrives, True, "supportsAllDrives get unexpected value")
        
    def test_supportsAllDrives_non_boolean_in_config(self):
        """To verify that when supportsAllDrives are given as non boolean value in config.json then set default value True"""
        
        # provide supportsAllDrives other then boolean value
        supportsAllDrives = 123
        client =  GoogleClient('test', 'test', 'test', None, 'test', supportsAllDrives)
        self.assertEqual(client.supportsAllDrives, True, "supportsAllDrives get unexpected value")
        
    def test_supportsAllDrives_boolean_false_in_config(self):
        """To verify that when supportsAllDrives are given boolean value in config.json then use supportsAllDrives"""
        
        # provide supportsAllDrives boolean value
        supportsAllDrives = False
        client =  GoogleClient('test', 'test', 'test', None, 'test', supportsAllDrives)
        self.assertEqual(client.supportsAllDrives, supportsAllDrives, "supportsAllDrives get unexpected value")
