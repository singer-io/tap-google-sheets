import unittest
from unittest import mock
from tap_google_sheets import GoogleClient

class TestSupportsAllDrives(unittest.TestCase):
    def test_supportsAllDrives_not_in_config_file(self):
        """To verify that when the supportsAllDrives value is not given in config.json then set default value as False"""
        # supportsAllDrives not in config.json so 
        supportsAllDrives = None
        client = GoogleClient('test', 'test', 'test', None, 'test', supportsAllDrives)
        self.assertEqual(client.supportsAllDrives, False, "supportsAllDrives got unexpected value")
        
    def test_supportsAllDrives_other_than_true_in_config(self):
        """To verify that when the supportsAllDrives value is given other than true value in config.json then set default value False"""
        
        # provide supportsAllDrives other then boolean value
        supportsAllDrives = 123
        client =  GoogleClient('test', 'test', 'test', None, 'test', supportsAllDrives)
        self.assertEqual(client.supportsAllDrives, False, "supportsAllDrives got unexpected value")
        
    def test_supportsAllDrives_str_true_in_config(self):
        """To verify that when the supportsAllDrives value is given true as a string in config.json then set True"""
        
        # provide supportsAllDrives false as string value
        supportsAllDrives = 'true'
        client =  GoogleClient('test', 'test', 'test', None, 'test', supportsAllDrives)
        self.assertEqual(client.supportsAllDrives, True, "supportsAllDrives got unexpected value")
        
    def test_supportsAllDrives_boolean_true_in_config(self):
        """To verify that when the supportsAllDrives value is given True as boolean in config.json then use supportsAllDrives"""
        
        # provide supportsAllDrives boolean value
        supportsAllDrives = True
        client =  GoogleClient('test', 'test', 'test', None, 'test', supportsAllDrives)
        self.assertEqual(client.supportsAllDrives, supportsAllDrives, "supportsAllDrives got unexpected value")
