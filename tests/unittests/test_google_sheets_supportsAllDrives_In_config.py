import unittest
from unittest import mock
from tap_google_sheets import GoogleClient

class Testsupports_all_drives(unittest.TestCase):
    def test_supports_all_drives_not_in_config_file(self):
        """To verify that when the supports_all_drives value is not given in config.json then set default value as False"""
        # supports_all_drives not in config.json so 
        supports_all_drives = None
        client = GoogleClient('test', 'test', 'test', None, 'test', supports_all_drives)
        self.assertEqual(client.supports_all_drives, False, "supports_all_drives got unexpected value")
        
    def test_supports_all_drives_other_than_true_in_config(self):
        """To verify that when the supports_all_drives value is given other than true value in config.json then set default value False"""
        
        # provide supports_all_drives other then boolean value
        supports_all_drives = 123
        client =  GoogleClient('test', 'test', 'test', None, 'test', supports_all_drives)
        self.assertEqual(client.supports_all_drives, False, "supports_all_drives got unexpected value")
        
    def test_supports_all_drives_str_true_in_config(self):
        """To verify that when the supports_all_drives value is given true as a string in config.json then set True"""
        
        # provide supports_all_drives false as string value
        supports_all_drives = 'true'
        client =  GoogleClient('test', 'test', 'test', None, 'test', supports_all_drives)
        self.assertEqual(client.supports_all_drives, True, "supports_all_drives got unexpected value")
        
    def test_supports_all_drives_boolean_true_in_config(self):
        """To verify that when the supports_all_drives value is given True as boolean in config.json then use supports_all_drives"""
        
        # provide supports_all_drives boolean value
        supports_all_drives = True
        client =  GoogleClient('test', 'test', 'test', None, 'test', supports_all_drives)
        self.assertEqual(client.supports_all_drives, supports_all_drives, "supports_all_drives got unexpected value")
