import unittest
import os
import sys
import inspect

CURRENTDIR = os.path.dirname(
    os.path.abspath(inspect.getfile(inspect.currentframe())))
PARENTDIR = os.path.dirname(CURRENTDIR)
sys.path.insert(0, PARENTDIR)

from helper import Helper

class TestGetProviderName(unittest.TestCase):

    def test_when_there_are_more_than_one_alias(self):
        expected_key = 'Canton University'
        multipleAliases = {'ProviderName': 'University of England', 'ProviderAliases': [{'ProviderAlias': 'Canton University'}, {'ProviderAlias: Roath College'}, {'ProviderAlias': 'Whitchurch Engineering College'}]}
        
        key = Helper.get_provider_name(multipleAliases)
        self.assertEqual(expected_key, key)

    def test_when_there_is_only_one_alias(self):
        expected_key = 'Roath College'
        singleAlias = {'ProviderName': 'University of England', 'ProviderAliases': [{'ProviderAlias': 'Roath College'}]}

        key = Helper.get_provider_name(singleAlias)
        self.assertEqual(expected_key, key)

    def test_when_provider_aliases_is_none(self):
        expected_key = 'University of England'
        emptyProviderAliases = {'ProviderName': 'University of England', 'ProviderAliases': None}

        key = Helper.get_provider_name(emptyProviderAliases)
        self.assertEqual(expected_key, key)

    def test_when_provider_aliases_is_missing(self):
        expected_key = 'University of Wales'
        missingProviderAliases = {'ProviderName': 'University of Wales'}

        key = Helper.get_provider_name(missingProviderAliases)
        self.assertEqual(expected_key, key)
