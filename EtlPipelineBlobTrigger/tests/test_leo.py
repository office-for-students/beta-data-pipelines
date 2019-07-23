"""Test the course NSS statistics"""
# import json
import unittest

# import xml.etree.ElementTree as ET

# import xmltodict

from course_stats import Leo

# from testing_utils import get_string


class TestLeoDataFields(unittest.TestCase):
    def setUp(self):
        self.leo = Leo("XF")
        self.lookup = self.leo.data_fields_lookup

    def test_agg_lookup(self):
        xml_key = "LEOAGG"
        expected_key = "aggregation_level"
        expected_elem_type = "M"
        json_key = self.lookup[xml_key][0]
        elem_type = self.lookup[xml_key][1]
        self.assertEqual(expected_key, json_key)
        self.assertEqual(expected_elem_type, elem_type)

    def test_pop_lookup(self):
        xml_key = "LEOPOP"
        expected_key = "number_of_graduates"
        expected_elem_type = "M"
        json_key = self.lookup[xml_key][0]
        elem_type = self.lookup[xml_key][1]
        self.assertEqual(expected_key, json_key)
        self.assertEqual(expected_elem_type, elem_type)

    def test_subj_lookup(self):
        xml_key = "LEOSBJ"
        expected_key = "subject"
        expected_elem_type = "O"
        json_key = self.lookup[xml_key][0]
        elem_type = self.lookup[xml_key][1]
        self.assertEqual(expected_key, json_key)
        self.assertEqual(expected_elem_type, elem_type)


# TODO Test more of the functionality - more lookups etc

if __name__ == "__main__":
    unittest.main()
