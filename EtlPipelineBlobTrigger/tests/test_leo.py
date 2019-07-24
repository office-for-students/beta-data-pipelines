"""Test the course NSS statistics"""
import unittest

import xml.etree.ElementTree as ET

import xmltodict

from course_docs import get_country
from course_stats import Leo

from testing_utils import get_string


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


class TestGetStats(unittest.TestCase):
    def test_with_large_file(self):
        """Initial smoke test"""
        xml_string = get_string("fixtures/large-test-file.xml")
        root = ET.fromstring(xml_string)
        for institution in root.iter("INSTITUTION"):
            raw_inst_data = xmltodict.parse(ET.tostring(institution))[
                "INSTITUTION"
            ]
            country_code = get_country(raw_inst_data)["code"]
            for course in institution.findall("KISCOURSE"):
                raw_course_data = xmltodict.parse(ET.tostring(course))[
                    "KISCOURSE"
                ]
                leo = Leo(country_code)
                leo.get_stats(raw_course_data)


# TODO Test more of the functionality - more lookups etc

if __name__ == "__main__":
    unittest.main()
