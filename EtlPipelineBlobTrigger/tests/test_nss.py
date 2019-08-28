"""Test the course NSS statistics"""
import json
import unittest

import defusedxml.ElementTree as ET
import xmltodict

from course_stats import Nss
from testing_utils import get_string


class TestLookupDataFields(unittest.TestCase):
    def setUp(self):
        self.nss = Nss()
        self.lookup = self.nss.nss_data_fields_lookup

    def test_agg_lookup(self):
        xml_key = "NSSAGG"
        expected_key = "aggregation_level"
        expected_elem_type = "M"
        json_key = self.lookup[xml_key][0]
        elem_type = self.lookup[xml_key][1]
        self.assertEqual(expected_key, json_key)
        self.assertEqual(expected_elem_type, elem_type)

    def test_resp_rate_lookup(self):
        xml_key = "NSSRESP_RATE"
        expected_key = "response_rate"
        expected_elem_type = "M"
        json_key = self.lookup[xml_key][0]
        elem_type = self.lookup[xml_key][1]
        self.assertEqual(expected_key, json_key)
        self.assertEqual(expected_elem_type, elem_type)

    def test_pop_lookup(self):
        xml_key = "NSSPOP"
        expected_key = "number_of_students"
        expected_elem_type = "M"
        json_key = self.lookup[xml_key][0]
        elem_type = self.lookup[xml_key][1]
        self.assertEqual(expected_key, json_key)
        self.assertEqual(expected_elem_type, elem_type)

    def test_subj_lookup(self):
        xml_key = "NSSSBJ"
        expected_key = "subject"
        expected_elem_type = "O"
        json_key = self.lookup[xml_key][0]
        elem_type = self.lookup[xml_key][1]
        self.assertEqual(expected_key, json_key)
        self.assertEqual(expected_elem_type, elem_type)

    def test_question_lookup(self):
        xml_key = "Q1"
        expected_key = "question_1"
        expected_elem_type = "M"
        json_key = self.lookup[xml_key][0]
        elem_type = self.lookup[xml_key][1]
        self.assertEqual(expected_key, json_key)
        self.assertEqual(expected_elem_type, elem_type)


class TestLookupQuestionNumber(unittest.TestCase):
    def setUp(self):
        self.nss = Nss()
        self.lookup = self.nss.question_lookup

    def test_lookup_question_description(self):
        xml_key = "Q1"
        expected_description = "Staff are good at explaining things"
        description = self.lookup[xml_key]
        self.assertEqual(expected_description, description)


class TestNssGetStats(unittest.TestCase):
    def setUp(self):
        self.nss = Nss()

    def test_with_large_file(self):
        """Initial smoke test"""
        xml_string = get_string("fixtures/large-test-file.xml")
        root = ET.fromstring(xml_string)
        for institution in root.iter("INSTITUTION"):
            for course in institution.findall("KISCOURSE"):
                raw_course_data = xmltodict.parse(ET.tostring(course))["KISCOURSE"]
                self.nss.get_stats(raw_course_data)

    def test_get_stats_no_subj(self):
        raw_course_xml = xmltodict.parse(
            get_string("fixtures/course_nss_questions.xml")
        )["KISCOURSE"]
        expected_response = json.loads(
            get_string("fixtures/course_nss_questions_resp.json")
        )
        json_obj = self.nss.get_stats(raw_course_xml)
        self.assertEqual(json_obj[0], expected_response[0])

    def test_get_stats_subj(self):
        raw_course_xml = xmltodict.parse(get_string("fixtures/course_nss_subj.xml"))[
            "KISCOURSE"
        ]
        expected_response = json.loads(get_string("fixtures/course_nss_subj_resp.json"))
        json_obj = self.nss.get_stats(raw_course_xml)
        self.assertDictEqual(json_obj[0], expected_response[0])


# TODO Test more of the functionality - more lookups etc

if __name__ == "__main__":
    unittest.main()
