import json
import unittest

import defusedxml.ElementTree as ET
import xmltodict

from course_stats import JobType
from testing_utils import get_string


class TestGetJobTypeKey(unittest.TestCase):
    def setUp(self):
        self.job_type = JobType()

    def test_with_valid_key(self):
        expected_key = "number_of_students"
        xml_key = "JOBPOP"
        key = self.job_type.get_key(xml_key)
        self.assertEqual(expected_key, key)

    def test_with_invalid_key(self):
        invalid_xml_key = "invalid_key"
        with self.assertRaises(KeyError):
            self.job_type.get_key(invalid_xml_key)


class TestGetJobType(unittest.TestCase):
    def setUp(self):
        self.job_type = JobType()

    def test_with_large_file(self):
        """Initial smoke test"""
        xml_string = get_string("fixtures/large-test-file.xml")
        root = ET.fromstring(xml_string)
        for institution in root.iter("INSTITUTION"):
            for course in institution.findall("KISCOURSE"):
                raw_course_data = xmltodict.parse(ET.tostring(course))["KISCOURSE"]
                self.job_type.get_stats(raw_course_data)

    def test_get_stats_no_subj(self):
        raw_course_xml = xmltodict.parse(
            get_string("fixtures/course_no_subj_for_most.xml")
        )["KISCOURSE"]
        expected_response = json.loads(
            get_string("fixtures/course_no_job_type_subj_resp.json")
        )
        json_obj = self.job_type.get_stats(raw_course_xml)
        self.assertListEqual(json_obj, expected_response)

    def test_get_stats_with_subj(self):
        raw_course_xml = xmltodict.parse(
            get_string("fixtures/course_with_subj_for_most.xml")
        )["KISCOURSE"]
        expected_response = json.loads(
            get_string("fixtures/course_job_type_subj_resp.json")
        )
        json_obj = self.job_type.get_stats(raw_course_xml)
        self.assertListEqual(json_obj, expected_response)


# TODO Test more of the functionality - more lookups etc

if __name__ == "__main__":
    unittest.main()
