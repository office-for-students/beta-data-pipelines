import json
import unittest

import defusedxml.ElementTree as ET

import xmltodict

from course_stats import JobList

from testing_utils import get_string


class TestGetStats(unittest.TestCase):
    def test_with_large_file(self):
        """Initial smoke test"""
        xml_string = get_string("fixtures/large-test-file.xml")
        root = ET.fromstring(xml_string)
        for institution in root.iter("INSTITUTION"):
            for course in institution.findall("KISCOURSE"):
                raw_course_data = xmltodict.parse(ET.tostring(course))[
                    "KISCOURSE"
                ]
                job_list = JobList()
                json_job_list = job_list.get_stats(raw_course_data)
                if len(json_job_list) > 1:
                    print(json.dumps(json_job_list, indent=4))


class TestLookupDataFields(unittest.TestCase):
    def setUp(self):
        self.job_list = JobList()
        self.lookup = self.job_list.data_fields_lookup

    def test_agg_lookup(self):
        xml_key = "COMAGG"
        expected_key = "aggregation_level"
        expected_elem_type = "M"
        json_key = self.lookup[xml_key][0]
        elem_type = self.lookup[xml_key][1]
        self.assertEqual(expected_key, json_key)
        self.assertEqual(expected_elem_type, elem_type)


# TODO Test more of the functionality - more lookups etc

if __name__ == "__main__":
    unittest.main()
