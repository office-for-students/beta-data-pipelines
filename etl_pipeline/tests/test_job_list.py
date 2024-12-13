import json
import unittest

import xmltodict

from etl_pipeline.course_stats import JobList
from etl_pipeline.tests.test_helpers.testing_utils import get_string


class TestLookupDataFields(unittest.TestCase):
    def setUp(self):
        self.job_list = JobList(subject_codes={})
        self.lookup = self.job_list.data_fields_lookup

    def test_agg_lookup(self):
        xml_key = "COMAGG"
        expected_key = "aggregation_level"
        expected_elem_type = "M"
        json_key = self.lookup[xml_key][0]
        elem_type = self.lookup[xml_key][1]
        self.assertEqual(expected_key, json_key)
        self.assertEqual(expected_elem_type, elem_type)

    def test_resp_rate_lookup(self):
        xml_key = "COMRESP_RATE"
        expected_key = "response_rate"
        expected_elem_type = "M"
        json_key = self.lookup[xml_key][0]
        elem_type = self.lookup[xml_key][1]
        self.assertEqual(expected_key, json_key)
        self.assertEqual(expected_elem_type, elem_type)

    def test_pop_lookup(self):
        xml_key = "COMPOP"
        expected_key = "number_of_students"
        expected_elem_type = "M"
        json_key = self.lookup[xml_key][0]
        elem_type = self.lookup[xml_key][1]
        self.assertEqual(expected_key, json_key)
        self.assertEqual(expected_elem_type, elem_type)

    def test_subj_lookup(self):
        xml_key = "COMSBJ"
        expected_key = "subject"
        expected_elem_type = "O"
        json_key = self.lookup[xml_key][0]
        elem_type = self.lookup[xml_key][1]
        self.assertEqual(expected_key, json_key)
        self.assertEqual(expected_elem_type, elem_type)

    def test_joblist_lookup(self):
        xml_key = "JOBLIST"
        expected_key = "list"
        expected_elem_type = "O"
        json_key = self.lookup[xml_key][0]
        elem_type = self.lookup[xml_key][1]
        self.assertEqual(expected_key, json_key)
        self.assertEqual(expected_elem_type, elem_type)


class TestGetStats(unittest.TestCase):
    def setUp(self):
        self.job_list = JobList(subject_codes={})

    def test_no_subj(self):
        raw_course_xml = xmltodict.parse(
            get_string("fixtures/course_no_subj_for_most.xml")
        )["KISCOURSE"]
        expected_result = json.loads(
            get_string("fixtures/course_no_com_subj_resp.json")
        )
        json_obj = self.job_list.get_stats(raw_course_xml)
        self.assertListEqual(json_obj, expected_result)


if __name__ == "__main__":
    unittest.main()
