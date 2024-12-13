import json
import unittest

import xmltodict

from etl_pipeline.course_stats import JobType
from etl_pipeline.tests.test_helpers.testing_utils import get_string


class TestGetJobTypeKey(unittest.TestCase):
    def setUp(self):
        self.job_type = JobType(subject_codes={})

    def test_with_valid_key(self):
        expected_key = "number_of_students"
        xml_key = "JOBPOP"
        key = self.job_type.get_key(xml_key)
        self.assertEqual(expected_key, key)

    def test_with_invalid_key(self):
        invalid_xml_key = "invalid_key"
        self.assertIsNone(JobType(subject_codes={"test":"test"}).get_key(invalid_xml_key))


class TestGetJobType(unittest.TestCase):
    def setUp(self):
        self.job_type = JobType(subject_codes={})

    def test_get_stats_no_subj(self):
        raw_course_xml = xmltodict.parse(
            get_string("fixtures/course_no_subj_for_most.xml")
        )["KISCOURSE"]
        expected_response = json.loads(
            get_string("fixtures/course_no_job_type_subj_resp.json")
        )
        json_obj = self.job_type.get_stats(raw_course_xml)
        self.assertListEqual(json_obj, expected_response)


if __name__ == "__main__":
    unittest.main()
