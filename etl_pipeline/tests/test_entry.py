import json
import unittest

import xmltodict

from etl_pipeline.course_stats import Entry
from etl_pipeline.tests.test_helpers.testing_utils import get_string


class TestGetEntryKey(unittest.TestCase):
    def setUp(self):
        self.entry = Entry(subject_codes={})

    def test_with_valid_key(self):
        expected_key = "number_of_students"
        xml_key = "ENTPOP"
        key = Entry(subject_codes={"test":"test"}).get_key(xml_key)
        self.assertEqual(expected_key, key)


class TestGetEntry(unittest.TestCase):
    def setUp(self):
        self.entry = Entry(subject_codes={})

    def test_get_stats_no_subj(self):
        raw_course_xml = xmltodict.parse(
            get_string("fixtures/course_no_subj_for_most.xml")
        )["KISCOURSE"]
        expected_response = json.loads(
            get_string("fixtures/course_no_entry_subj_resp.json")
        )
        json_obj = self.entry.get_stats(raw_course_xml)
        self.assertListEqual(json_obj, expected_response)


# TODO Test more of the functionality - more lookups etc

if __name__ == "__main__":
    unittest.main()
