import json
import unittest

import defusedxml.ElementTree as ET
import xmltodict

from unittest import mock
from course_stats import Continuation
from testing_utils import get_string


class TestGetContinuationKey(unittest.TestCase):
    def setUp(self):
        self.continuation = Continuation()

    def test_with_valid_key(self):
        expected_key = "number_of_students"
        xml_key = "CONTPOP"
        key = self.continuation.get_key(xml_key)
        self.assertEqual(expected_key, key)

    def test_with_valid_key(self):
        invalid_xml_key = "invalid_key"
        with self.assertRaises(KeyError):
            self.continuation.get_key(invalid_xml_key)


class TestGetContinuation(unittest.TestCase):
    def setUp(self):
        self.continuation = Continuation()

    # def test_with_large_file(self):
    #     """Initial smoke test"""
    #     xml_string = get_string("fixtures/large-test-file.xml")
    #     root = ET.fromstring(xml_string)
    #     for institution in root.iter("INSTITUTION"):
    #         for course in institution.findall("KISCOURSE"):
    #             raw_course_data = xmltodict.parse(ET.tostring(course))[
    #                 "KISCOURSE"
    #             ]
    #             self.continuation.get_stats(raw_course_data)

    def test_get_stats_no_subj(self):
        raw_course_xml = xmltodict.parse(
            get_string("fixtures/course_no_subj_for_most.xml")
        )["KISCOURSE"]
        expected_response = json.loads(
            get_string("fixtures/course_no_cont_subj_resp.json")
        )
        continuation = self.continuation.get_stats(raw_course_xml)
        self.assertListEqual(continuation, expected_response)

    # def test_get_stats_two_continuations_no_subj(self):
    #     raw_course_xml = xmltodict.parse(
    #         get_string("fixtures/course_two_contins.xml")
    #     )["KISCOURSE"]
    #     expected_response = json.loads(
    #         get_string("fixtures/course_two_contins_resp.json")
    #     )
    #     continuation = self.continuation.get_stats(raw_course_xml)
    #     self.assertListEqual(continuation, expected_response)

    # def test_get_stats_subj(self):
    #     raw_course_xml = xmltodict.parse(
    #         get_string("fixtures/course_with_subj_for_most.xml")
    #     )["KISCOURSE"]
    #     expected_response = json.loads(
    #         get_string("fixtures/course_cont_subj_resp.json")
    #     )
    #     continuation = self.continuation.get_stats(raw_course_xml)
    #     self.assertListEqual(continuation, expected_response)

    def test_get_stats_agg_14(self):
        raw_course_xml = xmltodict.parse(
            get_string("fixtures/course_532986.xml")
        )["KISCOURSE"]
        expected_response = json.loads(
            get_string("fixtures/course_532986_resp.json")
        )
        continuation = self.continuation.get_stats(raw_course_xml)
        self.assertListEqual(continuation, expected_response)


# TODO Test more of the functionality - more lookups etc

if __name__ == "__main__":
    unittest.main()
